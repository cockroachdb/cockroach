// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"fmt"
	"net/url"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/diagutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

func TestCheckVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	t.Run("expected-reporting", func(t *testing.T) {
		r := diagutils.NewServer()
		defer r.Close()

		url := r.URL()
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &TestingKnobs{
					DiagnosticsTestingKnobs: diagnosticspb.TestingKnobs{
						OverrideUpdatesURL: &url,
					},
				},
			},
		})
		defer s.Stopper().Stop(ctx)
		s.CheckForUpdates(ctx)
		r.Close()

		if expected, actual := 1, r.NumRequests(); actual != expected {
			t.Fatalf("expected %v update checks, got %v", expected, actual)
		}

		last := r.LastRequestData()
		if expected, actual := s.(*TestServer).ClusterID().String(), last.UUID; expected != actual {
			t.Errorf("expected uuid %v, got %v", expected, actual)
		}

		if expected, actual := build.GetInfo().Tag, last.Version; expected != actual {
			t.Errorf("expected version tag %v, got %v", expected, actual)
		}

		if expected, actual := "OSS", last.LicenseType; expected != actual {
			t.Errorf("expected license type %v, got %v", expected, actual)
		}

		if expected, actual := "false", last.Internal; expected != actual {
			t.Errorf("expected internal to be %v, got %v", expected, actual)
		}
	})

	t.Run("npe", func(t *testing.T) {
		// Ensure nil, which happens when an empty env override URL is used, does not
		// cause a crash.
		var nilURL *url.URL
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &TestingKnobs{
					DiagnosticsTestingKnobs: diagnosticspb.TestingKnobs{
						OverrideUpdatesURL:   &nilURL,
						OverrideReportingURL: &nilURL,
					},
				},
			},
		})
		defer s.Stopper().Stop(ctx)
		s.CheckForUpdates(ctx)
		s.ReportDiagnostics(ctx)
	})
}

func TestUsageQuantization(t *testing.T) {
	defer leaktest.AfterTest(t)()

	r := diagutils.NewServer()
	defer r.Close()

	st := cluster.MakeTestingClusterSettings()
	ctx := context.Background()

	url := r.URL()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: st,
		Knobs: base.TestingKnobs{
			Server: &TestingKnobs{
				DiagnosticsTestingKnobs: diagnosticspb.TestingKnobs{
					OverrideReportingURL: &url,
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	ts := s.(*TestServer)

	// Disable periodic reporting so it doesn't interfere with the test.
	if _, err := db.Exec(`SET CLUSTER SETTING diagnostics.reporting.enabled = false`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`SET application_name = 'test'`); err != nil {
		t.Fatal(err)
	}

	// Issue some queries against the test app name.
	for i := 0; i < 8; i++ {
		if _, err := db.Exec(`SELECT 1`); err != nil {
			t.Fatal(err)
		}
	}
	// Between 10 and 100 queries is quantized to 10.
	for i := 0; i < 30; i++ {
		if _, err := db.Exec(`SELECT 1,2`); err != nil {
			t.Fatal(err)
		}
	}
	// Between 100 and 10000 gets quantized to 100.
	for i := 0; i < 200; i++ {
		if _, err := db.Exec(`SELECT 1,2,3`); err != nil {
			t.Fatal(err)
		}
	}
	// Above 10000 gets quantized to 10000.
	for i := 0; i < 10010; i++ {
		if _, err := db.Exec(`SHOW application_name`); err != nil {
			t.Fatal(err)
		}
	}

	// Flush the SQL stat pool.
	ts.Server.sqlServer.pgServer.SQLServer.ResetSQLStats(ctx)

	// Collect a round of statistics.
	ts.ReportDiagnostics(ctx)

	// The stats "hide" the application name by hashing it. To find the
	// test app name, we need to hash the ref string too prior to the
	// comparison.
	clusterSecret := sql.ClusterSecret.Get(&st.SV)
	hashedAppName := sql.HashForReporting(clusterSecret, "test")
	if hashedAppName == sql.FailedHashedValue {
		t.Fatalf("expected hashedAppName to not be 'unknown'")
	}

	testData := []struct {
		query         string
		expectedCount int64
	}{
		{`SELECT _`, 8},
		{`SELECT _, _`, 10},
		{`SELECT _, _, _`, 100},
		{`SHOW application_name`, 10000},
	}

	last := r.LastRequestData()
	for _, test := range testData {
		found := false
		for _, s := range last.SqlStats {
			if s.Key.App == hashedAppName && s.Key.Query == test.query {
				if s.Stats.Count != test.expectedCount {
					t.Errorf("quantization incorrect for query %q: expected %d, got %d",
						test.query, test.expectedCount, s.Stats.Count)
				}
				found = true
				break
			}
		}
		if !found {
			t.Errorf("query %q missing from stats", test.query)
		}
	}
}

// This test is deprecated; it is being replaced with datadriven tests
// (see sql.TestTelemetry).
func TestReportUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const elemName = "somestring"
	ctx := context.Background()

	r := diagutils.NewServer()
	defer r.Close()

	st := cluster.MakeTestingClusterSettings()

	url := r.URL()
	storeSpec := base.DefaultTestStoreSpec
	storeSpec.Attributes = roachpb.Attributes{Attrs: []string{elemName}}
	params := base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			storeSpec,
			base.DefaultTestStoreSpec,
		},
		Settings: st,
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "east"},
				{Key: "zone", Value: elemName},
				{Key: "state", Value: "ny"},
				{Key: "city", Value: "nyc"},
			},
		},
		Knobs: base.TestingKnobs{
			SQLLeaseManager: &lease.ManagerTestingKnobs{
				// Disable SELECT called for delete orphaned leases to keep
				// query stats stable.
				DisableDeleteOrphanedLeases: true,
			},
			Server: &TestingKnobs{
				DiagnosticsTestingKnobs: diagnosticspb.TestingKnobs{
					OverrideReportingURL: &url,
				},
			},
		},
	}

	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background()) // stopper will wait for the update/report loop to finish too.
	ts := s.(*TestServer)

	// make sure the test's generated activity is the only activity we measure.
	telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)

	if _, err := db.Exec(fmt.Sprintf(`CREATE DATABASE %s`, elemName)); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`SET CLUSTER SETTING server.time_until_store_dead = '90s'`); err != nil {
		t.Fatal(err)
	}
	// Disable periodic reporting so it doesn't interfere with the test.
	if _, err := db.Exec(`SET CLUSTER SETTING diagnostics.reporting.enabled = false`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(`SET CLUSTER SETTING diagnostics.reporting.send_crash_reports = false`); err != nil {
		t.Fatal(err)
	}

	for _, cmd := range []struct {
		resource string
		config   string
	}{
		{"TABLE system.rangelog", fmt.Sprintf(`constraints: [+zone=%[1]s, +%[1]s]`, elemName)},
		{"TABLE system.rangelog", `{gc: {ttlseconds: 1}}`},
		{"DATABASE system", `num_replicas: 5`},
		{"DATABASE system", fmt.Sprintf(`constraints: {"+zone=%[1]s,+%[1]s": 2, +%[1]s: 1}`, elemName)},
		{"DATABASE system", fmt.Sprintf(`experimental_lease_preferences: [[+zone=%[1]s,+%[1]s], [+%[1]s]]`, elemName)},
	} {
		testutils.SucceedsSoon(t, func() error {
			if _, err := db.Exec(
				fmt.Sprintf(`ALTER %s CONFIGURE ZONE = '%s'`, cmd.resource, cmd.config),
			); err != nil {
				// Work around gossip asynchronicity.
				return errors.Errorf("error applying zone config %q to %q: %v", cmd.config, cmd.resource, err)
			}
			return nil
		})
	}

	// Set cluster to an internal testing cluster
	q := `SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing'`
	if _, err := db.Exec(q); err != nil {
		t.Fatal(err)
	}

	expectedUsageReports := 0

	clusterSecret := sql.ClusterSecret.Get(&st.SV)
	testutils.SucceedsSoon(t, func() error {
		expectedUsageReports++

		node := ts.node.recorder.GenerateNodeStatus(ctx)
		// Clear the SQL stat pool before getting diagnostics.
		ts.sqlServer.pgServer.SQLServer.ResetSQLStats(ctx)
		ts.ReportDiagnostics(ctx)

		keyCounts := make(map[roachpb.StoreID]int64)
		rangeCounts := make(map[roachpb.StoreID]int64)
		totalKeys := int64(0)
		totalRanges := int64(0)

		for _, store := range node.StoreStatuses {
			if keys, ok := store.Metrics["keycount"]; ok {
				totalKeys += int64(keys)
				keyCounts[store.Desc.StoreID] = int64(keys)
			} else {
				t.Fatal("keycount not in metrics")
			}
			if replicas, ok := store.Metrics["replicas"]; ok {
				totalRanges += int64(replicas)
				rangeCounts[store.Desc.StoreID] = int64(replicas)
			} else {
				t.Fatal("replicas not in metrics")
			}
		}

		if expected, actual := expectedUsageReports, r.NumRequests(); expected != actual {
			t.Fatalf("expected %v reports, got %v", expected, actual)
		}
		last := r.LastRequestData()
		if expected, actual := ts.ClusterID().String(), last.UUID; expected != actual {
			return errors.Errorf("expected cluster id %v got %v", expected, actual)
		}
		if expected, actual := ts.node.Descriptor.NodeID, last.Node.NodeID; expected != actual {
			return errors.Errorf("expected node id %v got %v", expected, actual)
		}

		if last.Node.Hardware.Mem.Total == 0 {
			return errors.Errorf("expected non-zero total mem")
		}
		if last.Node.Hardware.Mem.Available == 0 {
			return errors.Errorf("expected non-zero available mem")
		}
		if actual, expected := last.Node.Hardware.Cpu.Numcpu, runtime.NumCPU(); int(actual) != expected {
			return errors.Errorf("expected %d num cpu, got %d", expected, actual)
		}
		if last.Node.Hardware.Cpu.Sockets == 0 {
			return errors.Errorf("expected non-zero sockets")
		}
		if last.Node.Hardware.Cpu.Mhz == 0.0 {
			return errors.Errorf("expected non-zero speed")
		}
		if last.Node.Os.Platform == "" {
			return errors.Errorf("expected non-empty OS")
		}

		if minExpected, actual := totalKeys, last.Node.KeyCount; minExpected > actual {
			return errors.Errorf("expected node keys at least %v got %v", minExpected, actual)
		}
		if minExpected, actual := totalRanges, last.Node.RangeCount; minExpected > actual {
			return errors.Errorf("expected node ranges at least %v got %v", minExpected, actual)
		}
		if minExpected, actual := len(params.StoreSpecs), len(last.Stores); minExpected > actual {
			return errors.Errorf("expected at least %v stores got %v", minExpected, actual)
		}
		if expected, actual := "true", last.Internal; expected != actual {
			t.Errorf("expected internal to be %v, got %v", expected, actual)
		}
		if expected, actual := len(params.Locality.Tiers), len(last.Node.Locality.Tiers); expected != actual {
			t.Errorf("expected locality to have %d tier, got %d", expected, actual)
		}
		for i := range params.Locality.Tiers {
			if expected, actual := sql.HashForReporting(clusterSecret, params.Locality.Tiers[i].Key),
				last.Node.Locality.Tiers[i].Key; expected != actual {
				t.Errorf("expected locality tier %d key to be %s, got %s", i, expected, actual)
			}
			if expected, actual := sql.HashForReporting(clusterSecret, params.Locality.Tiers[i].Value),
				last.Node.Locality.Tiers[i].Value; expected != actual {
				t.Errorf("expected locality tier %d value to be %s, got %s", i, expected, actual)
			}
		}

		for _, store := range last.Stores {
			if minExpected, actual := keyCounts[store.StoreID], store.KeyCount; minExpected > actual {
				return errors.Errorf("expected at least %v keys in store %v got %v", minExpected, store.StoreID, actual)
			}
			if minExpected, actual := rangeCounts[store.StoreID], store.RangeCount; minExpected > actual {
				return errors.Errorf("expected at least %v ranges in store %v got %v", minExpected, store.StoreID, actual)
			}
		}
		return nil
	})

	last := r.LastRequestData()
	// This check isn't clean, since the body is a raw proto binary and thus could
	// easily contain some encoded form of elemName, but *if* it ever does fail,
	// that is probably very interesting.
	if strings.Contains(last.RawReportBody, elemName) {
		t.Fatalf("%q should not appear in %q", elemName, last.RawReportBody)
	}

	// 3 + 3 = 6: set 3 initially and org is set mid-test for 3 altered settings,
	// plus version, reporting and secret settings are set in startup
	// migrations.
	if expected, actual := 6, len(last.AlteredSettings); expected != actual {
		t.Fatalf("expected %d changed settings, got %d: %v", expected, actual, last.AlteredSettings)
	}
	for key, expected := range map[string]string{
		"cluster.organization":                     "<redacted>",
		"diagnostics.reporting.send_crash_reports": "false",
		"server.time_until_store_dead":             "1m30s",
		"version":                                  clusterversion.TestingBinaryVersion.String(),
		"cluster.secret":                           "<redacted>",
	} {
		if got, ok := last.AlteredSettings[key]; !ok {
			t.Fatalf("expected report of altered setting %q", key)
		} else if got != expected {
			t.Fatalf("expected reported value of setting %q to be %q not %q", key, expected, got)
		}
	}

	// Verify that we receive the four auto-populated zone configs plus the two
	// modified above, and that their values are as expected.
	for _, expectedID := range []int64{
		keys.RootNamespaceID,
		keys.LivenessRangesID,
		keys.MetaRangesID,
		keys.RangeEventTableID,
		keys.SystemDatabaseID,
	} {
		if _, ok := last.ZoneConfigs[expectedID]; !ok {
			t.Errorf("didn't find expected ID %d in reported ZoneConfigs: %+v",
				expectedID, last.ZoneConfigs)
		}
	}
	hashedElemName := sql.HashForReporting(clusterSecret, elemName)
	hashedZone := sql.HashForReporting(clusterSecret, "zone")
	for id, zone := range last.ZoneConfigs {
		if id == keys.RootNamespaceID {
			if defZone := ts.Cfg.DefaultZoneConfig; !reflect.DeepEqual(zone, defZone) {
				t.Errorf("default zone config does not match: expected\n%+v got\n%+v", defZone, zone)
			}
		}
		if id == keys.RangeEventTableID {
			if a, e := zone.GC.TTLSeconds, int32(1); a != e {
				t.Errorf("expected zone %d GC.TTLSeconds = %d; got %d", id, e, a)
			}
			if a, e := zone.Constraints, []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Key: hashedZone, Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
						{Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
					},
				},
			}; !reflect.DeepEqual(a, e) {
				t.Errorf("expected zone %d Constraints = %+v; got %+v", id, e, a)
			}
		}
		if id == keys.SystemDatabaseID {
			if a, e := zone.Constraints, []zonepb.ConstraintsConjunction{
				{
					NumReplicas: 1,
					Constraints: []zonepb.Constraint{{Value: hashedElemName, Type: zonepb.Constraint_REQUIRED}},
				},
				{
					NumReplicas: 2,
					Constraints: []zonepb.Constraint{
						{Key: hashedZone, Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
						{Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
					},
				},
			}; !reflect.DeepEqual(a, e) {
				t.Errorf("expected zone %d Constraints = %+v; got %+v", id, e, a)
			}
			if a, e := zone.LeasePreferences, []zonepb.LeasePreference{
				{
					Constraints: []zonepb.Constraint{
						{Key: hashedZone, Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
						{Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
					},
				},
				{
					Constraints: []zonepb.Constraint{{Value: hashedElemName, Type: zonepb.Constraint_REQUIRED}},
				},
			}; !reflect.DeepEqual(a, e) {
				t.Errorf("expected zone %d LeasePreferences = %+v; got %+v", id, e, a)
			}
		}
	}
}
