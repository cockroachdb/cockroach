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
	"sort"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/diagutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
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
	ctx := context.TODO()

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

func TestReportUsage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const elemName = "somestring"
	const internalAppName = sqlbase.ReportableAppNamePrefix + "foo"
	ctx := context.TODO()

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
			SQLLeaseManager: &sql.LeaseManagerTestingKnobs{
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
	defer s.Stopper().Stop(context.TODO()) // stopper will wait for the update/report loop to finish too.
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

	telemetry.Count("test.a")
	telemetry.Count("test.b")
	telemetry.Count("test.b")
	c := telemetry.GetCounter("test.c")
	telemetry.Inc(c)
	telemetry.Inc(c)
	telemetry.Inc(c)

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
	if _, err := db.Exec(`INSERT INTO system.zones (id, config) VALUES (10000, null)`); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		fmt.Sprintf(`CREATE TABLE %[1]s.%[1]s (%[1]s INT8 CONSTRAINT %[1]s CHECK (%[1]s > 1))`, elemName),
	); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(
		fmt.Sprintf(`CREATE TABLE %[1]s.%[1]s_s (%[1]s SERIAL2)`, elemName),
	); err != nil {
		t.Fatal(err)
	}

	// Run some queries so we have some query statistics collected.
	for i := 0; i < 10; i++ {
		// Run some sample queries. Each are passed a string and int by Exec.
		// Note placeholders aren't allowed in some positions, including names.
		for _, q := range []string{
			`SELECT * FROM %[1]s.%[1]s WHERE %[1]s = length($1::string) OR %[1]s = $2`,
			`INSERT INTO %[1]s.%[1]s VALUES (length($1::string)), ($2)`,
		} {
			if _, err := db.Exec(fmt.Sprintf(q, elemName), elemName, 10003); err != nil {
				t.Fatal(err)
			}
		}
		// Even queries that don't use placeholders and contain literal strings
		// should still not cause those strings to appear in reports.
		for _, q := range []string{
			`SELECT * FROM %[1]s.%[1]s WHERE %[1]s = 1 AND '%[1]s' = '%[1]s'`,
			`INSERT INTO %[1]s.%[1]s VALUES (6), (7), (8)`,
			`INSERT INTO %[1]s.%[1]s SELECT unnest(ARRAY[9, 10, 11, 12])`,
			`SELECT (1, 20, 30, 40) = (SELECT %[1]s, 1, 2, 3 FROM %[1]s.%[1]s LIMIT 1)`,
			`SET application_name = '%[1]s'`,
			`SELECT %[1]s FROM %[1]s.%[1]s WHERE %[1]s = 1 AND lower('%[1]s') = lower('%[1]s')`,
			`UPDATE %[1]s.%[1]s SET %[1]s = %[1]s + 1`,
		} {
			if _, err := db.Exec(fmt.Sprintf(q, elemName)); err != nil {
				t.Fatal(err)
			}
		}
		// Application names with the '$ ' prefix should not be scrubbed.
		if _, err := db.Exec(`SET application_name = $1`, internalAppName); err != nil {
			t.Fatal(err)
		}
		// Set cluster to an internal testing cluster
		q := `SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing'`
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(`RESET application_name`); err != nil {
			t.Fatal(err)
		}
	}

	tables, err := ts.collectSchemaInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if actual := len(tables); actual != 2 {
		t.Fatalf("unexpected table count %d", actual)
	}
	for _, table := range tables {
		if expected, actual := "_", table.Name; expected != actual {
			t.Fatalf("unexpected table name, expected %q got %q", expected, actual)
		}
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

	if expected, actual := len(tables), len(last.Schema); expected != actual {
		t.Fatalf("expected %d tables in schema, got %d", expected, actual)
	}
	reportedByID := make(map[sqlbase.ID]sqlbase.TableDescriptor, len(tables))
	for _, tbl := range last.Schema {
		reportedByID[tbl.ID] = tbl
	}
	for _, tbl := range tables {
		r, ok := reportedByID[tbl.ID]
		if !ok {
			t.Fatalf("expected table %d to be in reported schema", tbl.ID)
		}
		if !reflect.DeepEqual(r, tbl) {
			t.Fatalf("reported table %d does not match: expected\n%+v got\n%+v", tbl.ID, tbl, r)
		}
	}

	expectedFeatureUsage := map[string]int32{
		"test.a": 1,
		"test.b": 2,
		"test.c": 3,

		// SERIAL normalization.
		"sql.schema.serial.rowid.int2": 1,
	}

	if expected, actual := len(expectedFeatureUsage), len(last.FeatureUsage); actual < expected {
		t.Fatalf("expected at least %d feature usage counts, got %d: %v", expected, actual, last.FeatureUsage)
	}
	t.Logf("%# v", pretty.Formatter(last.FeatureUsage))
	for key, expected := range expectedFeatureUsage {
		if got, ok := last.FeatureUsage[key]; !ok {
			t.Fatalf("expected report of feature %q", key)
		} else if got != expected {
			t.Fatalf("expected reported value of feature %q to be %d not %d", key, expected, got)
		}
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
			if a, e := zone.Constraints, []zonepb.Constraints{
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
			if a, e := zone.Constraints, []zonepb.Constraints{
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

	var foundKeys []string
	for _, s := range last.SqlStats {
		if strings.HasPrefix(s.Key.App, sqlbase.InternalAppNamePrefix) {
			// Let's ignore all internal queries for this test.
			continue
		}
		var tags []string
		if s.Key.Opt {
			tags = append(tags, "opt")
		}

		if s.Key.DistSQL {
			tags = append(tags, "dist")
		} else {
			tags = append(tags, "nodist")
		}

		if s.Key.Failed {
			tags = append(tags, "failed")
		} else {
			tags = append(tags, "ok")
		}

		foundKeys = append(foundKeys, fmt.Sprintf("[%s] %s", strings.Join(tags, ","), s.Key.Query))
	}
	sort.Strings(foundKeys)
	expectedKeys := []string{
		`[opt,nodist,ok] ALTER DATABASE _ CONFIGURE ZONE = _`,
		`[opt,nodist,ok] ALTER TABLE _ CONFIGURE ZONE = _`,
		`[opt,nodist,ok] CREATE DATABASE _`,
		`[opt,nodist,ok] SET CLUSTER SETTING "cluster.organization" = _`,
		`[opt,nodist,ok] SET CLUSTER SETTING "diagnostics.reporting.enabled" = _`,
		`[opt,nodist,ok] SET CLUSTER SETTING "diagnostics.reporting.send_crash_reports" = _`,
		`[opt,nodist,ok] SET CLUSTER SETTING "server.time_until_store_dead" = _`,
		`[opt,nodist,ok] SET application_name = $1`,
		`[opt,nodist,ok] SET application_name = DEFAULT`,
		`[opt,nodist,ok] SET application_name = _`,
		`[opt,nodist,ok] CREATE TABLE _ (_ INT8 NOT NULL DEFAULT unique_rowid())`,
		`[opt,nodist,ok] CREATE TABLE _ (_ INT8, CONSTRAINT _ CHECK (_ > _))`,
		`[opt,nodist,ok] INSERT INTO _ SELECT unnest(ARRAY[_, _, __more2__])`,
		`[opt,nodist,ok] INSERT INTO _ VALUES (_), (__more2__)`,
		`[opt,nodist,ok] INSERT INTO _ VALUES (length($1::STRING)), (__more1__)`,
		`[opt,nodist,ok] INSERT INTO _(_, _) VALUES (_, _)`,
		`[opt,nodist,ok] SELECT (_, _, __more2__) = (SELECT _, _, _, _ FROM _ LIMIT _)`,
		`[opt,nodist,ok] UPDATE _ SET _ = _ + _`,
		`[opt,dist,ok] SELECT * FROM _ WHERE (_ = _) AND (_ = _)`,
		`[opt,dist,ok] SELECT * FROM _ WHERE (_ = length($1::STRING)) OR (_ = $2)`,
		`[opt,dist,ok] SELECT _ FROM _ WHERE (_ = _) AND (lower(_) = lower(_))`,
	}
	sort.Strings(expectedKeys)
	t.Logf("expected:\n%s\ngot:\n%s", pretty.Sprint(expectedKeys), pretty.Sprint(foundKeys))
	for i, found := range foundKeys {
		if i >= len(expectedKeys) {
			t.Fatalf("extraneous stat entry: %q", found)
		}
		if expectedKeys[i] != found {
			t.Fatalf("expected entry %d to be %q, got %q", i, expectedKeys[i], found)
		}
	}
	if len(foundKeys) < len(expectedKeys) {
		t.Fatalf("expected %d stat entries, found %d", len(expectedKeys), len(foundKeys))
	}

	bucketByApp := make(map[string][]roachpb.CollectedStatementStatistics)
	for _, s := range last.SqlStats {
		if strings.HasPrefix(s.Key.App, sqlbase.InternalAppNamePrefix) {
			// Let's ignore all internal queries for this test.
			continue
		}
		bucketByApp[s.Key.App] = append(bucketByApp[s.Key.App], s)
	}

	if expected, actual := 3, len(bucketByApp); expected != actual {
		t.Fatalf("expected %d apps in stats report, got %d", expected, actual)
	}

	for appName, expectedStatements := range map[string][]string{
		"": {
			`ALTER DATABASE _ CONFIGURE ZONE = _`,
			`ALTER TABLE _ CONFIGURE ZONE = _`,
			`CREATE DATABASE _`,
			`CREATE TABLE _ (_ INT8, CONSTRAINT _ CHECK (_ > _))`,
			`CREATE TABLE _ (_ INT8 NOT NULL DEFAULT unique_rowid())`,
			`INSERT INTO _ VALUES (length($1::STRING)), (__more1__)`,
			`INSERT INTO _ VALUES (_), (__more2__)`,
			`INSERT INTO _ SELECT unnest(ARRAY[_, _, __more2__])`,
			`INSERT INTO _(_, _) VALUES (_, _)`,
			`SELECT (_, _, __more2__) = (SELECT _, _, _, _ FROM _ LIMIT _)`,
			`SELECT * FROM _ WHERE (_ = length($1::STRING)) OR (_ = $2)`,
			`SELECT * FROM _ WHERE (_ = _) AND (_ = _)`,
			`SET CLUSTER SETTING "server.time_until_store_dead" = _`,
			`SET CLUSTER SETTING "diagnostics.reporting.enabled" = _`,
			`SET CLUSTER SETTING "diagnostics.reporting.send_crash_reports" = _`,
			`SET application_name = _`,
		},
		elemName: {
			`SELECT _ FROM _ WHERE (_ = _) AND (lower(_) = lower(_))`,
			`UPDATE _ SET _ = _ + _`,
			`SET application_name = $1`,
		},
		internalAppName: {
			`SET CLUSTER SETTING "cluster.organization" = _`,
			`SET application_name = DEFAULT`,
		},
	} {
		maybeHashedAppName := sql.HashForReporting(clusterSecret, appName)
		if appName == internalAppName {
			// Exempted from hashing due to '$ ' prefix.
			maybeHashedAppName = internalAppName
		}
		if maybeHashedAppName == sql.FailedHashedValue {
			t.Fatalf("expected maybeHashedAppName to not be 'unknown'")
		}
		if app, ok := bucketByApp[maybeHashedAppName]; !ok {
			t.Fatalf("missing stats for app %q %+v", appName, bucketByApp)
		} else {
			if actual, expected := len(app), len(expectedStatements); expected != actual {
				for _, q := range app {
					t.Log(q.Key.Query, q.Key)
				}
				t.Fatalf("expected %d statements in app %q report, got %d: %+v", expected, appName, actual, app)
			}
			keys := make(map[string]struct{})
			for _, q := range app {
				keys[q.Key.Query] = struct{}{}
			}
			for _, expected := range expectedStatements {
				if _, ok := keys[expected]; !ok {
					t.Fatalf("expected %q in app %s: %s", expected, pretty.Sprint(appName), pretty.Sprint(keys))
				}
			}
		}
	}
}
