// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package diagnostics_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"runtime"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/kvccl/kvtenantccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/diagnostics"
	"github.com/cockroachdb/cockroach/pkg/server/diagnosticspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/diagutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/cloudinfo"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Dummy import to pull in kvtenantccl. This allows us to start tenants.
var _ = kvtenantccl.Connector{}

const elemName = "somestring"

func TestTenantReport(t *testing.T) {
	defer leaktest.AfterTest(t)
	defer log.Scope(t).Close(t)

	rt := startReporterTest(t)
	defer rt.Close()

	tenantArgs := base.TestTenantArgs{
		TenantID:                    roachpb.MakeTenantID(security.EmbeddedTenantIDs()[0]),
		AllowSettingClusterSettings: true,
		TestingKnobs:                rt.testingKnobs,
	}
	tenant, tenantDB := serverutils.StartTenant(t, rt.server, tenantArgs)
	reporter := tenant.DiagnosticsReporter().(*diagnostics.Reporter)

	ctx := context.Background()
	setupCluster(t, tenantDB)

	// Clear the SQL stat pool before getting diagnostics.
	rt.server.SQLServer().(*sql.Server).ResetSQLStats(ctx)
	reporter.ReportDiagnostics(ctx)

	require.Equal(t, 1, rt.diagServer.NumRequests())

	last := rt.diagServer.LastRequestData()
	require.Equal(t, rt.server.ClusterID().String(), last.UUID)
	require.Equal(t, tenantArgs.TenantID.String(), last.TenantID)
	require.Equal(t, "", last.NodeID)
	require.Equal(t, tenant.SQLInstanceID().String(), last.SQLInstanceID)
	require.Equal(t, "true", last.Internal)

	// Verify environment.
	verifyEnvironment(t, "", roachpb.Locality{}, &last.Env)

	// Verify SQL info.
	require.Equal(t, tenant.SQLInstanceID(), last.SQL.SQLInstanceID)

	// Verify FeatureUsage.
	require.NotZero(t, len(last.FeatureUsage))

	// Call PeriodicallyReportDiagnostics and ensure it sends out a report.
	reporter.PeriodicallyReportDiagnostics(ctx, rt.server.Stopper())
	testutils.SucceedsSoon(t, func() error {
		if rt.diagServer.NumRequests() != 2 {
			return errors.Errorf("did not receive a diagnostics report")
		}
		return nil
	})
}

// TestServerReport checks nodes, stores, localities, and zone configs.
// Telemetry metrics are checked in datadriven tests (see sql.TestTelemetry).
func TestServerReport(t *testing.T) {
	defer leaktest.AfterTest(t)
	defer log.Scope(t).Close(t)

	rt := startReporterTest(t)
	defer rt.Close()

	ctx := context.Background()
	setupCluster(t, rt.serverDB)

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
			if _, err := rt.serverDB.Exec(
				fmt.Sprintf(`ALTER %s CONFIGURE ZONE = '%s'`, cmd.resource, cmd.config),
			); err != nil {
				// Work around gossip asynchronicity.
				return errors.Errorf("error applying zone config %q to %q: %v", cmd.config, cmd.resource, err)
			}
			return nil
		})
	}

	expectedUsageReports := 0

	clusterSecret := sql.ClusterSecret.Get(&rt.settings.SV)
	testutils.SucceedsSoon(t, func() error {
		expectedUsageReports++

		node := rt.server.MetricsRecorder().GenerateNodeStatus(ctx)
		// Clear the SQL stat pool before getting diagnostics.
		rt.server.SQLServer().(*sql.Server).ResetSQLStats(ctx)
		rt.server.ReportDiagnostics(ctx)

		keyCounts := make(map[roachpb.StoreID]int64)
		rangeCounts := make(map[roachpb.StoreID]int64)
		totalKeys := int64(0)
		totalRanges := int64(0)

		for _, store := range node.StoreStatuses {
			keys, ok := store.Metrics["keycount"]
			require.True(t, ok, "keycount not in metrics")
			totalKeys += int64(keys)
			keyCounts[store.Desc.StoreID] = int64(keys)

			replicas, ok := store.Metrics["replicas"]
			require.True(t, ok, "replicas not in metrics")
			totalRanges += int64(replicas)
			rangeCounts[store.Desc.StoreID] = int64(replicas)
		}

		require.Equal(t, expectedUsageReports, rt.diagServer.NumRequests())

		last := rt.diagServer.LastRequestData()
		if expected, actual := rt.server.ClusterID().String(), last.UUID; expected != actual {
			return errors.Errorf("expected cluster id %v got %v", expected, actual)
		}
		if expected, actual := "system", last.TenantID; expected != actual {
			return errors.Errorf("expected tenant id %v got %v", expected, actual)
		}
		if expected, actual := rt.server.NodeID().String(), last.NodeID; expected != actual {
			return errors.Errorf("expected node id %v got %v", expected, actual)
		}
		if expected, actual := rt.server.NodeID().String(), last.SQLInstanceID; expected != actual {
			return errors.Errorf("expected sql instance id %v got %v", expected, actual)
		}
		if expected, actual := rt.server.NodeID(), last.Node.NodeID; expected != actual {
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
		if minExpected, actual := len(rt.serverArgs.StoreSpecs), len(last.Stores); minExpected > actual {
			return errors.Errorf("expected at least %v stores got %v", minExpected, actual)
		}
		if expected, actual := "true", last.Internal; expected != actual {
			return errors.Errorf("expected internal to be %v, got %v", expected, actual)
		}
		if expected, actual := len(rt.serverArgs.Locality.Tiers), len(last.Node.Locality.Tiers); expected != actual {
			return errors.Errorf("expected locality to have %d tier, got %d", expected, actual)
		}
		for i := range rt.serverArgs.Locality.Tiers {
			if expected, actual := sql.HashForReporting(clusterSecret, rt.serverArgs.Locality.Tiers[i].Key),
				last.Node.Locality.Tiers[i].Key; expected != actual {
				return errors.Errorf("expected locality tier %d key to be %s, got %s", i, expected, actual)
			}
			if expected, actual := sql.HashForReporting(clusterSecret, rt.serverArgs.Locality.Tiers[i].Value),
				last.Node.Locality.Tiers[i].Value; expected != actual {
				return errors.Errorf("expected locality tier %d value to be %s, got %s", i, expected, actual)
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

	last := rt.diagServer.LastRequestData()
	// This check isn't clean, since the body is a raw proto binary and thus could
	// easily contain some encoded form of elemName, but *if* it ever does fail,
	// that is probably very interesting.
	require.NotContains(t, last.RawReportBody, elemName)

	// 3 + 3 = 6: set 3 initially and org is set mid-test for 3 altered settings,
	// plus version, reporting and secret settings are set in startup
	// migrations.
	expected, actual := 6, len(last.AlteredSettings)
	require.Equal(t, expected, actual, "expected %d changed settings, got %d: %v", expected, actual, last.AlteredSettings)

	for key, expected := range map[string]string{
		"cluster.organization":                     "<redacted>",
		"diagnostics.reporting.send_crash_reports": "false",
		"server.time_until_store_dead":             "1m30s",
		"version":                                  clusterversion.TestingBinaryVersion.String(),
		"cluster.secret":                           "<redacted>",
	} {
		got, ok := last.AlteredSettings[key]
		require.True(t, ok, "expected report of altered setting %q", key)
		require.Equal(t, expected, got, "expected reported value of setting %q to be %q not %q", key, expected, got)
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
		_, ok := last.ZoneConfigs[expectedID]
		require.True(t, ok, "didn't find expected ID %d in reported ZoneConfigs: %+v",
			expectedID, last.ZoneConfigs)
	}
	hashedElemName := sql.HashForReporting(clusterSecret, elemName)
	hashedZone := sql.HashForReporting(clusterSecret, "zone")
	for id, zone := range last.ZoneConfigs {
		if id == keys.RootNamespaceID {
			require.Equal(t, zone, *rt.server.ExecutorConfig().(sql.ExecutorConfig).DefaultZoneConfig)
		}
		if id == keys.RangeEventTableID {
			require.Equal(t, int32(1), zone.GC.TTLSeconds)
			constraints := []zonepb.ConstraintsConjunction{
				{
					Constraints: []zonepb.Constraint{
						{Key: hashedZone, Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
						{Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
					},
				},
			}
			require.Equal(t, zone.Constraints, constraints)
		}
		if id == keys.SystemDatabaseID {
			constraints := []zonepb.ConstraintsConjunction{
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
			}
			require.Equal(t, constraints, zone.Constraints)
			prefs := []zonepb.LeasePreference{
				{
					Constraints: []zonepb.Constraint{
						{Key: hashedZone, Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
						{Value: hashedElemName, Type: zonepb.Constraint_REQUIRED},
					},
				},
				{
					Constraints: []zonepb.Constraint{{Value: hashedElemName, Type: zonepb.Constraint_REQUIRED}},
				},
			}
			require.Equal(t, prefs, zone.LeasePreferences)
		}
	}
}

type reporterTest struct {
	cloudEnable  func()
	settings     *cluster.Settings
	diagServer   *diagutils.Server
	testingKnobs base.TestingKnobs
	serverArgs   base.TestServerArgs
	server       serverutils.TestServerInterface
	serverDB     *gosql.DB
}

func (t *reporterTest) Close() {
	t.cloudEnable()
	t.diagServer.Close()
	// stopper will wait for the update/report loop to finish too.
	t.server.Stopper().Stop(context.Background())
}

func startReporterTest(t *testing.T) *reporterTest {
	// Disable cloud info reporting, since it slows down tests.
	rt := &reporterTest{
		cloudEnable: cloudinfo.Disable(),
		settings:    cluster.MakeTestingClusterSettings(),
		diagServer:  diagutils.NewServer(),
	}

	url := rt.diagServer.URL()
	rt.testingKnobs = base.TestingKnobs{
		SQLLeaseManager: &lease.ManagerTestingKnobs{
			// Disable SELECT called for delete orphaned leases to keep
			// query stats stable.
			DisableDeleteOrphanedLeases: true,
		},
		Server: &server.TestingKnobs{
			DiagnosticsTestingKnobs: diagnosticspb.TestingKnobs{
				OverrideReportingURL: &url,
			},
		},
	}

	storeSpec := base.DefaultTestStoreSpec
	storeSpec.Attributes = roachpb.Attributes{Attrs: []string{elemName}}
	rt.serverArgs = base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			storeSpec,
			base.DefaultTestStoreSpec,
		},
		Settings: rt.settings,
		Locality: roachpb.Locality{
			Tiers: []roachpb.Tier{
				{Key: "region", Value: "east"},
				{Key: "zone", Value: elemName},
				{Key: "state", Value: "ny"},
				{Key: "city", Value: "nyc"},
			},
		},
		Knobs: rt.testingKnobs,
	}
	rt.server, rt.serverDB, _ = serverutils.StartServer(t, rt.serverArgs)

	// Make sure the test's generated activity is the only activity we measure.
	telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ResetCounts)

	return rt
}

func setupCluster(t *testing.T, db *gosql.DB) {
	_, err := db.Exec(`SET CLUSTER SETTING server.time_until_store_dead = '90s'`)
	require.NoError(t, err)

	// Enable diagnostics reporting to test PeriodicallyReportDiagnostics.
	_, err = db.Exec(`SET CLUSTER SETTING diagnostics.reporting.enabled = true`)
	require.NoError(t, err)

	_, err = db.Exec(`SET CLUSTER SETTING diagnostics.reporting.send_crash_reports = false`)
	require.NoError(t, err)

	_, err = db.Exec(fmt.Sprintf(`CREATE DATABASE %s`, elemName))
	require.NoError(t, err)

	// Set cluster to an internal testing cluster
	q := `SET CLUSTER SETTING cluster.organization = 'Cockroach Labs - Production Testing'`
	_, err = db.Exec(q)
	require.NoError(t, err)
}

func verifyEnvironment(
	t *testing.T, secret string, locality roachpb.Locality, env *diagnosticspb.Environment,
) {
	require.NotEqual(t, 0, env.Hardware.Mem.Total)
	require.NotEqual(t, 0, env.Hardware.Mem.Available)
	require.Equal(t, int32(runtime.NumCPU()), env.Hardware.Cpu.Numcpu)
	require.NotEqual(t, 0, env.Hardware.Cpu.Sockets)
	require.NotEqual(t, 0.0, env.Hardware.Cpu.Mhz)
	require.NotEqual(t, 0.0, env.Os.Platform)
	require.NotEmpty(t, env.Build.Tag)
	require.NotEmpty(t, env.Build.Distribution)
	require.NotEmpty(t, env.LicenseType)

	require.Equal(t, len(locality.Tiers), len(env.Locality.Tiers))
	for i := range locality.Tiers {
		require.Equal(t, sql.HashForReporting(secret, locality.Tiers[i].Key), env.Locality.Tiers[i].Key)
		require.Equal(t, sql.HashForReporting(secret, locality.Tiers[i].Value), env.Locality.Tiers[i].Value)
	}
}
