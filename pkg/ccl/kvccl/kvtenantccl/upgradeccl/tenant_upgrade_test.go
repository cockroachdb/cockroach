// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package upgradeccl_test

import (
	"context"
	gosql "database/sql"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// TestTenantUpgrade exercises the case where a system tenant is in a
// non-finalized version state and creates a tenant. The test ensures
// that the newly created tenant begins in that same version.
//
// The first subtest creates the tenant in the mixed version state,
// then upgrades the system tenant, then upgrades the secondary tenant,
// and ensures everything is happy. It then restarts the tenant and ensures
// that the cluster version is properly set.
//
// The second subtest creates a new tenant after the system tenant has been
// upgraded and ensures that it is created at the final cluster version. It
// also verifies that the version is correct after a restart
func TestTenantUpgrade(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	v1 := clusterversion.TestingBinaryMinSupportedVersion
	v2 := clusterversion.TestingBinaryVersion

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		v2,
		v1,
		false, // initializeVersion
	)
	// Initialize the version to the BinaryMinSupportedVersion.
	require.NoError(t, clusterversion.Initialize(ctx,
		clusterversion.TestingBinaryMinSupportedVersion, &settings.SV))

	t.Log("starting server")
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Settings:          settings,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				BinaryVersionOverride:          v1,
			},
			// Make the upgrade faster by accelerating jobs.
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer ts.Stopper().Stop(ctx)
	sysDB := sqlutils.MakeSQLRunner(ts.SQLConn(t, ""))

	expectedInitialTenantVersion, _, _ := v0v1v2()
	startAndConnectToTenant := func(t *testing.T, id uint64) (tenant serverutils.ApplicationLayerInterface, tenantDB *gosql.DB) {
		settings := cluster.MakeTestingClusterSettingsWithVersions(
			v2,
			v1,
			false, // initializeVersion
		)
		// Initialize the version to the minimum it could be.
		require.NoError(t,
			clusterversion.Initialize(ctx, expectedInitialTenantVersion, &settings.SV))
		tenantArgs := base.TestTenantArgs{
			TenantID: roachpb.MustMakeTenantID(id),
			TestingKnobs: base.TestingKnobs{
				// Make the upgrade faster by accelerating jobs.
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
			},
			Settings: settings,
		}
		tenant, err := ts.StartTenant(ctx, tenantArgs)
		require.NoError(t, err)
		return tenant, tenant.SQLConn(t, "")
	}

	t.Run("upgrade tenant", func(t *testing.T) {
		t.Log("create a tenant before upgrading anything")
		const initialTenantID = 10
		tenantServer, conn := startAndConnectToTenant(t, initialTenantID)
		db := sqlutils.MakeSQLRunner(conn)

		t.Log("ensure that the tenant works")
		db.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{expectedInitialTenantVersion.String()}})
		db.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY)")
		db.Exec(t, "INSERT INTO t VALUES (1), (2)")

		t.Log("upgrade the storage cluster to v2")
		sysDB.Exec(t, "SET CLUSTER SETTING version = $1", v2.String())

		t.Log("ensure that the tenant still works")
		db.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})

		t.Log("upgrade the tenant cluster to v2")
		db.Exec(t, "SET CLUSTER SETTING version = $1", v2.String())

		t.Log("ensure that the tenant still works")
		db.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})
		db.CheckQueryResults(t, "SHOW CLUSTER SETTING version", [][]string{{v2.String()}})

		t.Log("restart the tenant")
		tenantServer.AppStopper().Stop(ctx)
		tenantServer, err := ts.StartTenant(ctx, base.TestTenantArgs{
			TenantID: roachpb.MustMakeTenantID(initialTenantID),
		})
		require.NoError(t, err)
		conn = tenantServer.SQLConn(t, "")
		db = sqlutils.MakeSQLRunner(conn)

		t.Log("ensure that the version is still at v2")
		db.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})
		db.CheckQueryResults(t, "SHOW CLUSTER SETTING version", [][]string{{v2.String()}})
	})

	t.Run("post-upgrade tenant", func(t *testing.T) {
		t.Log("create a new tenant")
		const postUpgradeTenantID = 11
		tenant, conn := startAndConnectToTenant(t, postUpgradeTenantID)

		t.Log("verify it is at v2")
		sqlutils.MakeSQLRunner(conn).CheckQueryResults(t,
			"SHOW CLUSTER SETTING version", [][]string{{v2.String()}})

		t.Log("restart the new tenant")
		tenant.AppStopper().Stop(ctx)
		var err error
		tenant, err = ts.StartTenant(ctx, base.TestTenantArgs{
			TenantID: roachpb.MustMakeTenantID(postUpgradeTenantID),
		})
		require.NoError(t, err)
		conn = tenant.SQLConn(t, "")

		t.Log("verify it still is at v2")
		sqlutils.MakeSQLRunner(conn).CheckQueryResults(t,
			"SHOW CLUSTER SETTING version", [][]string{{v2.String()}})
	})
}

// Returns three versions :
//   - v0 corresponds to the bootstrapped version of the tenant,
//   - v1, v2 correspond to adjacent releases.
func v0v1v2() (roachpb.Version, roachpb.Version, roachpb.Version) {
	v0 := clusterversion.ByKey(clusterversion.V22_2)
	v1 := clusterversion.TestingBinaryVersion
	v2 := clusterversion.TestingBinaryVersion
	if v1.Internal > 2 {
		v1.Internal -= 2
	} else {
		v2.Internal += 2
	}
	return v0, v1, v2
}

// TestTenantUpgradeFailure exercises cases where the tenant dies
// between version upgrades.
func TestTenantUpgradeFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	v0 := clusterversion.TestingBinaryMinSupportedVersion
	v2 := clusterversion.TestingBinaryVersion
	// v1 needs to be between v0 and v2. Set it to the minor release
	// after v0 and before v2.
	var v1 roachpb.Version
	for _, version := range clusterversion.ListBetween(v0, v2) {
		if version.Minor != v0.Minor {
			v1 = version
			break
		}
	}
	require.NotEqual(t, v1, roachpb.Version{})

	t.Log("starting server")
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		v2,
		v0,
		false, // initializeVersion
	)
	// Initialize the version to the BinaryMinSupportedVersion.
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Settings:          settings,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				BinaryVersionOverride:          v0,
			},
		},
	})
	defer ts.Stopper().Stop(ctx)
	sysDB := sqlutils.MakeSQLRunner(ts.SQLConn(t, ""))

	// Channel for stopping a tenant.
	tenantStopperChannel := make(chan struct{})
	shouldWaitForTenantToStop := true
	startAndConnectToTenant := func(t *testing.T, id uint64) (tenant serverutils.ApplicationLayerInterface, db *gosql.DB) {
		settings := cluster.MakeTestingClusterSettingsWithVersions(
			v2,
			v0,
			false, // initializeVersion
		)
		// Shorten the reclaim loop so that terminated SQL servers don't block
		// the upgrade from succeeding.
		instancestorage.ReclaimLoopInterval.Override(ctx, &settings.SV, 250*time.Millisecond)
		slinstance.DefaultTTL.Override(ctx, &settings.SV, 15*time.Second)
		slinstance.DefaultHeartBeat.Override(ctx, &settings.SV, 500*time.Millisecond)
		tenantStopper := stop.NewStopper()
		// Initialize the version to the minimum it could be.
		require.NoError(t, clusterversion.Initialize(ctx, v0, &settings.SV))
		tenantArgs := base.TestTenantArgs{
			Stopper:  tenantStopper,
			TenantID: roachpb.MustMakeTenantID(id),
			TestingKnobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				// Disable the span config job so that it doesn't interfere with
				// the upgrade interlock.
				SpanConfig: &spanconfig.TestingKnobs{
					ManagerDisableJobCreation: true,
				},
				UpgradeManager: &upgradebase.TestingKnobs{
					DontUseJobs: true,
					RegistryOverride: func(v roachpb.Version) (upgradebase.Upgrade, bool) {
						switch v {
						case v1:
							return upgrade.NewTenantUpgrade("testing",
								v1,
								upgrade.NoPrecondition,
								func(
									ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
								) error {
									t.Logf("v1 migration running")
									return nil
								}), true
						case v2:
							return upgrade.NewTenantUpgrade("testing next",
								v2,
								upgrade.NoPrecondition,
								func(
									ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
								) error {
									t.Log("v2 migration starts running")
									defer t.Log("v2 migration finishes running")
									if shouldWaitForTenantToStop {
										tenantStopperChannel <- struct{}{}
										// Wait until we are sure the stopper is quiescing.
										for {
											select {
											case <-tenant.AppStopper().ShouldQuiesce():
												return nil
											default:
												continue
											}
										}
									}
									return nil
								}), true
						default:
							return nil, false
						}
					},
				},
			},
			Settings: settings,
		}
		tenant, err := ts.StartTenant(ctx, tenantArgs)
		require.NoError(t, err)
		tenantDB := tenant.SQLConn(t, "")
		return tenant, tenantDB
	}

	t.Run("upgrade tenant have it crash then resume", func(t *testing.T) {
		t.Log("create a tenant before upgrading anything")
		const initialTenantID = 10
		tenant, conn := startAndConnectToTenant(t, initialTenantID)
		db := sqlutils.MakeSQLRunner(conn)

		t.Log("ensure that the tenant works and verify its version")
		db.CheckQueryResults(t,
			"SHOW CLUSTER SETTING version", [][]string{{v0.String()}})
		db.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY)")
		db.Exec(t, "INSERT INTO t VALUES (1), (2)")
		// Use to wait for tenant crash leading to a clean up.
		waitForTenantClose := make(chan struct{})
		// Cause the upgrade to crash on v1.
		go func() {
			<-tenantStopperChannel
			t.Log("received async notification to stop tenant")
			tenant.AppStopper().Stop(ctx)
			t.Log("tenant stopped")
			waitForTenantClose <- struct{}{}
		}()

		t.Log("upgrade the storage cluster to v2")
		sysDB.Exec(t, "SET CLUSTER SETTING version = $1", v2.String())

		t.Log("ensure that the tenant still works")
		db.CheckQueryResults(t,
			"SELECT * FROM t", [][]string{{"1"}, {"2"}})
		t.Log("upgrade the tenant cluster, expecting the upgrade to fail on v1")
		db.ExpectNonNilErr(t,
			"SET CLUSTER SETTING version = $1", v2.String())

		t.Log("waiting for tenant shutdown to complete")
		<-waitForTenantClose

		t.Log("restart the tenant server")
		tenant, conn = startAndConnectToTenant(t, initialTenantID)
		db = sqlutils.MakeSQLRunner(conn)

		t.Log("ensure that the tenant still works and the target version wasn't reached")
		db.CheckQueryResults(t,
			"SELECT * FROM t", [][]string{{"1"}, {"2"}})
		db.CheckQueryResults(t,
			"SELECT split_part(version, '-', 1) FROM [SHOW CLUSTER SETTING version]",
			[][]string{{v1.String()}})

		t.Log("restart the tenant")
		tenant.AppStopper().Stop(ctx)
		tenant, conn = startAndConnectToTenant(t, initialTenantID)
		defer tenant.AppStopper().Stop(ctx)
		db = sqlutils.MakeSQLRunner(conn)

		// Keep trying to resume the stopper channel until the channel is closed,
		// since we may repeatedly wait on it due to transaction retries. In
		// the other case the stopper is used, so no such risk exists.
		go func() {
			for {
				_, ok := <-tenantStopperChannel
				if !ok {
					t.Log("tenant stopper channel closed")
					return
				}
			}
		}()
		t.Log("make sure that all shutdown SQL instance are gone before proceeding")
		// We need to wait here because if we don't, the upgrade may hit
		// errors because it's trying to bump the cluster version for a SQL
		// instance which doesn't exist (i.e. the one that was restarted above).
		db.CheckQueryResultsRetry(t,
			"SELECT count(*) FROM system.sql_instances WHERE session_id IS NOT NULL",
			[][]string{{"1"}})

		t.Log("upgrade the tenant cluster to v2")
		shouldWaitForTenantToStop = false
		db.Exec(t, "SET CLUSTER SETTING version = $1", v2.String())

		close(tenantStopperChannel)

		t.Log("validate the target version v2 has been reached")
		db.CheckQueryResults(t,
			"SELECT * FROM t", [][]string{{"1"}, {"2"}})
		db.CheckQueryResults(t,
			"SHOW CLUSTER SETTING version", [][]string{{v2.String()}})
		tenant.AppStopper().Stop(ctx)
	})
}
