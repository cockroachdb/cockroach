// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvtenantccl_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
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
	skip.WithIssue(t, 97076, "flaky test")
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		false, // initializeVersion
	)
	// Initialize the version to the BinaryMinSupportedVersion.
	require.NoError(t, clusterversion.Initialize(ctx,
		clusterversion.TestingBinaryMinSupportedVersion, &settings.SV))
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Test validates tenant behavior. No need for the default test
			// tenant.
			DisableDefaultTestTenant: true,
			Settings:                 settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          clusterversion.TestingBinaryMinSupportedVersion,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	connectToTenant := func(t *testing.T, addr string) (_ *gosql.DB, cleanup func()) {
		pgURL, cleanupPGUrl := sqlutils.PGUrl(t, addr, "Tenant", url.User(username.RootUser))
		tenantDB, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		return tenantDB, func() {
			tenantDB.Close()
			cleanupPGUrl()
		}
	}
	expectedInitialTenantVersion, _, _ := v0v1v2()
	mkTenant := func(t *testing.T, id uint64) (tenantDB *gosql.DB, cleanup func()) {
		settings := cluster.MakeTestingClusterSettingsWithVersions(
			clusterversion.TestingBinaryVersion,
			clusterversion.TestingBinaryMinSupportedVersion,
			false, // initializeVersion
		)
		// Initialize the version to the minimum it could be.
		require.NoError(t, clusterversion.Initialize(ctx,
			expectedInitialTenantVersion, &settings.SV))
		tenantArgs := base.TestTenantArgs{
			TenantID:     roachpb.MustMakeTenantID(id),
			TestingKnobs: base.TestingKnobs{},
			Settings:     settings,
		}
		tenant, err := tc.Server(0).StartTenant(ctx, tenantArgs)
		require.NoError(t, err)
		return connectToTenant(t, tenant.SQLAddr())
	}

	t.Run("upgrade tenant", func(t *testing.T) {
		// Create a tenant before upgrading anything and verify its version.
		const initialTenantID = 10
		initialTenant, cleanup := mkTenant(t, initialTenantID)
		initialTenantRunner := sqlutils.MakeSQLRunner(initialTenant)

		// Ensure that the tenant works.
		initialTenantRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{expectedInitialTenantVersion.String()}})
		initialTenantRunner.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY)")
		initialTenantRunner.Exec(t, "INSERT INTO t VALUES (1), (2)")

		// Upgrade the host cluster.
		sqlutils.MakeSQLRunner(tc.ServerConn(0)).Exec(t,
			"SET CLUSTER SETTING version = $1",
			clusterversion.TestingBinaryVersion.String())

		// Ensure that the tenant still works.
		initialTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})

		// Upgrade the tenant cluster.
		initialTenantRunner.Exec(t,
			"SET CLUSTER SETTING version = $1",
			clusterversion.TestingBinaryVersion.String())

		// Ensure that the tenant still works.
		initialTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})
		initialTenantRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{clusterversion.TestingBinaryVersion.String()}})

		// Restart the tenant and ensure that the version is correct.
		cleanup()
		{
			tenantServer, err := tc.Server(0).StartTenant(ctx, base.TestTenantArgs{
				TenantID: roachpb.MustMakeTenantID(initialTenantID),
			})
			require.NoError(t, err)
			initialTenant, cleanup = connectToTenant(t, tenantServer.SQLAddr())
			defer cleanup()
			initialTenantRunner = sqlutils.MakeSQLRunner(initialTenant)
		}
		initialTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})
		initialTenantRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{clusterversion.TestingBinaryVersion.String()}})
	})

	t.Run("post-upgrade tenant", func(t *testing.T) {
		// Create a new tenant and ensure it has the right version.
		const postUpgradeTenantID = 11
		postUpgradeTenant, cleanup := mkTenant(t, postUpgradeTenantID)
		sqlutils.MakeSQLRunner(postUpgradeTenant).CheckQueryResults(t,
			"SHOW CLUSTER SETTING version",
			[][]string{{clusterversion.TestingBinaryVersion.String()}})

		// Restart the new tenant and ensure it has the right version.
		cleanup()
		{
			tenantServer, err := tc.Server(0).StartTenant(ctx, base.TestTenantArgs{
				TenantID: roachpb.MustMakeTenantID(postUpgradeTenantID),
			})
			require.NoError(t, err)
			postUpgradeTenant, cleanup = connectToTenant(t, tenantServer.SQLAddr())
			defer cleanup()
		}
		sqlutils.MakeSQLRunner(postUpgradeTenant).CheckQueryResults(t,
			"SHOW CLUSTER SETTING version",
			[][]string{{clusterversion.TestingBinaryVersion.String()}})
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
	skip.WithIssue(t, 98555, "flaky test")
	defer log.Scope(t).Close(t)
	// Contains information for starting a tenant
	// and maintaining a stopper.
	type tenantInfo struct {
		v2onMigrationStopper *stop.Stopper
		tenantArgs           *base.TestTenantArgs
	}
	v0, v1, v2 := v0v1v2()
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		v2,
		v0,
		false, // initializeVersion
	)
	// Initialize the version to the BinaryMinSupportedVersion.
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// Test validates tenant behavior. No need for the default test
			// tenant here.
			DisableDefaultTestTenant: true,
			Settings:                 settings,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					BinaryVersionOverride:          v0,
				},
			},
		},
	})
	defer tc.Stopper().Stop(ctx)

	// Channel for stopping a tenant.
	tenantStopperChannel := make(chan struct{})
	startAndConnectToTenant := func(t *testing.T, tenantInfo *tenantInfo) (_ *gosql.DB, cleanup func()) {
		tenant, err := tc.Server(0).StartTenant(ctx, *tenantInfo.tenantArgs)
		require.NoError(t, err)
		pgURL, cleanupPGUrl := sqlutils.PGUrl(t, tenant.SQLAddr(), "Tenant", url.User(username.RootUser))
		tenantDB, err := gosql.Open("postgres", pgURL.String())
		require.NoError(t, err)
		return tenantDB, func() {
			tenantDB.Close()
			cleanupPGUrl()
		}
	}
	mkTenant := func(t *testing.T, id uint64) *tenantInfo {
		settings := cluster.MakeTestingClusterSettingsWithVersions(
			v2,
			v0,
			false, // initializeVersion
		)
		// Shorten the reclaim loop so that terminated SQL servers don't block
		// the upgrade from succeeding.
		instancestorage.ReclaimLoopInterval.Override(ctx, &settings.SV, 250*time.Millisecond)
		slinstance.DefaultTTL.Override(ctx, &settings.SV, 3*time.Second)
		slinstance.DefaultHeartBeat.Override(ctx, &settings.SV, 500*time.Millisecond)
		v2onMigrationStopper := stop.NewStopper()
		// Initialize the version to the minimum it could be.
		require.NoError(t, clusterversion.Initialize(ctx,
			v0, &settings.SV))
		tenantArgs := base.TestTenantArgs{
			Stopper:  v2onMigrationStopper,
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
					ListBetweenOverride: func(from, to roachpb.Version) []roachpb.Version {
						return []roachpb.Version{v1, v2}
					},
					RegistryOverride: func(v roachpb.Version) (upgradebase.Upgrade, bool) {
						switch v {
						case v1:
							return upgrade.NewTenantUpgrade("testing",
								v1,
								upgrade.NoPrecondition,
								func(
									ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
								) error {
									return nil
								}), true
						case v2:
							return upgrade.NewTenantUpgrade("testing next",
								v2,
								upgrade.NoPrecondition,
								func(
									ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps,
								) error {
									tenantStopperChannel <- struct{}{}
									return nil
								}), true
						default:
							panic("Unexpected version number observed.")
						}
					},
				},
			},
			Settings: settings,
		}
		return &tenantInfo{tenantArgs: &tenantArgs,
			v2onMigrationStopper: v2onMigrationStopper}
	}

	t.Run("upgrade tenant have it crash then resume", func(t *testing.T) {
		// Create a tenant before upgrading anything and verify its version.
		const initialTenantID = 10
		tenantInfo := mkTenant(t, initialTenantID)
		tenant, cleanup := startAndConnectToTenant(t, tenantInfo)
		initialTenantRunner := sqlutils.MakeSQLRunner(tenant)
		// Ensure that the tenant works.
		initialTenantRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{v0.String()}})
		initialTenantRunner.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY)")
		initialTenantRunner.Exec(t, "INSERT INTO t VALUES (1), (2)")
		// Use to wait for tenant crash leading to a clean up.
		waitForTenantClose := make(chan struct{})
		// Cause the upgrade to crash on v1.
		go func() {
			<-tenantStopperChannel
			tenant.Close()
			tenantInfo.v2onMigrationStopper.Stop(ctx)
			waitForTenantClose <- struct{}{}
		}()
		// Upgrade the host cluster to the latest version.
		sqlutils.MakeSQLRunner(tc.ServerConn(0)).Exec(t,
			"SET CLUSTER SETTING version = $1",
			clusterversion.TestingBinaryVersion.String())
		// Ensure that the tenant still works.
		initialTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})
		// Upgrade the tenant cluster, but the upgrade will fail on v1.
		initialTenantRunner.ExpectErr(t,
			".*(database is closed|failed to connect|closed network connection|upgrade failed due to transient SQL servers)+",
			"SET CLUSTER SETTING version = $1",
			v2.String())
		<-waitForTenantClose
		cleanup()
		tenantInfo = mkTenant(t, initialTenantID)
		tenant, cleanup = startAndConnectToTenant(t, tenantInfo)
		initialTenantRunner = sqlutils.MakeSQLRunner(tenant)
		// Ensure that the tenant still works and the target
		// version wasn't reached.
		initialTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})
		initialTenantRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{v1.String()}})

		// Restart the tenant and ensure that the version is correct.
		cleanup()
		tenantInfo.v2onMigrationStopper.Stop(ctx)
		{
			tenantInfo = mkTenant(t, initialTenantID)
			tca, cleanup := startAndConnectToTenant(t, tenantInfo)
			defer cleanup()
			initialTenantRunner = sqlutils.MakeSQLRunner(tca)
		}
		// Keep trying to resume the stopper channel until the channel is closed,
		// since we may repeatedly wait on it due to transaction retries. In
		// the other case the stopper is used, so no such risk exists.
		go func() {
			for {
				_, ok := <-tenantStopperChannel
				if !ok {
					return
				}
			}
		}()
		// Make sure that all shutdown SQL instance are gone before proceeding.
		// We need to wait here because if we don't, the upgrade may hit
		// errors because it's trying to bump the cluster version for a SQL
		// instance which doesn't exist (i.e. the one that was restarted above).
		initialTenantRunner.CheckQueryResultsRetry(t,
			"SELECT count(*) FROM system.sql_instances WHERE session_id IS NOT NULL", [][]string{{"1"}})

		// Upgrade the tenant cluster.
		initialTenantRunner.Exec(t,
			"SET CLUSTER SETTING version = $1",
			v2.String())
		close(tenantStopperChannel)
		// Validate the target version has been reached.
		initialTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})
		initialTenantRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{v2.String()}})
		tenantInfo.v2onMigrationStopper.Stop(ctx)
	})
}

// TestTenantUpgradeInterlock validates the interlock between upgrading SQL
// servers and other SQL servers which may be running (and/or in the process
// of starting up). It runs with two SQL servers for the same tenant and starts
// one server which performs the upgrade, while the second server is starting
// up. It steps through all the phases of the interlock and validates that the
// system performs as expected for both starting SQL servers which are at the
// correct binary version, and those that are using a binary version that
// is too low.
func TestTenantUpgradeInterlock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Times out under stress race
	skip.UnderStressRace(t)
	// Test takes 30s to run
	skip.UnderShort(t)
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	const (
		currentBinaryVersion = iota
		laggingBinaryVersion
		numConfigs
	)

	type interlockTestVariant int

	var variants = map[interlockTestVariant]string{
		currentBinaryVersion: "current binary version",
		laggingBinaryVersion: "lagging binary version",
	}

	type interlockTestConfig struct {
		name          string
		expUpgradeErr [numConfigs][]string // empty if expecting a nil error
		expStartupErr [numConfigs]string   // empty if expecting a nil error
		pausePoint    upgradebase.PausePoint
	}

	var tests = []interlockTestConfig{
		{
			// In this case we won't see the new server in the first
			// transaction, and will instead see it when we try and commit the
			// first transaction.
			name:       "pause after first check for instances",
			pausePoint: upgradebase.AfterFirstCheckForInstances,
			expUpgradeErr: [numConfigs][]string{
				{""},
				{"pq: upgrade failed due to active SQL servers with incompatible binary version",
					fmt.Sprintf("sql server 2 is running a binary version %s which is less than the attempted upgrade version", clusterversion.TestingBinaryMinSupportedVersion.String())},
			},
			expStartupErr: [numConfigs]string{
				"",
				"",
			},
		},
		{
			name:       "pause after fence RPC",
			pausePoint: upgradebase.AfterFenceRPC,
			expUpgradeErr: [numConfigs][]string{
				{""},
				{"pq: upgrade failed due to active SQL servers with incompatible binary version"},
			},
			expStartupErr: [numConfigs]string{
				"",
				"",
			},
		},
		{
			name:       "pause after fence write to settings table",
			pausePoint: upgradebase.AfterFenceWriteToSettingsTable,
			expUpgradeErr: [numConfigs][]string{
				{""},
				{""},
			},
			expStartupErr: [numConfigs]string{
				"",
				"preventing SQL server from starting because its binary version is too low for the tenant active version",
			},
		},
		{
			name:       "pause after second check of instances",
			pausePoint: upgradebase.AfterSecondCheckForInstances,
			expUpgradeErr: [numConfigs][]string{
				{""},
				{""},
			},
			expStartupErr: [numConfigs]string{
				"",
				"preventing SQL server from starting because its binary version is too low for the tenant active version",
			},
		},
		{
			name:       "pause after migration",
			pausePoint: upgradebase.AfterMigration,
			expUpgradeErr: [numConfigs][]string{
				{""},
				{""},
			},
			expStartupErr: [numConfigs]string{
				"",
				"preventing SQL server from starting because its binary version is too low for the tenant active version",
			},
		},
		{
			name:       "pause after version bump RPC",
			pausePoint: upgradebase.AfterVersionBumpRPC,
			expUpgradeErr: [numConfigs][]string{
				{""},
				{""},
			},
			expStartupErr: [numConfigs]string{
				"",
				"preventing SQL server from starting because its binary version is too low for the tenant active version",
			},
		},
		{
			name:       "pause after write to settings table",
			pausePoint: upgradebase.AfterVersionWriteToSettingsTable,
			expUpgradeErr: [numConfigs][]string{
				{""},
				{""},
			},
			expStartupErr: [numConfigs]string{
				"",
				"preventing SQL server from starting because its binary version is too low for the tenant active version",
			},
		},
	}

	runTest := func(t *testing.T, variant interlockTestVariant, test interlockTestConfig) {
		t.Logf(`upgrade interlock test: running variant "%s", configuration: "%s"`, variants[variant], test.name)

		reachedChannel := make(chan struct{})
		resumeChannel := make(chan struct{})
		completedChannel := make(chan struct{})
		defer close(reachedChannel)
		defer close(resumeChannel)
		defer close(completedChannel)

		bv := clusterversion.TestingBinaryVersion
		msv := clusterversion.TestingBinaryMinSupportedVersion

		// If there are any non-empty errors expected on the upgrade, then we're
		// expecting it to fail.
		expectingUpgradeToFail := test.expUpgradeErr[variant][0] != ""
		finalUpgradeVersion := clusterversion.TestingBinaryVersion
		if expectingUpgradeToFail {
			finalUpgradeVersion = msv
		}

		disableBackgroundTasks := func(s *cluster.Settings) {
			// This test makes use of inter-SQL server gRPCs, some of which are
			// going between SQL pods of incompatible version numbers. In cases
			// where a gRPC handshake detects an invalid versioned SQL server,
			// it will fail, and also trip the gRPC breaker. This will then
			// cause the test to fail with a "breaker open" error, instead of
			// the error we're expecting. As a result, we disable everything
			// that could run gRPCs in the background to ensure that the upgrade
			// gRPCs detect the first version mismatch error.
			stats.AutomaticStatisticsClusterMode.Override(ctx, &s.SV, false)
			stats.UseStatisticsOnSystemTables.Override(ctx, &s.SV, false)
			stats.AutomaticStatisticsOnSystemTables.Override(ctx, &s.SV, false)
			sql.DistSQLClusterExecMode.Override(ctx, &s.SV, int64(sessiondatapb.DistSQLOff))
		}

		reduceLeaseDurationAndReclaimLoopInterval := func(s *cluster.Settings) {
			// In cases where the other SQL server fails, it may go down holding
			// a descriptor lease. In that case, we want to decrease the lease
			// duration so that we can reclaim the lease faster than the default
			// (5 minutes). This allows the test to complete faster.
			lease.LeaseDuration.Override(ctx, &s.SV, 2000*time.Millisecond)
			lease.LeaseRenewalDuration.Override(ctx, &s.SV, 0)
			// Also in the failure case, we need the SQL instance (stored in
			// the system.sql_instances table) for the failed SQL server to
			// be removed as quickly as possible. This is because a failed
			// instance in that table can cause RPC attempts to that server
			// to fail. The changes below in the other server setup will
			// ensure that the failed SQL server expires quickly. Here we
			// need to also ensure that it will be reclaimed quickly.
			instancestorage.ReclaimLoopInterval.Override(ctx, &s.SV, 1000*time.Millisecond)
		}

		reduceDefaultTTLAndHeartBeat := func(s *cluster.Settings) {
			// In the case where a SQL server is expected to fail, we need the
			// SQL instance (stored in the system.sql_instances table) for the
			// failed SQL server to be removed as quickly as possible. This is
			// because a failed instance in that table can cause RPC attempts to
			// that server to fail. To accomplish this, we decrease the default
			// TTL and heartbeat for the sessions so that they expire and are
			// cleaned up faster.
			slinstance.DefaultTTL.Override(ctx, &s.SV, 250*time.Millisecond)
			slinstance.DefaultHeartBeat.Override(ctx, &s.SV, 50*time.Millisecond)
		}

		// Initialize the version to the BinaryMinSupportedVersion so that
		// we can perform upgrades.
		settings := cluster.MakeTestingClusterSettingsWithVersions(bv, msv, false /* initializeVersion */)
		disableBackgroundTasks(settings)
		require.NoError(t, clusterversion.Initialize(ctx, msv, &settings.SV))

		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				// Test validates tenant behavior. No need for the default test
				// tenant.
				DisableDefaultTestTenant: true,
				Settings:                 settings,
				Knobs: base.TestingKnobs{
					SpanConfig: &spanconfig.TestingKnobs{
						ManagerDisableJobCreation: true,
					},
					Server: &server.TestingKnobs{
						DisableAutomaticVersionUpgrade: make(chan struct{}),
						// Initialize to the minimum supported version
						// so that we can perform the upgrade below.
						BinaryVersionOverride: msv,
					},
				},
			},
		})
		defer tc.Stopper().Stop(ctx)

		connectToTenant := func(t *testing.T, addr string) (_ *gosql.DB, cleanup func()) {
			pgURL, cleanupPGUrl := sqlutils.PGUrl(t, addr, "Tenant", url.User(username.RootUser))
			tenantDB, err := gosql.Open("postgres", pgURL.String())
			require.NoError(t, err)
			return tenantDB, func() {
				tenantDB.Close()
				cleanupPGUrl()
			}
		}

		mkTenant := func(t *testing.T, id roachpb.TenantID, bv roachpb.Version, minBv roachpb.Version) (tenantDB *gosql.DB, cleanup func()) {
			settings := cluster.MakeTestingClusterSettingsWithVersions(bv, minBv, false /* initializeVersion */)
			disableBackgroundTasks(settings)
			if test.expStartupErr[variant] != "" {
				reduceLeaseDurationAndReclaimLoopInterval(settings)
			}
			require.NoError(t, clusterversion.Initialize(ctx, minBv, &settings.SV))
			tenantArgs := base.TestTenantArgs{
				TenantID: id,
				TestingKnobs: base.TestingKnobs{
					JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
					UpgradeManager: &upgradebase.TestingKnobs{
						InterlockPausePoint:               test.pausePoint,
						InterlockResumeChannel:            &resumeChannel,
						InterlockReachedPausePointChannel: &reachedChannel,
					},
				},
				Settings: settings,
			}
			tenant, err := tc.Server(0).StartTenant(ctx, tenantArgs)
			require.NoError(t, err)
			return connectToTenant(t, tenant.SQLAddr())
		}

		// Create a tenant before upgrading anything, and verify its
		// version.
		tenantID := serverutils.TestTenantID()
		tenant, cleanup := mkTenant(t, tenantID, bv, msv)
		defer cleanup()
		initialTenantRunner := sqlutils.MakeSQLRunner(tenant)

		// Ensure that the tenant works.
		initialTenantRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{msv.String()}})
		initialTenantRunner.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY)")
		initialTenantRunner.Exec(t, "INSERT INTO t VALUES (1), (2)")
		initialTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})

		// Validate that the host cluster is at the expected version, and
		// upgrade it to the binary version.
		hostClusterRunner := sqlutils.MakeSQLRunner(tc.ServerConn(0))
		hostClusterRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
			[][]string{{msv.String()}})
		hostClusterRunner.Exec(t, "SET CLUSTER SETTING version = $1", bv.String())

		// Ensure that the tenant still works.
		initialTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})

		// Start upgrading the tenant. This call will pause at the specified
		// pause point, and once resumed (below, outside this function),
		// will complete the upgrade.
		go func() {
			release := func() {
				log.Infof(ctx, "upgrade completed, resuming post-upgrade work")
				completedChannel <- struct{}{}
			}
			defer release()

			// Since we're in a separate go routine from the main test, we can't
			// use the SQLRunner methods for running queries, since they Fatal
			// under the covers. See https://go.dev/doc/go1.16#vet-testing-T for
			// more details.
			if expectingUpgradeToFail {
				getPossibleUpgradeErrorsString := func(variant interlockTestVariant) string {
					possibleErrorsString := ""
					for i := range test.expUpgradeErr[variant] {
						expErrString := test.expUpgradeErr[variant][i]
						if i > 0 {
							possibleErrorsString += " OR "
						}
						possibleErrorsString += `"` + expErrString + `"`
					}
					return possibleErrorsString
				}
				_, err := tenant.Exec("SET CLUSTER SETTING version = $1", bv.String())
				if err == nil {
					t.Errorf("expected %s, got: success", getPossibleUpgradeErrorsString(variant))
				} else {
					foundExpErr := false
					expErrString := ""
					for i := range test.expUpgradeErr[variant] {
						expErrString = test.expUpgradeErr[variant][i]
						if strings.Contains(err.Error(), expErrString) {
							foundExpErr = true
							break
						}
					}
					if !foundExpErr {
						t.Errorf("expected %s, got: %s", getPossibleUpgradeErrorsString(variant), err.Error())
					}
				}
			} else {
				_, err := tenant.Exec("SET CLUSTER SETTING version = $1", bv.String())
				if err != nil {
					t.Error("unexpected error: ", err.Error())
				}
			}
		}()

		// Wait until the upgrader reaches the pause point before
		// starting.
		<-reachedChannel

		// Now start a second SQL server for this tenant to see how the
		// two SQL servers interact.
		otherMsv := msv
		otherBv := bv
		if variant == laggingBinaryVersion {
			// If we're in "lagging binary" mode, we want the server to
			// startup with a binary that is too old for the upgrade to
			// succeed. To make this happen we set the binary version to
			// the tenant's minimum binary version, and then set the
			// server's minimum binary version to one major release
			// earlier. The last step isn't strictly required, but we
			// do it to prevent any code from tripping which validates
			// that the binary version cannot equal the minimum binary
			// version.
			otherBv = msv
			otherMsv.Major = msv.Major - 1
		}
		otherServerSettings := cluster.MakeTestingClusterSettingsWithVersions(otherBv, otherMsv, false /* initializeVersion */)
		disableBackgroundTasks(otherServerSettings)
		if test.expStartupErr[variant] != "" {
			reduceLeaseDurationAndReclaimLoopInterval(otherServerSettings)
			reduceDefaultTTLAndHeartBeat(otherServerSettings)
		}
		require.NoError(t, clusterversion.Initialize(ctx, otherMsv, &otherServerSettings.SV))
		otherServerStopper := stop.NewStopper()
		otherServer, otherServerStartError := tc.Server(0).StartTenant(ctx,
			base.TestTenantArgs{
				Stopper:  otherServerStopper,
				TenantID: tenantID,
				Settings: otherServerSettings,
			})

		var otherTenantRunner *sqlutils.SQLRunner
		expectingStartupToFail := test.expStartupErr[variant] != ""
		numTenantsStr := "1"
		if expectingStartupToFail {
			otherServerStopper.Stop(ctx)
		} else if otherServerStartError == nil {
			defer otherServer.Stopper().Stop(ctx)
			otherTenant, otherCleanup := connectToTenant(t, otherServer.SQLAddr())
			defer otherCleanup()
			otherTenantRunner = sqlutils.MakeSQLRunner(otherTenant)
			numTenantsStr = "2"
		}

		// Based on the success or failure of the "other" SQL server startup,
		// we'll either have 1 or 2 SQL servers running at this point. Confirm
		// that we're in the desired state by querying the sql_instances table
		// directly. We do this before we continue to ensure that the upgrade
		// doesn't encounter any stale SQL instances.
		initialTenantRunner.CheckQueryResultsRetry(t,
			"SELECT count(*) FROM SYSTEM.SQL_INSTANCES WHERE SESSION_ID IS NOT NULL", [][]string{{numTenantsStr}})

		// With tenant started, resume the upgrade and wait for it to
		// complete.
		resumeChannel <- struct{}{}
		<-completedChannel
		log.Infof(ctx, "continuing after upgrade completed")

		// Now that we've resumed the upgrade, process any errors. We must do
		// that after the upgrade completes because any unexpected errors above
		// will cause the test to FailNow, thus bringing down the cluster and
		// the tenant, which could cause the upgrade to hang if it's not resumed
		// first.
		if expectingStartupToFail {
			require.ErrorContains(t, otherServerStartError, test.expStartupErr[variant])
		} else {
			require.NoError(t, otherServerStartError)
		}

		// Handle errors and any subsequent processing once the upgrade
		// is completed.
		if test.expStartupErr[variant] == "" {
			// Validate that the version is as expected, and that the
			// second sql pod still works.
			otherTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})
			otherTenantRunner.CheckQueryResultsRetry(t, "SHOW CLUSTER SETTING version",
				[][]string{{finalUpgradeVersion.String()}})
		}
	}

	for variant := range variants {
		variantName := variants[variant]
		for i := range tests {
			test := tests[i]
			testName := variantName + "_" + test.name
			t.Run(testName, func(t *testing.T) { runTest(t, variant, test) })
		}
	}
}
