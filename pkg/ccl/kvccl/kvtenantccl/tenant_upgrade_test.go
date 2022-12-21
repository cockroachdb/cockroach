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
	"net/url"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
				UpgradeManager: &upgradebase.TestingKnobs{
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
			".*(database is closed|failed to connect|closed network connection)+",
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
		{
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
		// Upgrade the tenant cluster. The "SucceedsSoon" is necessary here
		// because the tenant upgrade could fail due to the fact that we have
		// a cached view of the above crashed SQL server lying around, which
		// we then try and contact as part of the upgrade process.
		initialTenantRunner.ExecSucceedsSoon(t,
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	var tests = []struct {
		expUpgradeErr []string // empty if expecting a nil error
		expStartupErr []string // empty if expecting a nil error
		pausePoint    int
	}{
		{
			// In this case we won't see the new server in the first
			// transaction, and will instead see it when we try and commit the
			// first transaction.
			pausePoint: upgradebase.AfterFirstCheckForInstances,
			expUpgradeErr: []string{
				"",
				"pq: upgrade failed due to active SQL servers with incompatible binary version",
			},
			expStartupErr: []string{
				"",
				"",
			},
		},
		{
			pausePoint: upgradebase.AfterFenceRPC,
			expUpgradeErr: []string{
				"",
				"pq: upgrade failed due to active SQL servers with incompatible binary version",
			},
			expStartupErr: []string{
				"",
				"",
			},
		},
		{
			pausePoint: upgradebase.AfterFenceWriteToSettingsTable,
			expUpgradeErr: []string{
				"",
				// TODO(ajstorm): It's not ideal that we're failing both the
				//  startup _and_ the upgrade here and in the tests below. This
				//  is caused by the fact that when the server startup fails, we
				//  don't immediately remove the instance from the sql_instances
				//  table. We may want to fix this so that the upgrade will
				//  succeed on the first attempt (or at least on a subsequent
				//  attempt, if utilize SucceedsSoon).
				"pq: upgrade failed due to active SQL servers with incompatible binary version",
			},
			expStartupErr: []string{
				"",
				"preventing SQL server from starting because its binary version is too low for the tenant active version",
			},
		},
		{
			pausePoint: upgradebase.AfterSecondCheckForInstances,
			expUpgradeErr: []string{
				"",
				"pq: upgrade failed due to active SQL servers with incompatible binary version",
			},
			expStartupErr: []string{
				"",
				"preventing SQL server from starting because its binary version is too low for the tenant active version",
			},
		},
		{
			pausePoint: upgradebase.AfterMigration,
			expUpgradeErr: []string{
				"",
				"pq: upgrade failed due to active SQL servers with incompatible binary version",
			},
			expStartupErr: []string{
				"",
				"preventing SQL server from starting because its binary version is too low for the tenant active version",
			},
		},
		{
			pausePoint: upgradebase.AfterVersionBumpRPC,
			expUpgradeErr: []string{
				"",
				"pq: upgrade failed due to active SQL servers with incompatible binary version",
			},
			expStartupErr: []string{
				"",
				"preventing SQL server from starting because its binary version is too low for the tenant active version",
			},
		},
		{
			pausePoint: upgradebase.AfterVersionWriteToSettingsTable,
			expUpgradeErr: []string{
				"",
				"pq: upgrade failed due to active SQL servers with incompatible binary version",
			},
			expStartupErr: []string{
				"",
				"preventing SQL server from starting because its binary version is too low for the tenant active version",
			},
		},
	}

	const (
		currentBinaryVersion = iota
		laggingBinaryVersion
	)

	var configurations = []int{
		currentBinaryVersion,
		laggingBinaryVersion,
	}

	for _, config := range configurations {
		config := config
		for i, test := range tests {
			test := test
			func() {
				reachedChannel := make(chan struct{})
				resumeChannel := make(chan struct{})
				completedChannel := make(chan struct{})
				defer close(reachedChannel)
				defer close(resumeChannel)
				defer close(completedChannel)

				bv := clusterversion.TestingBinaryVersion
				msv := clusterversion.ByKey(sql.TenantCreationMinSupportedVersionKey)

				t.Logf("upgrade interlock test: running configuration %d iteration %d", config, i)
				expectingUpgradeToFail := test.expUpgradeErr[config] != ""
				finalUpgradeVersion := clusterversion.TestingBinaryVersion
				if expectingUpgradeToFail {
					finalUpgradeVersion = msv
				}

				// Initialize the version to the BinaryMinSupportedVersion so that
				// we can perform upgrades.
				settings := cluster.MakeTestingClusterSettingsWithVersions(bv, msv, false /* initializeVersion */)
				require.NoError(t, clusterversion.Initialize(ctx, msv, &settings.SV))

				tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
					ServerArgs: base.TestServerArgs{
						// Test validates tenant behavior. No need for the default test
						// tenant.
						DisableDefaultTestTenant: true,
						Settings:                 settings,
						Knobs: base.TestingKnobs{
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
					settings := cluster.MakeTestingClusterSettingsWithVersions(bv, msv, false /* initializeVersion */)
					require.NoError(t, clusterversion.Initialize(ctx, msv, &settings.SV))
					// Disable distsql and auto stats so that we don't have
					// interference from background jobs trying to initiate gRPC
					// connections. New gRPC connections cause problems with
					// this test because they could detect the down-level sql
					// server and then trip the gRPC breaker. This will then
					// cause the test to fail with a "breaker open" error,
					// instead of the error we're expecting.
					sql.DistSQLClusterExecMode.Override(ctx, &settings.SV, int64(sessiondatapb.DistSQLOff))
					stats.AutomaticStatisticsClusterMode.Override(ctx, &settings.SV, false)
					tenantArgs := base.TestTenantArgs{
						TenantID: id,
						TestingKnobs: base.TestingKnobs{
							SpanConfig: &spanconfig.TestingKnobs{
								ManagerDisableJobCreation: true,
							},
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
						completedChannel <- struct{}{}
					}
					defer release()

					if expectingUpgradeToFail {
						initialTenantRunner.ExpectErr(t,
							test.expUpgradeErr[config],
							"SET CLUSTER SETTING version = $1",
							bv.String())
					} else {
						initialTenantRunner.Exec(t,
							"SET CLUSTER SETTING version = $1",
							bv.String())
					}
				}()

				// Create a separate SQL server to startup
				// Wait until the upgrader reaches the pause point before
				// starting.
				<-reachedChannel

				// Now start a second SQL server for this tenant to see how the
				// two SQL servers interact.
				otherServerSettings := cluster.MakeTestingClusterSettingsWithVersions(bv, msv, false /* initializeVersion */)
				otherMsv := msv
				otherBv := bv
				if config == laggingBinaryVersion {
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
					otherServerSettings = cluster.MakeTestingClusterSettingsWithVersions(otherBv, otherMsv, false /* initializeVersion */)
				}
				require.NoError(t, clusterversion.Initialize(ctx, otherBv, &otherServerSettings.SV))
				// Disable distsql and auto stats as noted above.
				sql.DistSQLClusterExecMode.Override(ctx, &otherServerSettings.SV, int64(sessiondatapb.DistSQLOff))
				stats.AutomaticStatisticsClusterMode.Override(ctx, &otherServerSettings.SV, false)
				otherServer, err := tc.Server(0).StartTenant(ctx,
					base.TestTenantArgs{
						TenantID: tenantID,
						Settings: otherServerSettings,
					})

				// With tenant started, resume the upgrade and wait for it to
				// complete.
				resumeChannel <- struct{}{}
				<-completedChannel

				// Handle errors and any subsequent processing once the upgrade
				// is completed.
				if test.expStartupErr[config] != "" {
					require.ErrorContains(t, err, test.expStartupErr[config])
				} else {
					require.NoError(t, err)

					otherTenant, cleanup := connectToTenant(t, otherServer.SQLAddr())
					defer cleanup()
					otherTenantRunner := sqlutils.MakeSQLRunner(otherTenant)

					// Validate that the version is as expected, and that the
					// second sql pod still works.
					otherTenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})
					otherTenantRunner.CheckQueryResults(t, "SHOW CLUSTER SETTING version",
						[][]string{{finalUpgradeVersion.String()}})
				}
			}()
		}
	}
}
