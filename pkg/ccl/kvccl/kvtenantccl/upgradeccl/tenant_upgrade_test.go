// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgradeccl_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// autoUpgradeClusterSetting wraps data about an auto-upgrade related
// cluster setting that should lead to the auto upgrade process not
// being able to run while the setting is active, due to the given
// upgrade status.
type autoUpgradeClusterSetting struct {
	name          string
	value         any
	upgradeStatus int
}

// testTenantAutoUpgrades exercises the auto upgrade logic for
// tenants. If a `clusterSetting` is passed, they are set on the
// tenant before any upgrade takes place, and reset prior to the point
// where the tenant auto upgrade should kick in.
func testTenantAutoUpgrade(t *testing.T, clusterSetting *autoUpgradeClusterSetting) {
	defer log.Scope(t).Close(t)
	skip.UnderRace(t)

	v0 := clusterversion.MinSupported
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		v0.Version(),
		false, // initializeVersion
	)
	// Initialize the version to v0.
	require.NoError(t, clusterversion.Initialize(ctx, v0.Version(), &settings.SV))

	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Settings:          settings,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         v0.Version(),
			},
			SQLEvalContext: &eval.TestingKnobs{
				// When the host binary version is not equal to its cluster version, tenant logical version is set
				// to the host's minimum supported binary version. We need this override to ensure that the tenant is
				// created at v0.
				TenantLogicalVersionKeyOverride: v0,
			},
		},
	})
	defer ts.Stopper().Stop(ctx)
	sysDB := sqlutils.MakeSQLRunner(ts.SQLConn(t, serverutils.DBName("")))

	expectedInitialTenantVersion := v0.Version()
	expectedFinalTenantVersion := clusterversion.Latest.Version()

	tenantSettings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		v0.Version(),
		false, // initializeVersion
	)
	require.NoError(t, clusterversion.Initialize(ctx,
		expectedInitialTenantVersion, &tenantSettings.SV))

	upgradeInfoCh := make(chan struct {
		Status    int
		UpgradeTo roachpb.Version
	}, 1)
	mkTenant := func(t *testing.T, name string) (tenantDB *gosql.DB) {
		tenantArgs := base.TestSharedProcessTenantArgs{
			TenantName: roachpb.TenantName(name),
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					TenantAutoUpgradeInfo:          upgradeInfoCh,
					TenantAutoUpgradeLoopFrequency: time.Second,
					ClusterVersionOverride:         v0.Version(),
				},
			},
		}
		_, tenantDB, err := ts.TenantController().StartSharedProcessTenant(ctx, tenantArgs)
		require.NoError(t, err)
		return tenantDB
	}

	// Create a shared process tenant and its SQL server.
	const tenantName = "app"
	tenantDB := mkTenant(t, tenantName)
	tenantRunner := sqlutils.MakeSQLRunner(tenantDB)

	// Ensure that the tenant works.
	tenantRunner.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY)")
	tenantRunner.Exec(t, "INSERT INTO t VALUES (1), (2)")

	// Apply the cluster setting, if any.
	if clusterSetting != nil {
		tenantRunner.Exec(t, fmt.Sprintf("SET CLUSTER SETTING %s = $1", clusterSetting.name), clusterSetting.value)
	}

	// Upgrade the host cluster.
	sysDB.Exec(t,
		"SET CLUSTER SETTING version = $1",
		expectedFinalTenantVersion.String())

	// Ensure that the tenant still works.
	tenantRunner.CheckQueryResults(t, "SELECT * FROM t", [][]string{{"1"}, {"2"}})

	waitForUpgradeInfo := func(expectedVersion roachpb.Version, expectedStatus int) {
		succeedsSoon := 20 * time.Second

		for {
			select {
			case upgradeInfo := <-upgradeInfoCh:
				if upgradeInfo.UpgradeTo == expectedVersion && upgradeInfo.Status == expectedStatus {
					return
				}
			case <-time.After(succeedsSoon):
				t.Fatalf(
					"failed to receive the auto upgrade status for version %s and status %d after %d seconds",
					expectedVersion, expectedStatus, int(succeedsSoon.Seconds()),
				)
			}
		}
	}

	// Reset cluster setting, if any.
	if clusterSetting != nil {
		// Wait for us to receive an upgrade event indicating that we are
		// not upgrading immediately after the storage cluster due to a
		// cluster setting configuration.
		waitForUpgradeInfo(roachpb.Version{}, clusterSetting.upgradeStatus)

		tenantRunner.Exec(t, fmt.Sprintf("RESET CLUSTER SETTING %s", clusterSetting.name))
	}

	// Wait for auto upgrade status to be received by the testing
	// knob. If the min supported version and the `Latest` version are
	// on the same major release, the tenant will just realize that it
	// is already upgraded. Otherwise, the upgrade should be allowed to
	// continue.
	expectedVersion := expectedFinalTenantVersion
	expectedStatus := server.UpgradeAllowed
	if expectedFinalTenantVersion.Major == v0.Version().Major &&
		expectedFinalTenantVersion.Minor == v0.Version().Minor {
		expectedVersion = roachpb.Version{}
		expectedStatus = server.UpgradeAlreadyCompleted
	}
	waitForUpgradeInfo(expectedVersion, int(expectedStatus))
}

func TestTenantAutoUpgradeNoClusterSettings(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDuress(t, "slow test")
	testTenantAutoUpgrade(t, nil)
}

func TestTenantAutoUpgradeWithAutoUpgradeClusterSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDuress(t, "slow test")
	testTenantAutoUpgrade(t, &autoUpgradeClusterSetting{
		name:          "cluster.auto_upgrade.enabled",
		value:         false,
		upgradeStatus: int(server.UpgradeDisabledByConfiguration),
	})
}

func TestTenantAutoUpgradeWithPreserveDowngradeOptionClusterSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderDuress(t, "slow test")
	testTenantAutoUpgrade(t, &autoUpgradeClusterSetting{
		name:          "cluster.preserve_downgrade_option",
		value:         clusterversion.MinSupported.Version().String(),
		upgradeStatus: int(server.UpgradeDisabledByConfigurationToPreserveDowngrade),
	})
}

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
	skip.UnderRace(t)
	ctx := context.Background()

	v1 := clusterversion.MinSupported.Version()
	v2 := clusterversion.Latest.Version()

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		v2,
		v1,
		false, // initializeVersion
	)
	// Initialize the version to the MinSupportedVersion.
	require.NoError(t, clusterversion.Initialize(ctx, v1, &settings.SV))

	t.Log("starting server")
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Settings:          settings,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         v1,
			},
			// Make the upgrade faster by accelerating jobs.
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer ts.Stopper().Stop(ctx)
	sysDB := sqlutils.MakeSQLRunner(ts.SQLConn(t))

	startAndConnectToTenant := func(t *testing.T, id uint64) (tenant serverutils.ApplicationLayerInterface, tenantDB *gosql.DB) {
		settings := cluster.MakeTestingClusterSettingsWithVersions(
			v2,
			v1,
			false, // initializeVersion
		)
		// Initialize the version to the minimum it could be.
		require.NoError(t, clusterversion.Initialize(ctx, v1, &settings.SV))
		tenantArgs := base.TestTenantArgs{
			TenantID: roachpb.MustMakeTenantID(id),
			TestingKnobs: base.TestingKnobs{
				// Make the upgrade faster by accelerating jobs.
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
				},
			},
			Settings: settings,
		}
		tenant, err := ts.TenantController().StartTenant(ctx, tenantArgs)
		require.NoError(t, err)
		return tenant, tenant.SQLConn(t)
	}

	t.Run("upgrade tenant", func(t *testing.T) {
		t.Log("create a tenant before upgrading anything")
		const initialTenantID = 10
		tenantServer, conn := startAndConnectToTenant(t, initialTenantID)
		db := sqlutils.MakeSQLRunner(conn)

		t.Log("ensure that the tenant works")
		db.CheckQueryResults(t, "SHOW CLUSTER SETTING version", [][]string{{v1.String()}})
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
		tenantServer, err := ts.TenantController().StartTenant(ctx, base.TestTenantArgs{
			TestingKnobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
				},
			},
			TenantID: roachpb.MustMakeTenantID(initialTenantID),
		})
		require.NoError(t, err)
		conn = tenantServer.SQLConn(t)
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
		tenant, err = ts.TenantController().StartTenant(ctx, base.TestTenantArgs{
			TenantID: roachpb.MustMakeTenantID(postUpgradeTenantID),
		})
		require.NoError(t, err)
		conn = tenant.SQLConn(t)

		t.Log("verify it still is at v2")
		sqlutils.MakeSQLRunner(conn).CheckQueryResults(t,
			"SHOW CLUSTER SETTING version", [][]string{{v2.String()}})
	})
}

// TestTenantUpgradeFailure exercises cases where the tenant dies
// between version upgrades.
func TestTenantUpgradeFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	v0 := clusterversion.MinSupported.Version()
	v2 := clusterversion.Latest.Version()
	// v1 needs to be between v0 and v2. Set it to the minor release
	// after v0 and before v2.
	var v1 roachpb.Version
	for _, version := range clusterversion.ListBetween(v0, v2) {
		if version.Minor != v0.Minor {
			v1 = version
			break
		}
	}
	if v1 == (roachpb.Version{}) {
		// There is no in-between version supported; skip this test.
		skip.IgnoreLint(t, "test can only run when we support two previous releases")
	}

	t.Log("starting server")
	ctx := context.Background()
	settings := cluster.MakeTestingClusterSettingsWithVersions(
		v2,
		v0,
		false, // initializeVersion
	)
	// Initialize the version to the MinSupportedVersion.
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Settings:          settings,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         v0,
			},
		},
	})
	defer ts.Stopper().Stop(ctx)
	sysDB := sqlutils.MakeSQLRunner(ts.SQLConn(t))

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
		slbase.DefaultTTL.Override(ctx, &settings.SV, 15*time.Second)
		slbase.DefaultHeartBeat.Override(ctx, &settings.SV, 500*time.Millisecond)
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
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
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
								}, upgrade.RestoreActionNotRequired("test"),
							), true
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
								}, upgrade.RestoreActionNotRequired("test"),
							), true
						default:
							return nil, false
						}
					},
				},
			},
			Settings: settings,
		}
		tenant, err := ts.TenantController().StartTenant(ctx, tenantArgs)
		require.NoError(t, err)
		tenantDB := tenant.SQLConn(t)
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
