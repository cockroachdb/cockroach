// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func makeBackupFixtureSpecs(override scheduledBackupSpecs) scheduledBackupSpecs {
	backupSpecs := makeBackupSpecs(override.backupSpecs, defaultBackupFixtureSpecs.backupSpecs)
	specs := scheduledBackupSpecs{
		backupSpecs:              backupSpecs,
		incrementalBackupCrontab: defaultBackupFixtureSpecs.incrementalBackupCrontab,
	}
	if override.incrementalBackupCrontab != "" {
		specs.incrementalBackupCrontab = override.incrementalBackupCrontab
	}
	specs.ignoreExistingBackups = override.ignoreExistingBackups
	return specs
}

// defaultBackupFixtureSpecs defines the default scheduled backup used to create a fixture.
var defaultBackupFixtureSpecs = scheduledBackupSpecs{
	// Runs an incremental backup every 5 minutes.
	incrementalBackupCrontab: "*/5 * * * *",

	// The default option of false prevents roachtest users from overriding the
	// latest backup in a collection, which may be used in restore roachtests.
	ignoreExistingBackups: false,

	backupSpecs: backupSpecs{
		version:           "v23.1.11",
		cloud:             spec.AWS,
		fullBackupDir:     "LATEST",
		numBackupsInChain: 48,
		workload: tpceRestore{
			customers: 25000,
		},
	},
}

const scheduleLabel = "schedule_cluster"

// fixtureFromMasterVersion should be used in the backupSpecs version field to
// create a fixture using the bleeding edge of master. In the backup fixture
// path on external storage, the {version} subdirectory will be equal to this
// value.
const fixtureFromMasterVersion = "latest"

type scheduledBackupSpecs struct {
	backupSpecs
	// ignoreExistingBackups if set to true, will allow a new backup chain
	// to get written to an already existing backup collection.
	ignoreExistingBackups    bool
	incrementalBackupCrontab string
}

func (sbs scheduledBackupSpecs) scheduledBackupCmd() string {
	// This backup schedule will first run a full backup immediately and then the
	// incremental backups at the given incrementalBackupCrontab cadence until the user cancels the
	// backup schedules. To ensure that only one full backup chain gets created,
	// begin the backup schedule at the beginning of the week, as a new full
	// backup will get created on Sunday at Midnight ;)
	options := ""
	if !sbs.nonRevisionHistory {
		options = "WITH revision_history"
	}
	backupCmd := fmt.Sprintf(`BACKUP INTO %s %s`, sbs.backupCollection(), options)
	cmd := fmt.Sprintf(`CREATE SCHEDULE %s FOR %s RECURRING '%s' FULL BACKUP '@weekly' WITH SCHEDULE OPTIONS first_run = 'now'`,
		scheduleLabel, backupCmd, sbs.incrementalBackupCrontab)
	if sbs.ignoreExistingBackups {
		cmd = cmd + ",ignore_existing_backups"
	}
	return cmd
}

type backupFixtureSpecs struct {
	// hardware specifies the roachprod specs to create the scheduledBackupSpecs fixture on.
	hardware hardwareSpecs

	// scheduledBackupSpecs specifies the scheduled scheduledBackupSpecs fixture which will be created.
	scheduledBackupSpecs scheduledBackupSpecs

	// initWorkloadViaRestore, if specified, initializes the cluster via restore
	// of an older fixture. The fields specified here will override any fields
	// specified in the scheduledBackupSpecs field above.
	initWorkloadViaRestore *restoreSpecs

	timeout time.Duration
	// A no-op, used only to set larger timeouts due roachtests limiting timeouts based on the suite
	suites registry.SuiteSet

	testName string

	// If non-empty, the test will be skipped with the supplied reason.
	skip string
}

func (bf *backupFixtureSpecs) initTestName() {
	bf.testName = fmt.Sprintf("backupFixture/%s/revision-history=%t/%s", bf.scheduledBackupSpecs.workload.String(), !bf.scheduledBackupSpecs.nonRevisionHistory, bf.scheduledBackupSpecs.cloud)
}

func makeBackupDriver(t test.Test, c cluster.Cluster, sp backupFixtureSpecs) backupDriver {
	return backupDriver{
		t:  t,
		c:  c,
		sp: sp,
	}
}

type backupDriver struct {
	sp backupFixtureSpecs

	t test.Test
	c cluster.Cluster
}

func (bd *backupDriver) prepareCluster(ctx context.Context) {
	if err := bd.sp.scheduledBackupSpecs.CloudIsCompatible(bd.c.Cloud()); err != nil {
		bd.t.Skip(err.Error())
	}
	version := clusterupgrade.CurrentVersion()
	if bd.sp.scheduledBackupSpecs.version != fixtureFromMasterVersion {
		version = clusterupgrade.MustParseVersion(bd.sp.scheduledBackupSpecs.version)
	}
	bd.t.L().Printf("Creating cluster with version %s", version)

	binaryPath, err := clusterupgrade.UploadCockroach(ctx, bd.t, bd.t.L(), bd.c,
		bd.sp.hardware.getCRDBNodes(), version)
	require.NoError(bd.t, err)

	require.NoError(bd.t, clusterupgrade.StartWithSettings(ctx, bd.t.L(), bd.c,
		bd.sp.hardware.getCRDBNodes(),
		option.NewStartOpts(option.NoBackupSchedule, option.DisableWALFailover),
		install.BinaryOption(binaryPath)))

	bd.assertCorrectCockroachBinary(ctx)
	if !bd.sp.scheduledBackupSpecs.ignoreExistingBackups {
		// This check allows the roachtest to fail fast, instead of when the
		// scheduled backup cmd is issued.
		require.False(bd.t, bd.checkForExistingBackupCollection(ctx), fmt.Sprintf("existing backup in collection %s", bd.sp.scheduledBackupSpecs.backupCollection()))
	}
}

// checkForExistingBackupCollection returns true if there exists a backup in the collection path.
func (bd *backupDriver) checkForExistingBackupCollection(ctx context.Context) bool {
	collectionQuery := fmt.Sprintf(`SELECT count(*) FROM [SHOW BACKUPS IN %s]`,
		bd.sp.scheduledBackupSpecs.backupCollection())
	conn := bd.c.Conn(ctx, bd.t.L(), 1)
	sql := sqlutils.MakeSQLRunner(conn)
	var collectionCount int
	sql.QueryRow(bd.t, collectionQuery).Scan(&collectionCount)
	return collectionCount > 0
}

func (bd *backupDriver) assertCorrectCockroachBinary(ctx context.Context) {
	binaryQuery := "SELECT value FROM crdb_internal.node_build_info WHERE field = 'Version'"
	conn := bd.c.Conn(ctx, bd.t.L(), 1)
	sql := sqlutils.MakeSQLRunner(conn)
	var binaryVersion string
	sql.QueryRow(bd.t, binaryQuery).Scan(&binaryVersion)
	if bd.sp.scheduledBackupSpecs.version != fixtureFromMasterVersion {
		require.Equal(bd.t, bd.sp.scheduledBackupSpecs.version, binaryVersion, "cluster not running on expected binary")
	} else {
		require.Contains(bd.t, binaryVersion, "dev")
	}
}

func (bd *backupDriver) initWorkload(ctx context.Context) {
	if bd.sp.initWorkloadViaRestore == nil {
		bd.t.L().Printf(`Initializing workload`)
		bd.sp.scheduledBackupSpecs.workload.init(ctx, bd.t, bd.c, bd.sp.hardware)
		return
	}
	computedRestoreSpecs := restoreSpecs{
		hardware:               bd.sp.hardware,
		backup:                 makeBackupSpecs(bd.sp.initWorkloadViaRestore.backup, bd.sp.scheduledBackupSpecs.backupSpecs),
		restoreUptoIncremental: bd.sp.initWorkloadViaRestore.restoreUptoIncremental,
	}
	restoreDriver := makeRestoreDriver(bd.t, bd.c, computedRestoreSpecs)
	bd.t.L().Printf(`Initializing workload via restore`)
	restoreDriver.getAOST(ctx)
	// Only restore the database because a cluster restore will also restore the
	// scheduled_jobs system table, which will automatically begin any backed up
	// backup schedules, which complicates fixture generation.
	target := fmt.Sprintf("DATABASE %s", restoreDriver.sp.backup.workload.DatabaseName())
	require.NoError(bd.t, restoreDriver.run(ctx, target))
}

func (bd *backupDriver) runWorkload(ctx context.Context) error {
	return bd.sp.scheduledBackupSpecs.workload.run(ctx, bd.t, bd.c, bd.sp.hardware)
}

// scheduleBackups begins the backup schedule.
func (bd *backupDriver) scheduleBackups(ctx context.Context) {
	conn := bd.c.Conn(ctx, bd.t.L(), 1)
	sql := sqlutils.MakeSQLRunner(conn)
	sql.Exec(bd.t, bd.sp.scheduledBackupSpecs.scheduledBackupCmd())
}

// monitorBackups pauses the schedule once the target number of backups in the
// chain have been taken.
func (bd *backupDriver) monitorBackups(ctx context.Context) {
	conn := bd.c.Conn(ctx, bd.t.L(), 1)
	sql := sqlutils.MakeSQLRunner(conn)
	for {
		time.Sleep(1 * time.Minute)
		var activeScheduleCount int
		scheduleCountQuery := fmt.Sprintf(`SELECT count(*) FROM [SHOW SCHEDULES] WHERE label='%s' AND schedule_status='ACTIVE'`, scheduleLabel)
		sql.QueryRow(bd.t, scheduleCountQuery).Scan(&activeScheduleCount)
		if activeScheduleCount < 2 {
			bd.t.L().Printf(`First full backup still running`)
			continue
		}
		var backupCount int
		backupCountQuery := fmt.Sprintf(`SELECT count(DISTINCT end_time) FROM [SHOW BACKUP FROM LATEST IN %s]`, bd.sp.scheduledBackupSpecs.backupCollection())
		sql.QueryRow(bd.t, backupCountQuery).Scan(&backupCount)
		bd.t.L().Printf(`%d scheduled backups taken`, backupCount)
		if backupCount >= bd.sp.scheduledBackupSpecs.numBackupsInChain {
			pauseSchedulesQuery := fmt.Sprintf(`PAUSE SCHEDULES WITH x AS (SHOW SCHEDULES) SELECT id FROM x WHERE label = '%s'`, scheduleLabel)
			sql.QueryRow(bd.t, pauseSchedulesQuery)
			break
		}
	}
}

func registerBackupFixtures(r registry.Registry) {
	for _, bf := range []backupFixtureSpecs{
		{
			// 400GB backup fixture with 48 incremental layers. This is used by
			// - restore/tpce/400GB/aws/inc-count=48/nodes=4/cpus=8
			// - restore/tpce/400GB/aws/nodes=4/cpus=16
			// - restore/tpce/400GB/aws/nodes=4/cpus=8
			// - restore/tpce/400GB/aws/nodes=8/cpus=8
			hardware: makeHardwareSpecs(hardwareSpecs{workloadNode: true}),
			scheduledBackupSpecs: makeBackupFixtureSpecs(scheduledBackupSpecs{
				backupSpecs: backupSpecs{
					version: fixtureFromMasterVersion},
			}),
			timeout: 5 * time.Hour,
			initWorkloadViaRestore: &restoreSpecs{
				backup:                 backupSpecs{version: "v22.2.0", numBackupsInChain: 48},
				restoreUptoIncremental: 48,
			},
			skip:   "only for fixture generation",
			suites: registry.Suites(registry.Nightly),
		},
		{
			// 400GB backup fixture, no revision history, with 48 incremental layers.
			// This will used by the online restore roachtests. During 24.2
			// development, we can use it to enable OR of incremental backups.
			hardware: makeHardwareSpecs(hardwareSpecs{workloadNode: true}),
			scheduledBackupSpecs: makeBackupFixtureSpecs(scheduledBackupSpecs{
				backupSpecs: backupSpecs{
					version:            fixtureFromMasterVersion,
					nonRevisionHistory: true,
				},
			}),
			timeout: 5 * time.Hour,
			initWorkloadViaRestore: &restoreSpecs{
				backup: backupSpecs{
					version:           fixtureFromMasterVersion,
					numBackupsInChain: 48,
				},
				restoreUptoIncremental: 12,
			},
			skip:   "only for fixture generation",
			suites: registry.Suites(registry.Nightly),
		},
		{
			// 8TB backup fixture, no revision history, with 48 incremental layers.
			// This will used by the online restore roachtests. During 24.2
			// development, we can use it to enable OR of incremental backups.
			hardware: makeHardwareSpecs(hardwareSpecs{nodes: 10, volumeSize: 1500, workloadNode: true}),
			scheduledBackupSpecs: makeBackupFixtureSpecs(scheduledBackupSpecs{
				backupSpecs: backupSpecs{
					version:            fixtureFromMasterVersion,
					nonRevisionHistory: true,
					workload:           tpceRestore{customers: 500000},
				},
			}),
			timeout: 23 * time.Hour,
			initWorkloadViaRestore: &restoreSpecs{
				backup: backupSpecs{
					version:            "v23.1.11",
					numBackupsInChain:  48,
					nonRevisionHistory: true,
				},
				restoreUptoIncremental: 12,
			},
			skip:   "only for fixture generation",
			suites: registry.Suites(registry.Weekly),
		},
		{
			// 8TB Backup Fixture.
			hardware: makeHardwareSpecs(hardwareSpecs{nodes: 10, volumeSize: 2000, workloadNode: true}),
			scheduledBackupSpecs: makeBackupFixtureSpecs(scheduledBackupSpecs{
				backupSpecs: backupSpecs{
					version:  fixtureFromMasterVersion,
					workload: tpceRestore{customers: 500000}}}),
			timeout: 25 * time.Hour,
			initWorkloadViaRestore: &restoreSpecs{
				backup:                 backupSpecs{version: "v22.2.1", numBackupsInChain: 48},
				restoreUptoIncremental: 48,
			},
			// Use weekly to allow an over 24 hour timeout.
			suites: registry.Suites(registry.Weekly),
			skip:   "only for fixture generation",
		},
		{
			// Default Fixture, Run on GCE. Initiated by the tpce --init.
			hardware: makeHardwareSpecs(hardwareSpecs{workloadNode: true}),
			scheduledBackupSpecs: makeBackupFixtureSpecs(scheduledBackupSpecs{
				backupSpecs: backupSpecs{cloud: spec.GCE}}),
			// TODO(radu): this should be OnlyGCE.
			suites:  registry.Suites(registry.Nightly),
			timeout: 5 * time.Hour,
			skip:    "only for fixture generation",
		},
		{
			// 32TB Backup Fixture.
			hardware: makeHardwareSpecs(hardwareSpecs{nodes: 15, cpus: 16, volumeSize: 5000, workloadNode: true}),
			scheduledBackupSpecs: makeBackupFixtureSpecs(scheduledBackupSpecs{
				backupSpecs: backupSpecs{workload: tpceRestore{customers: 2000000}}}),
			initWorkloadViaRestore: &restoreSpecs{
				backup:                 backupSpecs{version: "v22.2.1", numBackupsInChain: 48},
				restoreUptoIncremental: 48,
			},
			// Use weekly to allow an over 24 hour timeout.
			suites:  registry.Suites(registry.Weekly),
			timeout: 48 * time.Hour,
			skip:    "only for fixture generation",
		},
		{
			hardware: makeHardwareSpecs(hardwareSpecs{workloadNode: true}),
			scheduledBackupSpecs: makeBackupFixtureSpecs(scheduledBackupSpecs{
				backupSpecs: backupSpecs{
					workload:           tpccRestore{opts: tpccRestoreOptions{warehouses: 5000}},
					nonRevisionHistory: true,
				},
			}),
			initWorkloadViaRestore: &restoreSpecs{
				backup:                 backupSpecs{version: "v23.1.1", numBackupsInChain: 48},
				restoreUptoIncremental: 48,
			},
			timeout: 1 * time.Hour,
			suites:  registry.Suites(registry.Nightly),
			skip:    "only for fixture generation",
		},
	} {
		bf := bf
		bf.initTestName()
		r.Add(registry.TestSpec{
			Name:              bf.testName,
			Owner:             registry.OwnerDisasterRecovery,
			Cluster:           bf.hardware.makeClusterSpecs(r, bf.scheduledBackupSpecs.cloud),
			Timeout:           bf.timeout,
			EncryptionSupport: registry.EncryptionMetamorphic,
			CompatibleClouds:  bf.scheduledBackupSpecs.CompatibleClouds(),
			Suites:            bf.suites,
			Skip:              bf.skip,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {

				bd := makeBackupDriver(t, c, bf)
				bd.prepareCluster(ctx)
				bd.initWorkload(ctx)

				workloadCtx, workloadCancel := context.WithCancel(ctx)
				m := c.NewMonitor(workloadCtx)
				defer func() {
					workloadCancel()
					m.Wait()
				}()
				m.Go(func(ctx context.Context) error {
					err := bd.runWorkload(ctx)
					// We expect the workload to return a context cancelled error because
					// the roachtest driver cancels the monitor's context after the backup
					// schedule completes.
					if err != nil && ctx.Err() == nil {
						// Implies the workload context was not cancelled and the workload cmd returned a
						// different error.
						return errors.Wrapf(err, `Workload context was not cancelled. Error returned by workload cmd`)
					}
					bd.t.L().Printf("workload successfully finished")
					return nil
				})
				bd.scheduleBackups(ctx)
				bd.monitorBackups(ctx)
			},
		})
	}
}
