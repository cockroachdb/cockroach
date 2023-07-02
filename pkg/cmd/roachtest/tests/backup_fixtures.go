// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"strings"
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
		version:         "v23.1.1",
		cloud:           spec.AWS,
		fullBackupDir:   "LATEST",
		backupsIncluded: 48,
		workload: tpceRestore{
			customers: 25000,
		},
	},
}

const scheduleLabel = "schedule_cluster"

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
	var ignoreExistingBackupsOpt string
	if sbs.ignoreExistingBackups {
		ignoreExistingBackupsOpt = "ignore_existing_backups"
	}
	backupCmd := fmt.Sprintf(`BACKUP INTO %s WITH revision_history`, sbs.backupCollection())
	cmd := fmt.Sprintf(`CREATE SCHEDULE %s FOR %s RECURRING '%s' FULL BACKUP '@weekly' WITH SCHEDULE OPTIONS first_run = 'now', %s`,
		scheduleLabel, backupCmd, sbs.incrementalBackupCrontab, ignoreExistingBackupsOpt)
	return cmd
}

type backupFixtureSpecs struct {
	// hardware specifies the roachprod specs to create the backup fixture on.
	hardware hardwareSpecs

	// backup specifies the scheduled backup fixture which will be created.
	backup scheduledBackupSpecs

	// initFromBackupSpecs, if specified, initializes the cluster via restore of an older fixture.
	// The fields specified here will override any fields specified in the backup field above.
	initFromBackupSpecs backupSpecs

	timeout  time.Duration
	tags     map[string]struct{}
	testName string

	// If non-empty, the test will be skipped with the supplied reason.
	skip string
}

func (bf *backupFixtureSpecs) initTestName() {
	bf.testName = "backupFixture/" + bf.backup.workload.String() + "/" + bf.backup.cloud
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

	if bd.c.Spec().Cloud != bd.sp.backup.cloud {
		// For now, only run the test on the cloud provider that also stores the backup.
		bd.t.Skip(fmt.Sprintf("test configured to run on %s", bd.sp.backup.cloud))
	}
	version := strings.Replace(bd.sp.backup.version, "v", "", 1)
	binaryPath, err := clusterupgrade.UploadVersion(ctx, bd.t, bd.t.L(), bd.c,
		bd.sp.hardware.getCRDBNodes(), version)
	require.NoError(bd.t, err)

	require.NoError(bd.t, clusterupgrade.StartWithSettings(ctx, bd.t.L(), bd.c,
		bd.sp.hardware.getCRDBNodes(),
		option.DefaultStartOptsNoBackups(),
		install.BinaryOption(binaryPath)))

	bd.assertCorrectCockroachBinary(ctx)
	if !bd.sp.backup.ignoreExistingBackups {
		// This check allows the roachtest to fail fast, instead of when the
		// scheduled backup cmd is issued.
		require.False(bd.t, bd.checkForExistingBackupCollection(ctx))
	}
}

// checkForExistingBackupCollection returns true if there exists a backup in the collection path.
func (bd *backupDriver) checkForExistingBackupCollection(ctx context.Context) bool {
	collectionQuery := fmt.Sprintf(`SELECT count(*) FROM [SHOW BACKUPS IN %s]`,
		bd.sp.backup.backupCollection())
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
	require.Equal(bd.t, bd.sp.backup.version, binaryVersion, "cluster not running on expected binary")
}

func (bd *backupDriver) initWorkload(ctx context.Context) {
	if bd.sp.initFromBackupSpecs.version == "" {
		bd.t.L().Printf(`Initializing workload`)
		bd.sp.backup.workload.init(ctx, bd.t, bd.c, bd.sp.hardware)
		return
	}
	bd.t.L().Printf(`Initializing workload via restore`)
	restoreDriver := makeRestoreDriver(bd.t, bd.c, restoreSpecs{
		hardware: bd.sp.hardware,
		backup:   makeBackupSpecs(bd.sp.initFromBackupSpecs, bd.sp.backup.backupSpecs),
	})
	restoreDriver.getAOST(ctx)
	// Only restore the database because a cluster restore will also restore the
	// scheduled_jobs system table, which will automatically begin any backed up
	// backup schedules, which complicates fixture generation.
	target := fmt.Sprintf("DATABASE %s", restoreDriver.sp.backup.workload.DatabaseName())
	require.NoError(bd.t, restoreDriver.run(ctx, target))
}

func (bd *backupDriver) runWorkload(ctx context.Context) error {
	return bd.sp.backup.workload.run(ctx, bd.t, bd.c, bd.sp.hardware)
}

// scheduleBackups begins the backup schedule.
func (bd *backupDriver) scheduleBackups(ctx context.Context) {
	conn := bd.c.Conn(ctx, bd.t.L(), 1)
	sql := sqlutils.MakeSQLRunner(conn)
	sql.Exec(bd.t, bd.sp.backup.scheduledBackupCmd())
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
		backupCountQuery := fmt.Sprintf(`SELECT count(DISTINCT end_time) FROM [SHOW BACKUP FROM LATEST IN %s]`, bd.sp.backup.backupCollection())
		sql.QueryRow(bd.t, backupCountQuery).Scan(&backupCount)
		bd.t.L().Printf(`%d scheduled backups taken`, backupCount)
		if backupCount >= bd.sp.backup.backupsIncluded {
			pauseSchedulesQuery := fmt.Sprintf(`PAUSE SCHEDULES WITH x AS (SHOW SCHEDULES) SELECT id FROM x WHERE label = '%s'`, scheduleLabel)
			sql.QueryRow(bd.t, pauseSchedulesQuery)
			break
		}
	}
}

func registerBackupFixtures(r registry.Registry) {
	for _, bf := range []backupFixtureSpecs{
		{
			// Default AWS Backup Fixture
			hardware:            makeHardwareSpecs(hardwareSpecs{workloadNode: true}),
			backup:              makeBackupFixtureSpecs(scheduledBackupSpecs{}),
			timeout:             5 * time.Hour,
			initFromBackupSpecs: backupSpecs{version: "v22.2.0"},
			skip:                "only for fixture generation",
			tags:                registry.Tags("aws"),
		},
		{
			// Default Fixture, Run on GCE. Initiated by the tpce --init.
			hardware: makeHardwareSpecs(hardwareSpecs{workloadNode: true}),
			backup: makeBackupFixtureSpecs(scheduledBackupSpecs{
				backupSpecs: backupSpecs{
					cloud: spec.GCE}}),
			timeout: 5 * time.Hour,
			skip:    "only for fixture generation",
		},
		{
			// 15 GB Backup Fixture. Note, this fixture is created weekly to
			// ensure the fixture generation code works.
			hardware: makeHardwareSpecs(hardwareSpecs{workloadNode: true, cpus: 4}),
			backup: makeBackupFixtureSpecs(
				scheduledBackupSpecs{
					incrementalBackupCrontab: "*/2 * * * *",
					ignoreExistingBackups:    true,
					backupSpecs: backupSpecs{
						backupsIncluded: 4,
						workload:        tpceRestore{customers: 1000}}}),
			initFromBackupSpecs: backupSpecs{version: "v22.2.1", backupProperties: "inc-count=48"},
			timeout:             2 * time.Hour,
			tags:                registry.Tags("weekly", "aws-weekly"),
		},
		{
			// 8TB Backup Fixture.
			hardware: makeHardwareSpecs(hardwareSpecs{nodes: 10, volumeSize: 2000, workloadNode: true}),
			backup: makeBackupFixtureSpecs(scheduledBackupSpecs{
				backupSpecs: backupSpecs{
					workload: tpceRestore{customers: 500000}}}),
			timeout:             25 * time.Hour,
			initFromBackupSpecs: backupSpecs{version: "v22.2.1", backupProperties: "inc-count=48"},
			// add the weekly tags to allow an over 24 hour timeout.
			tags: registry.Tags("weekly", "aws-weekly"),
			skip: "only for fixture generation",
		},
		{
			// 32TB Backup Fixture.
			hardware: makeHardwareSpecs(hardwareSpecs{nodes: 15, cpus: 16, volumeSize: 5000, workloadNode: true}),
			backup: makeBackupFixtureSpecs(scheduledBackupSpecs{
				backupSpecs: backupSpecs{
					workload: tpceRestore{customers: 2000000}}}),
			initFromBackupSpecs: backupSpecs{version: "v22.2.1", backupProperties: "inc-count=48"},
			timeout:             48 * time.Hour,
			// add the weekly tags to allow an over 24 hour timeout.
			tags: registry.Tags("weekly", "aws-weekly"),
			skip: "only for fixture generation",
		},
	} {
		bf := bf
		bf.initTestName()
		r.Add(registry.TestSpec{
			Name:              bf.testName,
			Owner:             registry.OwnerDisasterRecovery,
			Cluster:           bf.hardware.makeClusterSpecs(r, bf.backup.cloud),
			Timeout:           bf.timeout,
			EncryptionSupport: registry.EncryptionMetamorphic,
			Tags:              bf.tags,
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
