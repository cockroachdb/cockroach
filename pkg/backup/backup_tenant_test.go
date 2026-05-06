// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	_ "github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestBackupTenantImportingTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(at): enable once LinkExternalSSTable is permitted for secondary tenants.
	backuptestutils.DisableFastRestoreForTest(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
			},
		})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	tenantID := roachpb.MustMakeTenantID(10)
	tSrv, tSQL := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantID:     tenantID,
		TestingKnobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
	})
	defer tSQL.Close()
	runner := sqlutils.MakeSQLRunner(tSQL)

	if _, err := tSQL.Exec("SET CLUSTER SETTING jobs.debug.pausepoints = 'import.after_ingest';"); err != nil {
		t.Fatal(err)
	}
	if _, err := tSQL.Exec("CREATE TABLE x (id INT PRIMARY KEY, n INT, s STRING)"); err != nil {
		t.Fatal(err)
	}
	if _, err := tSQL.Exec("INSERT INTO x VALUES (1000, 1, 'existing')"); err != nil {
		t.Fatal(err)
	}
	if _, err := tSQL.Exec("IMPORT INTO x CSV DATA ('workload:///csv/bank/bank?rows=100&version=1.0.0')"); !testutils.IsError(err, "pause") {
		t.Fatal(err)
	}
	var jobID jobspb.JobID
	err := tSQL.QueryRow(`SELECT job_id FROM [show jobs] WHERE job_type = 'IMPORT'`).Scan(&jobID)
	require.NoError(t, err)
	jobutils.WaitForJobToPause(t, runner, jobID)

	// tenant now has a fully ingested, paused import, so back them up.
	const dst = "userfile:///t"
	if _, err := sqlDB.DB.ExecContext(ctx, `BACKUP TENANT 10 INTO $1`, dst); err != nil {
		t.Fatal(err)
	}
	// Destroy the tenant, then restore it.
	tSrv.AppStopper().Stop(ctx)
	if _, err := sqlDB.DB.ExecContext(ctx, "ALTER TENANT [10] STOP SERVICE"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.DB.ExecContext(ctx, "DROP TENANT [10] IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.DB.ExecContext(ctx, "RESTORE TENANT 10 FROM LATEST IN $1", dst); err != nil {
		t.Fatal(err)
	}

	if err := tc.Server(0).TenantController().WaitForTenantReadiness(ctx, tenantID); err != nil {
		t.Fatal(err)
	}

	_, tSQL = serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantID:     tenantID,
		TestingKnobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
	})
	defer tSQL.Close()

	if _, err := tSQL.Exec(`UPDATE system.jobs SET claim_session_id = NULL, claim_instance_id = NULL WHERE id = $1`, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := tSQL.Exec(`DELETE FROM system.lease`); err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		if _, err := tSQL.Exec(`CANCEL JOB $1`, jobID); err != nil {
			return err
		}

		var status string
		if err := tSQL.QueryRow(`SELECT status FROM [show jobs] WHERE job_id = $1`, jobID).Scan(&status); err != nil {
			return err
		}
		if status == string(jobs.StateCanceled) {
			return nil
		}
		return errors.Newf("%s", status)
	})

	var rowCount int
	if err := tSQL.QueryRow(`SELECT count(*) FROM x`).Scan(&rowCount); err != nil {
		t.Fatal(err)
	}
	require.Equal(t, 1, rowCount)
}

// TestTenantBackupMultiRegionDatabases ensures secondary tenants restoring
// MR databases respect the
// sql.virtual_cluster.feature_access.multiregion.enabled cluster
// setting.
func TestTenantBackupMultiRegionDatabases(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "test is too heavy to run under stress")

	// TODO(at): enable once LinkExternalSSTable is permitted for secondary tenants.
	backuptestutils.DisableFastRestoreForTest(t)

	tc, db, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /*numServers*/, base.TestingKnobs{},
	)
	defer cleanup()
	sqlDB := sqlutils.MakeSQLRunner(db)

	tenID := roachpb.MustMakeTenantID(10)
	_, tSQL := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantID:     tenID,
		TestingKnobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
	})
	defer tSQL.Close()
	tenSQLDB := sqlutils.MakeSQLRunner(tSQL)

	// Setup.
	const tenDst = "userfile:///ten_backup"
	const hostDst = "userfile:///host_backup"
	tenSQLDB.Exec(t, `CREATE DATABASE mrdb PRIMARY REGION "us-east1"`)
	tenSQLDB.Exec(t, fmt.Sprintf("BACKUP DATABASE mrdb INTO '%s'", tenDst))

	sqlDB.Exec(t, `CREATE DATABASE mrdb PRIMARY REGION "us-east1"`)
	sqlDB.Exec(t, fmt.Sprintf("BACKUP DATABASE mrdb INTO '%s'", hostDst))

	{
		// Flip the tenant-read only cluster setting; ensure database can be restored
		// on the system tenant but not on the secondary tenant.
		setAndWaitForSystemVisibleClusterSetting(
			t,
			sql.SecondaryTenantsMultiRegionAbstractionsEnabledSettingName,
			sqlDB,
			tenSQLDB,
			tenID,
			"false",
		)

		tenSQLDB.Exec(t, "DROP DATABASE mrdb CASCADE")
		tenSQLDB.ExpectErr(
			t,
			"setting .* disallows secondary tenant to restore a multi-region database",
			fmt.Sprintf("RESTORE DATABASE mrdb FROM LATEST IN '%s'", tenDst),
		)

		// The system tenant should remain unaffected.
		sqlDB.Exec(t, "DROP DATABASE mrdb CASCADE")
		sqlDB.Exec(t, fmt.Sprintf("RESTORE DATABASE mrdb FROM LATEST IN '%s'", hostDst))
	}

	{
		// Flip the tenant-read only cluster setting back to true and ensure the
		// restore succeeds.
		setAndWaitForSystemVisibleClusterSetting(
			t,
			sql.SecondaryTenantsMultiRegionAbstractionsEnabledSettingName,
			sqlDB,
			tenSQLDB,
			tenID,
			"true",
		)

		tenSQLDB.Exec(t, fmt.Sprintf("RESTORE DATABASE mrdb FROM LATEST IN '%s'", tenDst))
	}

	{
		// Ensure tenant's restoring non multi-region databases are unaffected
		// by this setting. We set sql.defaults.primary_region for good measure.
		tenSQLDB.Exec(
			t,
			fmt.Sprintf(
				"SET CLUSTER SETTING %s = 'us-east1'", sqlclustersettings.DefaultPrimaryRegionClusterSettingName,
			),
		)
		setAndWaitForSystemVisibleClusterSetting(
			t,
			sql.SecondaryTenantsMultiRegionAbstractionsEnabledSettingName,
			sqlDB,
			tenSQLDB,
			tenID,
			"false",
		)

		tenSQLDB.Exec(t, "CREATE DATABASE nonMrDB")
		tenSQLDB.Exec(t, fmt.Sprintf("BACKUP DATABASE nonMrDB INTO '%s'", tenDst))

		tenSQLDB.Exec(t, "DROP DATABASE nonMrDB CASCADE")
		tenSQLDB.Exec(t, fmt.Sprintf("RESTORE DATABASE nonMrDB FROM LATEST IN '%s'", tenDst))
	}
}

// TestBackupTenantDroppedExcludeFromBackupTable pins down a bug where a
// host-issued tenant backup would fail with BatchTimestampBeforeGCError after
// a table marked exclude_data_from_backup was dropped while the backup's
// protected timestamp was held.
//
// The bug had two layers, and the test exercises both:
//
//  1. The declarative schema changer cleared
//     descriptor.ExcludeDataFromBackup as part of DROP TABLE (each
//     TableStorageParam element transitioning PUBLIC -> ABSENT emitted a
//     ResetTableStorageParam op, which mutated the soon-to-be-deleted
//     descriptor). This made the spanconfig reconciler regenerate the
//     span config record without the flag, which made KV stop attaching
//     DataExcludedFromBackup to the GC error, which made the backup
//     processor unable to recover.
//
//  2. Even with the descriptor field preserved, the GC job was free to
//     delete the descriptor and zone config while the backup PTS was
//     still held (because both KVSubscriber and the gcjob's isProtected
//     filter out backup PTSes that opt into IgnoreIfExcludedFromBackup
//     for excluded spans). Removing the descriptor took the span config
//     record with it, breaking the backup the same way as (1).
//
// The fix is in two parts: the schema changer no-ops storage-param resets
// on a descriptor that's already been marked DROP, and the GC job blocks
// descriptor cleanup until any covering PTS (under a "descriptor cleanup"
// scope that does not honor IgnoreIfExcludedFromBackup) is released.
//
// The test pauses a tenant backup after its PTS is written, drops the
// excluded table inside the tenant, force-advances MVCC GC past the PTS
// time, then resumes the backup and requires it to complete.
func TestBackupTenantDroppedExcludeFromBackupTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(at): enable once LinkExternalSSTable is permitted for secondary tenants.
	backuptestutils.DisableFastRestoreForTest(t)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
		},
	})
	defer tc.Stopper().Stop(ctx)
	hostDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	// Speed up PTS reconciliation and closed timestamps so the host's KV
	// nodes quickly observe span-config and PTS changes coming from the
	// tenant. The GC poll interval is system-visible, so set it from the
	// host as well; the tenant inherits the value.
	hostDB.Exec(t, `SET CLUSTER SETTING kv.protectedts.poll_interval = '10ms'`)
	hostDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'`)
	hostDB.Exec(t, `SET CLUSTER SETTING sql.gc_job.wait_for_gc.interval = '1s'`)

	tenantID := roachpb.MustMakeTenantID(10)
	_, tSQL := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantID:     tenantID,
		TestingKnobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
	})
	defer tSQL.Close()
	tenantDB := sqlutils.MakeSQLRunner(tSQL)

	// Drop the GC TTL inside the tenant so MVCC GC can quickly catch up
	// after the table is dropped. Disable range merges so the dropped
	// table's range stays distinct from the rest of the tenant; otherwise
	// the post-clear merge would mask the test bug behind the replica's
	// cached span config no longer reporting ExcludeDataFromBackup.
	tenantDB.Exec(t, `ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 1`)
	hostDB.Exec(t, `SET CLUSTER SETTING kv.range_merge.queue.enabled = false`)

	// Create an excluded table inside the tenant with some data, plus a
	// trailing sibling table so the excluded table's range is bounded on
	// both sides (otherwise r100 would extend to /Tenant/11/Max and its
	// cached span config would not necessarily report
	// ExcludeDataFromBackup, masking the bug).
	tenantDB.Exec(t, `CREATE DATABASE db`)
	tenantDB.Exec(t, `USE db`)
	tenantDB.Exec(t, `CREATE TABLE t (k INT PRIMARY KEY, v STRING)`)
	tenantDB.Exec(t, `ALTER TABLE t SET (exclude_data_from_backup = true)`)
	tenantDB.Exec(t, `INSERT INTO t SELECT generate_series(1, 100), 'initial'`)
	tenantDB.Exec(t, `CREATE TABLE bookend (k INT PRIMARY KEY)`)

	// The table needs its own range so the ExcludeDataFromBackup span
	// config applies and the GC queue treats it independently.
	waitForTableSplit(t, tSQL, "t", "db")
	waitForTableSplit(t, tSQL, "bookend", "db")

	// Pause the tenant backup just after its PTS has been written but
	// before its export flow starts.
	hostDB.Exec(t, `SET CLUSTER SETTING jobs.debug.pausepoints = 'backup.before.flow'`)
	const backupURI = "userfile:///exclude-tenant-test"
	var backupJobID jobspb.JobID
	hostDB.QueryRow(t,
		fmt.Sprintf(`BACKUP TENANT 10 INTO '%s' WITH detached`, backupURI),
	).Scan(&backupJobID)
	jobutils.WaitForJobToPause(t, hostDB, backupJobID)
	hostDB.Exec(t, `RESET CLUSTER SETTING jobs.debug.pausepoints`)

	// Wait for the host's span config reconciler to pick up the backup's
	// PTS record as a system span config. The GC job's descriptor-cleanup
	// gate reads PTS info from span configs (an async derived view), so
	// the protection must be visible there before we trigger GC.
	kvAccessor := tc.Server(0).SpanConfigKVAccessor().(spanconfig.KVAccessor)
	testutils.SucceedsSoon(t, func() error {
		configs, err := kvAccessor.GetAllSystemSpanConfigsThatApply(ctx, tenantID)
		if err != nil {
			return err
		}
		for _, cfg := range configs {
			for _, pp := range cfg.GCPolicy.ProtectionPolicies {
				if pp.IgnoreIfExcludedFromBackup {
					return nil
				}
			}
		}
		return errors.New("backup PTS not yet reconciled into system span configs")
	})

	// Drop the table while the backup PTS is held. The tenant can't call
	// crdb_internal.kv_enqueue_replica itself (host-only), so capture the
	// range ID from the tenant and drive GC from the host side.
	var rangeID int
	tenantDB.QueryRow(t, `SELECT range_id FROM [SHOW RANGES FROM TABLE db.t]`).Scan(&rangeID)
	tenantDB.Exec(t, `DROP TABLE db.t`)

	// Repeatedly enqueue the range for MVCC GC. Because the span is
	// excluded, MVCC GC ignores the host's backup PTS and reclaims the
	// data, advancing the GC threshold past the backup's read timestamp.
	// This is what causes the resumed backup to hit BatchTimestampBeforeGCError;
	// the test then verifies that the error carries DataExcludedFromBackup
	// so the backup processor can swallow it.
	const enqueueGC = `SELECT crdb_internal.kv_enqueue_replica($1, 'mvccGC', true, true)`
	deadline := timeutil.Now().Add(10 * time.Second)
	for timeutil.Now().Before(deadline) {
		hostDB.Exec(t, enqueueGC, rangeID)
		time.Sleep(500 * time.Millisecond)
	}

	hostDB.Exec(t, `RESUME JOB $1`, backupJobID)
	jobutils.WaitForJobToSucceed(t, hostDB, backupJobID)
}
