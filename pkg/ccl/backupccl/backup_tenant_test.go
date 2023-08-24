// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	_ "github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/jobutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestBackupTenantImportingTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
	if _, err := sqlDB.DB.ExecContext(ctx, `BACKUP TENANT 10 TO $1`, dst); err != nil {
		t.Fatal(err)
	}
	// Destroy the tenant, then restore it.
	tSrv.AppStopper().Stop(ctx)
	if _, err := sqlDB.DB.ExecContext(ctx, "ALTER TENANT [10] STOP SERVICE; DROP TENANT [10] IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.DB.ExecContext(ctx, "RESTORE TENANT 10 FROM $1", dst); err != nil {
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
		if status == string(jobs.StatusCanceled) {
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

	skip.UnderStressRace(t, "test is too heavy to run under stress")

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
		setAndWaitForTenantReadOnlyClusterSetting(
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
		setAndWaitForTenantReadOnlyClusterSetting(
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
				"SET CLUSTER SETTING %s = 'us-east1'", sql.DefaultPrimaryRegionClusterSettingName,
			),
		)
		setAndWaitForTenantReadOnlyClusterSetting(
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
