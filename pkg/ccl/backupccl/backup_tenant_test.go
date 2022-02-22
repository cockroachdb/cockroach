// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	_ "github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	tSrv, tSQL := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantID:     roachpb.MakeTenantID(10),
		TestingKnobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
	})
	defer tSQL.Close()

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
	var jobID int
	if err := tSQL.QueryRow(`SELECT job_id FROM [show jobs] WHERE job_type = 'IMPORT'`).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	tc.Servers[0].JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
	// wait for it to pause

	testutils.SucceedsSoon(t, func() error {
		var status string
		if err := tSQL.QueryRow(`SELECT status FROM [show jobs] WHERE job_id = $1`, jobID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == string(jobs.StatusPaused) {
			return nil
		}
		return errors.Newf("%s", status)
	})

	// tenant now has a fully ingested, paused import, so back them up.
	const dst = "userfile:///t"
	if _, err := sqlDB.DB.ExecContext(ctx, `BACKUP TENANT 10 TO $1`, dst); err != nil {
		t.Fatal(err)
	}
	// Destroy the tenant, then restore it.
	tSrv.Stopper().Stop(ctx)
	if _, err := sqlDB.DB.ExecContext(ctx, "SELECT crdb_internal.destroy_tenant(10, true)"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.DB.ExecContext(ctx, "RESTORE TENANT 10 FROM $1", dst); err != nil {
		t.Fatal(err)
	}
	tSrv, tSQL = serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{
		TenantID:     roachpb.MakeTenantID(10),
		Existing:     true,
		TestingKnobs: base.TestingKnobs{JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals()},
	})
	defer tSQL.Close()

	if _, err := tSQL.Exec(`UPDATE system.jobs SET claim_session_id = NULL, claim_instance_id = NULL WHERE id = $1`, jobID); err != nil {
		t.Fatal(err)
	}
	if _, err := tSQL.Exec(`DELETE FROM system.lease`); err != nil {
		t.Fatal(err)
	}
	if _, err := tSQL.Exec(`CANCEL JOB $1`, jobID); err != nil {
		t.Fatal(err)
	}
	tSrv.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()
	testutils.SucceedsSoon(t, func() error {
		var status string
		if err := tSQL.QueryRow(`SELECT status FROM [show jobs] WHERE job_id = $1`, jobID).Scan(&status); err != nil {
			t.Fatal(err)
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
