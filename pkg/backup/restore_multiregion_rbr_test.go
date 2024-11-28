// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// The goal of this test is to ensure that a user is able to perform a
// regionless restore and modify the restored object without an enterprise
// license.
func TestMultiRegionRegionlessRestoreNoLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t, "test is too heavy to run under stress")

	ctx := context.Background()

	dir, dirCleanupfn := testutils.TempDir(t)
	defer dirCleanupfn()

	_, mrSqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, base.TestingKnobs{}, multiregionccltestutils.WithBaseDirectory(dir),
	)
	defer cleanup()
	mrSql := sqlutils.MakeSQLRunner(mrSqlDB)

	// Create the database & table, insert some values.
	mrSql.Exec(t,
		`CREATE DATABASE d PRIMARY REGION "us-east1" REGIONS "us-east2";
            CREATE TABLE d.t (x INT);
            INSERT INTO d.t VALUES (1), (2), (3);`,
	)

	if err := backuptestutils.VerifyBackupRestoreStatementResult(t, mrSql, `BACKUP DATABASE d INTO $1`, localFoo); err != nil {
		t.Fatal(err)
	}

	defer utilccl.TestingDisableEnterprise()()

	sqlTC := testcluster.StartTestCluster(
		t, singleNode, base.TestClusterArgs{ServerArgs: base.TestServerArgs{
			ExternalIODir:     dir,
			DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				TenantTestingKnobs: &sql.TenantTestingKnobs{
					// This test expects a specific tenant ID to be selected after DROP TENANT.
					EnableTenantIDReuse: true,
				},
			},
		}},
	)
	defer sqlTC.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlTC.Conns[0])

	if err := backuptestutils.VerifyBackupRestoreStatementResult(t, sqlDB, `RESTORE DATABASE d FROM LATEST IN $1 WITH remove_regions`, localFoo); err != nil {
		t.Fatal(err)
	}

	// Perform some writes to d's table.
	sqlDB.Exec(t, `INSERT INTO d.t VALUES (4), (5), (6)`)

	var rowCount int
	sqlDB.QueryRow(t, `SELECT count(x) FROM d.t`).Scan(&rowCount)
	require.Equal(t, 6, rowCount)
}
