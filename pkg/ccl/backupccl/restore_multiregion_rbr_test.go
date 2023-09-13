// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package backupccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// The goal of this test is to ensure that if a user ever performed a
// regionless restore where the backed-up target has a regional by row table,
// they would be able to get themselves out of a stuck state without needing
// an enterprise license.
func TestMultiRegionRegionlessRestoreNoLicense(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	// Make table regional by row.
	mrSql.Exec(t, `ALTER TABLE d.t SET LOCALITY REGIONAL BY ROW;`)

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

	if err := backuptestutils.VerifyBackupRestoreStatementResult(t, sqlDB, `RESTORE DATABASE d FROM LATEST IN $1 WITH strip_localities`, localFoo); err != nil {
		t.Fatal(err)
	}

	// Get us in the state that allows us to perform writes.
	// This is the main purpose of this test - we want to ensure that this process is available
	// to those without enterprise licenses.
	sqlDB.Exec(t, `ALTER TABLE d.t ALTER COLUMN crdb_region SET DEFAULT 'us-east1';
                        ALTER TABLE d.t CONFIGURE ZONE DISCARD;`)

	sqlDB.Exec(t, ``)

	// Perform some writes to d's table.
	sqlDB.Exec(t, `INSERT INTO d.t VALUES (4), (5), (6)`)

	var rowCount int
	sqlDB.QueryRow(t, `SELECT count(x) FROM d.t`).Scan(&rowCount)
	require.Equal(t, 6, rowCount)

}
