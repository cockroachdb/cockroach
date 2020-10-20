// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestRestoreOldVersions ensures that we can successfully restore tables
// and databases exported by old version.
//
// The files being restored live in testdata and are all made from the same
// input SQL which lives in <testdataBase>/create.sql.
//
// The SSTs were created via the following commands:
//
//  VERSION=...
//  roachprod wipe local
//  roachprod stage local release ${VERSION}
//  roachprod start local
//  # If the version is v1.0.7 then you need to enable enterprise with the
//  # enterprise.enabled cluster setting.
//  roachprod sql local:1 -- -e "$(cat pkg/ccl/backupccl/testdata/restore_old_versions/create.sql)"
//  # Create an S3 bucket to store the backup.
//  roachprod sql local:1 -- -e "BACKUP DATABASE test TO 's3://<bucket-name>/${VERSION}?AWS_ACCESS_KEY_ID=<...>&AWS_SECRET_ACCESS_KEY=<...>'"
//  # Then download the backup from s3 and plop the files into the appropriate
//  # testdata directory.
//
func TestRestoreOldVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const (
		testdataBase    = "testdata/restore_old_versions"
		exportDirs      = testdataBase + "/exports"
		clusterDirs     = testdataBase + "/cluster"
		exceptionalDirs = testdataBase + "/exceptional"
	)

	t.Run("table-restore", func(t *testing.T) {
		dirs, err := ioutil.ReadDir(exportDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(exportDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), restoreOldVersionTest(exportDir))
		}
	})

	t.Run("cluster-restore", func(t *testing.T) {
		dirs, err := ioutil.ReadDir(clusterDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(clusterDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), restoreOldVersionClusterTest(exportDir))
		}
	})

	// exceptional backups are backups that were possible to generate on old
	// versions, but are now disallowed, but we should check that we fail
	// gracefully with them.
	t.Run("exceptional-backups", func(t *testing.T) {
		t.Run("x-db-type-reference", func(t *testing.T) {
			backupUnderTest := "xDbRef"
			/*
				This backup was generated with the following SQL:

				CREATE TYPE t AS ENUM ('foo');
				CREATE TABLE tbl (a t);
				CREATE DATABASE otherdb;
				ALTER TABLE tbl RENAME TO otherdb.tbl;
				BACKUP DATABASE otherdb TO 'nodelocal://1/xDbRef';

				This was permitted on some release candidates of v20.2. (#55709)
			*/
			dir, err := os.Stat(filepath.Join(exceptionalDirs, backupUnderTest))
			require.NoError(t, err)
			require.True(t, dir.IsDir())

			// We could create tables which reference types in another database on
			// 20.2 release candidates.
			exportDir, err := filepath.Abs(filepath.Join(exceptionalDirs, dir.Name()))
			require.NoError(t, err)

			externalDir, dirCleanup := testutils.TempDir(t)
			ctx := context.Background()
			tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					ExternalIODir: externalDir,
				},
			})
			sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
			defer func() {
				tc.Stopper().Stop(ctx)
				dirCleanup()
			}()
			err = os.Symlink(exportDir, filepath.Join(externalDir, "foo"))
			require.NoError(t, err)

			// Expect this restore to fail.
			sqlDB.ExpectErr(t, `type "t" has unknown ParentID 50`, `RESTORE DATABASE otherdb FROM $1`, LocalFoo)

			// Expect that we don't crash and that we emit NULL for data that we
			// cannot resolve (e.g. missing database descriptor, create_statement).
			sqlDB.CheckQueryResults(t, `
SELECT
  database_name, parent_schema_name, object_name, object_type, create_statement 
FROM [SHOW BACKUP SCHEMAS '`+LocalFoo+`' WITH privileges]
ORDER BY object_type, object_name`, [][]string{
				{"NULL", "NULL", "otherdb", "database", "NULL"},
				{"otherdb", "public", "tbl", "table", "NULL"},
				{"NULL", "public", "_t", "type", "NULL"},
				{"NULL", "public", "t", "type", "NULL"},
			})
		})
	})
}

func restoreOldVersionTest(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		params := base.TestServerArgs{}
		const numAccounts = 1000
		_, _, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
			InitNone, base.TestClusterArgs{ServerArgs: params})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(dir, "foo"))
		require.NoError(t, err)
		sqlDB.Exec(t, `CREATE DATABASE test`)
		var unused string
		var importedRows int
		sqlDB.QueryRow(t, `RESTORE test.* FROM $1`, LocalFoo).Scan(
			&unused, &unused, &unused, &importedRows, &unused, &unused,
		)
		const totalRows = 12
		if importedRows != totalRows {
			t.Fatalf("expected %d rows, got %d", totalRows, importedRows)
		}
		results := [][]string{
			{"1", "1", "1"},
			{"2", "2", "2"},
			{"3", "3", "3"},
		}
		sqlDB.CheckQueryResults(t, `SELECT * FROM test.t1 ORDER BY k`, results)
		sqlDB.CheckQueryResults(t, `SELECT * FROM test.t2 ORDER BY k`, results)
		sqlDB.CheckQueryResults(t, `SELECT * FROM test.t4 ORDER BY k`, results)
	}
}

func restoreOldVersionClusterTest(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		externalDir, dirCleanup := testutils.TempDir(t)
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				ExternalIODir: externalDir,
			},
		})
		sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
		defer func() {
			tc.Stopper().Stop(ctx)
			dirCleanup()
		}()
		err := os.Symlink(exportDir, filepath.Join(externalDir, "foo"))
		require.NoError(t, err)

		// Ensure that the restore succeeds.
		sqlDB.Exec(t, `RESTORE FROM $1`, LocalFoo)

		sqlDB.CheckQueryResults(t, "SHOW USERS", [][]string{
			{"admin", "", "{}"},
			{"craig", "", "{}"},
			{"root", "", "{admin}"},
		})
		sqlDB.CheckQueryResults(t, "SELECT * FROM system.comments", [][]string{
			{"0", "52", "0", "database comment string"},
			{"1", "53", "0", "table comment string"},
		})
		sqlDB.CheckQueryResults(t, "SELECT * FROM data.bank", [][]string{{"1"}})
	}
}
