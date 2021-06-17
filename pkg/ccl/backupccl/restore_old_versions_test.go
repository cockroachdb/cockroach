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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
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
		fkRevDirs       = testdataBase + "/fk-rev-history"
		clusterDirs     = testdataBase + "/cluster"
		exceptionalDirs = testdataBase + "/exceptional"
		privilegeDirs   = testdataBase + "/privileges"
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

	t.Run("fk-rev-restore", func(t *testing.T) {
		dirs, err := ioutil.ReadDir(fkRevDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(fkRevDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), restoreOldVersionFKRevTest(exportDir))
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

	t.Run("zoneconfig_privilege_restore", func(t *testing.T) {
		dirs, err := ioutil.ReadDir(privilegeDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(privilegeDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), restoreV201ZoneconfigPrivilegeTest(exportDir))
		}
	})
}

func restoreOldVersionTest(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		params := base.TestServerArgs{}
		const numAccounts = 1000
		_, _, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
			InitManualReplication, base.TestClusterArgs{ServerArgs: params})
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

		results = append(results, []string{"4", "5", "6"})
		sqlDB.Exec(t, `INSERT INTO test.t1 VALUES (4, 5 ,6)`)
		sqlDB.CheckQueryResults(t, `SELECT * FROM test.t1 ORDER BY k`, results)
	}
}

// restoreV201ZoneconfigPrivilegeTest checks that privilege descriptors with
// ZONECONFIG from tables and databases are correctly restored.
// The ZONECONFIG bit was overwritten to be USAGE in 20.2 onwards.
// We only need to test restoring with full cluster backup / restore as
// it is the only form of restore that restores privileges.
func restoreV201ZoneconfigPrivilegeTest(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		const numAccounts = 1000
		_, _, _, tmpDir, cleanupFn := BackupRestoreTestSetup(t, MultiNode, numAccounts, InitManualReplication)
		defer cleanupFn()

		_, _, sqlDB, cleanup := backupRestoreTestSetupEmpty(t, singleNode, tmpDir,
			InitManualReplication, base.TestClusterArgs{})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(tmpDir, "foo"))
		require.NoError(t, err)
		sqlDB.Exec(t, `RESTORE FROM $1`, LocalFoo)
		testDBGrants := [][]string{
			{"test", "admin", "ALL"},
			{"test", "root", "ALL"},
			{"test", "testuser", "ZONECONFIG"},
		}
		sqlDB.CheckQueryResults(t, `show grants on database test`, testDBGrants)

		testTableGrants := [][]string{
			{"test", "public", "test_table", "admin", "ALL"},
			{"test", "public", "test_table", "root", "ALL"},
			{"test", "public", "test_table", "testuser", "ZONECONFIG"},
		}
		sqlDB.CheckQueryResults(t, `show grants on test.test_table`, testTableGrants)

		testTable2Grants := [][]string{
			{"test", "public", "test_table2", "admin", "ALL"},
			{"test", "public", "test_table2", "root", "ALL"},
			{"test", "public", "test_table2", "testuser", "ALL"},
		}
		sqlDB.CheckQueryResults(t, `show grants on test.test_table2`, testTable2Grants)
	}
}

func restoreOldVersionFKRevTest(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		params := base.TestServerArgs{}
		const numAccounts = 1000
		_, _, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
			InitManualReplication, base.TestClusterArgs{ServerArgs: params})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(dir, "foo"))
		require.NoError(t, err)
		sqlDB.Exec(t, `CREATE DATABASE ts`)
		sqlDB.Exec(t, `RESTORE test.rev_times FROM $1 WITH into_db = 'ts'`, LocalFoo)
		for _, ts := range sqlDB.QueryStr(t, `SELECT logical_time FROM ts.rev_times`) {

			sqlDB.Exec(t, fmt.Sprintf(`RESTORE DATABASE test FROM $1 AS OF SYSTEM TIME %s`, ts[0]), LocalFoo)
			// Just rendering the constraints loads and validates schema.
			sqlDB.Exec(t, `SELECT * FROM pg_catalog.pg_constraint`)
			sqlDB.Exec(t, `DROP DATABASE test`)

			// Restore a couple tables, including parent but not child_pk.
			sqlDB.Exec(t, `CREATE DATABASE test`)
			sqlDB.Exec(t, fmt.Sprintf(`RESTORE test.circular FROM $1 AS OF SYSTEM TIME %s`, ts[0]), LocalFoo)
			sqlDB.Exec(t, fmt.Sprintf(`RESTORE test.parent, test.child FROM $1 AS OF SYSTEM TIME %s  WITH skip_missing_foreign_keys`, ts[0]), LocalFoo)
			sqlDB.Exec(t, `SELECT * FROM pg_catalog.pg_constraint`)
			sqlDB.Exec(t, `DROP DATABASE test`)

			// Now do each table on its own with skip_missing_foreign_keys.
			sqlDB.Exec(t, `CREATE DATABASE test`)
			for _, name := range []string{"child_pk", "child", "circular", "parent"} {
				sqlDB.Exec(t, fmt.Sprintf(`RESTORE test.%s FROM $1 AS OF SYSTEM TIME %s WITH skip_missing_foreign_keys`, name, ts[0]), LocalFoo)
			}
			sqlDB.Exec(t, `SELECT * FROM pg_catalog.pg_constraint`)
			sqlDB.Exec(t, `DROP DATABASE test`)

		}
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

/*
func TestCreateIncBackupMissingIndexEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	defer jobs.TestingSetAdoptAndCancelIntervals(5*time.Millisecond, 5*time.Millisecond)()

	const numAccounts = 10
	const numBackups = 9
	windowSize := int(numAccounts / 3)

	blockBackfill := make(chan struct{})
	defer close(blockBackfill)

	backfillWaiting := make(chan struct{})
	defer close(backfillWaiting)

	ctx, tc, sqlDB, dir, cleanupFn := backupRestoreTestSetupWithParams(
		t, singleNode, 0, InitManualReplication, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{Knobs: base.TestingKnobs{
				DistSQL: &execinfra.TestingKnobs{
					RunBeforeBackfillChunk: func(sp roachpb.Span) error {
						select {
						case backfillWaiting <- struct{}{}:
						case <-time.After(time.Second * 5):
							panic("timeout blocking in knob")
						}
						select {
						case <-blockBackfill:
						case <-time.After(time.Second * 5):
							panic("timeout blocking in knob")
						}
						return nil
					},
				},
			}},
		},
	)
	defer cleanupFn()
	args := base.TestServerArgs{ExternalIODir: dir}
	rng, _ := randutil.NewPseudoRand()

	sqlDB.Exec(t, `CREATE TABLE data.jsontest (id INT PRIMARY KEY, j JSONB)`)
	sqlDB.Exec(t, `INSERT INTO data.jsontest VALUES (1, '{"a": "a", "b": "b"}'), (2, '{"c": "c", "d":"d"}')`)

	sqlDB.Exec(t, `CREATE TABLE data.geotest (id INT PRIMARY KEY, p geometry(point))`)
	sqlDB.Exec(t, `INSERT INTO data.geotest VALUES (1, 'POINT(1.0 1.0)'), (2, 'POINT(2.0 2.0)')`)

	var backupDirs []string
	var checksums []uint32
	{
		for backupNum := 0; backupNum < numBackups; backupNum++ {
			// In the following, windowSize is `w` and offset is `o`. The first
			// mutation creates accounts with id [w,3w). Every mutation after
			// that deletes everything less than o, leaves [o, o+w) unchanged,
			// mutates [o+w,o+2w), and inserts [o+2w,o+3w).
			offset := windowSize * backupNum
			var buf bytes.Buffer
			fmt.Fprintf(&buf, `DELETE FROM data.bank WHERE id < %d; `, offset)
			buf.WriteString(`UPSERT INTO data.bank VALUES `)
			for j := 0; j < windowSize*2; j++ {
				if j != 0 {
					buf.WriteRune(',')
				}
				id := offset + windowSize + j
				payload := randutil.RandBytes(rng, backupRestoreRowPayloadSize)
				fmt.Fprintf(&buf, `(%d, %d, '%s')`, id, backupNum, payload)
			}
			sqlDB.Exec(t, buf.String())
			createErr := make(chan error)
			go func() {
				defer close(createErr)
				var stmt string
				switch backupNum % 3 {
				case 0:
					stmt = fmt.Sprintf(`CREATE INDEX test_idx_%d ON data.bank (balance)`, backupNum+1)
				case 1:
					stmt = fmt.Sprintf(`CREATE INDEX test_idx_%d ON data.jsontest USING GIN(j)`, backupNum+1)
				case 2:
					stmt = fmt.Sprintf(`CREATE INDEX test_idx_%d ON data.geotest USING GIST(p)`, backupNum+1)
				}
				t.Log(stmt)
				_, err := sqlDB.DB.ExecContext(ctx, stmt)
				createErr <- err
			}()
			select {
			case <-backfillWaiting:
			case err := <-createErr:
				t.Fatal(err)
			}
			checksums = append(checksums, checksumBankPayload(t, sqlDB))

			backupDir := fmt.Sprintf("nodelocal://0/%d", backupNum)
			var from string
			if backupNum > 0 {
				from = fmt.Sprintf(` INCREMENTAL FROM %s`, strings.Join(backupDirs, `,`))
			}
			sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO '%s' %s`, backupDir, from))
			blockBackfill <- struct{}{}
			require.NoError(t, <-createErr)

			backupDirs = append(backupDirs, fmt.Sprintf(`'%s'`, backupDir))
		}

		// Test a regression in RESTORE where the batch end key was not
		// being set correctly in Import: make an incremental backup such that
		// the greatest key in the diff is less than the previous backups.
		sqlDB.Exec(t, `INSERT INTO data.bank VALUES (0, -1, 'final')`)
		checksums = append(checksums, checksumBankPayload(t, sqlDB))
		sqlDB.Exec(t, fmt.Sprintf(`BACKUP TO '%s' %s`,
			"nodelocal://0/final", fmt.Sprintf(` INCREMENTAL FROM %s`, strings.Join(backupDirs, `,`)),
		))
		backupDirs = append(backupDirs, `'nodelocal://0/final'`)
	}
	os.Rename(dir, path/to/testdata)
}
*/

// TestRestoreOldBackupMissingOfflineIndexes tests restoring a backup made by
// v20.2 prior to the introduction of excluding offline indexes in #62572 using
// the commented-out code above in TestCreateIncBackupMissingIndexEntries. Note:
// that code needs to be pasted into a branch checkout _prior_ to the inclusion
// of the mentioned PR.
func TestRestoreOldBackupMissingOfflineIndexes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderRace(t, "times out under race cause it starts up two test servers")
	ctx := context.Background()

	badBackups, err := filepath.Abs("testdata/restore_old_versions/inc_missing_addsst/v20.2.7")
	require.NoError(t, err)
	args := base.TestServerArgs{ExternalIODir: badBackups}
	backupDirs := make([]string, 9)
	for i := range backupDirs {
		backupDirs[i] = fmt.Sprintf("'nodelocal://0/%d'", i)
	}

	// Start a new cluster to restore into.
	{
		for i := len(backupDirs); i > 0; i-- {
			restoreTC := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{ServerArgs: args})
			defer restoreTC.Stopper().Stop(context.Background())
			sqlDBRestore := sqlutils.MakeSQLRunner(restoreTC.Conns[0])
			from := strings.Join(backupDirs[:i], `,`)
			sqlDBRestore.Exec(t, fmt.Sprintf(`RESTORE FROM %s`, from))

			for j := i; j > 1; j-- {
				var res int64
				switch j % 3 {
				case 2:
					for i := 0; i < 50; i++ {
						if err := sqlDBRestore.DB.QueryRowContext(ctx,
							fmt.Sprintf(`SELECT count(*) FROM data.bank@test_idx_%d`, j-1),
						).Scan(&res); err != nil {
							if !strings.Contains(err.Error(), `not found`) {
								t.Fatal(err)
							}
							t.Logf("index %d doesn't exist yet on attempt %d", j-1, i)
							time.Sleep(time.Millisecond * 50)
							continue
						}
						break
					}
					var expected int64
					sqlDBRestore.QueryRow(t, `SELECT count(*) FROM data.bank@primary`).Scan(&expected)
					if res != expected {
						t.Fatalf("got %d, expected %d", res, expected)
					}
				// case 1 and 0 are both inverted, which we can't validate via SQL, so
				// this is just checking that it eventually shows up, i.e. that the code
				// to validate and create the schema change works.
				case 0:
					found := false
					for i := 0; i < 50; i++ {
						if err := sqlDBRestore.DB.QueryRowContext(ctx,
							fmt.Sprintf(`SELECT count(*) FROM data.jsontest@test_idx_%d`, j-1),
						).Scan(&res); err != nil {
							if strings.Contains(err.Error(), `is inverted`) {
								found = true
								break
							}
							if !strings.Contains(err.Error(), `not found`) {
								t.Fatal(err)
							}
							t.Logf("index %d doesn't exist yet on attempt %d", j-1, i)
							time.Sleep(time.Millisecond * 50)
						}
					}
					if !found {
						t.Fatal("expected index to come back")
					}
				}
			}
		}
	}
}
