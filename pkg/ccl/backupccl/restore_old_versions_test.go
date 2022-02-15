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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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
	testdataBase := testutils.TestDataPath(t, "restore_old_versions")
	var (
		exportDirsWithoutInterleave = testdataBase + "/exports-without-interleaved"
		exportDirs                  = testdataBase + "/exports"
		fkRevDirs                   = testdataBase + "/fk-rev-history"
		clusterDirs                 = testdataBase + "/cluster"
		exceptionalDirs             = testdataBase + "/exceptional"
		privilegeDirs               = testdataBase + "/privileges"
		multiRegionDirs             = testdataBase + "/multi-region"
		publicSchemaDirs            = testdataBase + "/public-schema-remap"
	)

	t.Run("table-restore", func(t *testing.T) {
		dirs, err := ioutil.ReadDir(exportDirsWithoutInterleave)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(exportDirsWithoutInterleave, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), restoreOldVersionTest(exportDir))
		}
	})

	t.Run("table-restore-with-interleave", func(t *testing.T) {
		dirs, err := ioutil.ReadDir(exportDirsWithoutInterleave)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(exportDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), restoreOldVersionTestWithInterleave(exportDir))
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

	t.Run("multi-region-restore", func(t *testing.T) {
		skip.UnderRace(t, "very slow as it starts multiple servers")
		dirs, err := ioutil.ReadDir(multiRegionDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(multiRegionDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), runOldVersionMultiRegionTest(exportDir))
		}
	})

	// exceptional backups are backups that were possible to generate on old
	// versions, but are now disallowed, but we should check that we fail
	// gracefully with them.
	t.Run("exceptional-backups", func(t *testing.T) {
		t.Run("duplicate-db-desc", func(t *testing.T) {
			backupUnderTest := "doubleDB"
			/*
					This backup was generated with the following SQL on (v21.1.6):

				  CREATE DATABASE db1;
				  DROP DATABASE db1;
				  CREATE DATABASE db1;
				  BACKUP TO 'nodelocal://1/doubleDB' WITH revision_history;
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

			sqlDB.Exec(t, `RESTORE FROM $1`, localFoo)
			sqlDB.Exec(t, `DROP DATABASE db1;`)
			sqlDB.Exec(t, `RESTORE DATABASE db1 FROM $1`, localFoo)
			sqlDB.CheckQueryResults(t,
				`SELECT count(*) FROM [SHOW DATABASES] WHERE database_name = 'db1'`,
				[][]string{{"1"}},
			)
		})

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
			sqlDB.ExpectErr(t, `type "t" has unknown ParentID 50`, `RESTORE DATABASE otherdb FROM $1`, localFoo)

			// Expect that we don't crash and that we emit NULL for data that we
			// cannot resolve (e.g. missing database descriptor, create_statement).
			sqlDB.CheckQueryResults(t, `
SELECT
  database_name, parent_schema_name, object_name, object_type, create_statement
FROM [SHOW BACKUP SCHEMAS '`+localFoo+`' WITH privileges]
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

	t.Run("public_schema_remap", func(t *testing.T) {
		dirs, err := ioutil.ReadDir(publicSchemaDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(publicSchemaDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), restorePublicSchemaRemap(exportDir))
		}
	})

	t.Run("public_schema_mixed_version", func(t *testing.T) {
		dirs, err := ioutil.ReadDir(publicSchemaDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(publicSchemaDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), restorePublicSchemaMixedVersion(exportDir))
		}
	})

	t.Run("missing_public_schema_namespace_entry", func(t *testing.T) {
		dirs, err := ioutil.ReadDir(publicSchemaDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(publicSchemaDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), restoreSyntheticPublicSchemaNamespaceEntry(exportDir))
		}
	})

	t.Run("missing_public_schema_namespace_entry_cleanup_on_fail", func(t *testing.T) {
		dirs, err := ioutil.ReadDir(publicSchemaDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(publicSchemaDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), restoreSyntheticPublicSchemaNamespaceEntryCleanupOnFail(exportDir))
		}
	})
}

func restoreOldVersionTestWithInterleave(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		params := base.TestServerArgs{}
		const numAccounts = 1000
		_, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
			InitManualReplication, base.TestClusterArgs{ServerArgs: params})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(dir, "foo"))
		require.NoError(t, err)
		sqlDB.Exec(t, `CREATE DATABASE test`)
		// Restore should now fail.
		sqlDB.ExpectErr(t,
			"pq: restoring interleaved tables is no longer allowed. table t3 was found to be interleaved",
			`RESTORE test.* FROM $1`, localFoo)
	}
}

func runOldVersionMultiRegionTest(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		const numNodes = 9
		dir, dirCleanupFn := testutils.TempDir(t)
		defer dirCleanupFn()
		ctx := context.Background()

		params := make(map[int]base.TestServerArgs, numNodes)
		for i := 0; i < 9; i++ {
			var region string
			switch i / 3 {
			case 0:
				region = "europe-west2"
			case 1:
				region = "us-east1"
			case 2:
				region = "us-west1"
			}
			params[i] = base.TestServerArgs{
				Locality: roachpb.Locality{
					Tiers: []roachpb.Tier{
						{Key: "region", Value: region},
					},
				},
				ExternalIODir: dir,
			}
		}

		tc := testcluster.StartTestCluster(t, numNodes, base.TestClusterArgs{
			ServerArgsPerNode: params,
		})
		defer tc.Stopper().Stop(ctx)
		require.NoError(t, os.Symlink(exportDir, filepath.Join(dir, "external_backup_dir")))

		sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

		var unused string
		var importedRows int
		sqlDB.QueryRow(t, `RESTORE DATABASE multi_region_db FROM $1`, `nodelocal://0/external_backup_dir`).Scan(
			&unused, &unused, &unused, &importedRows, &unused, &unused,
		)
		const totalRows = 12
		if importedRows != totalRows {
			t.Fatalf("expected %d rows, got %d", totalRows, importedRows)
		}
		sqlDB.Exec(t, `USE multi_region_db`)
		sqlDB.CheckQueryResults(t, `select table_name, locality FROM [show tables] ORDER BY table_name;`, [][]string{
			{`tbl_global`, `GLOBAL`},
			{`tbl_primary_region`, `REGIONAL BY TABLE IN PRIMARY REGION`},
			{`tbl_regional_by_row`, `REGIONAL BY ROW`},
			{`tbl_regional_by_table`, `REGIONAL BY TABLE IN "us-east1"`},
		})
		sqlDB.CheckQueryResults(t, `SELECT region FROM [SHOW REGIONS FROM DATABASE] ORDER BY region`, [][]string{
			{`europe-west2`},
			{`us-east1`},
			{`us-west1`},
		})
		sqlDB.CheckQueryResults(t, `SELECT * FROM tbl_primary_region ORDER BY pk`, [][]string{
			{`1`, `a`},
			{`2`, `b`},
			{`3`, `c`},
		})
		sqlDB.CheckQueryResults(t, `SELECT * FROM tbl_global ORDER BY pk`, [][]string{
			{`4`, `d`},
			{`5`, `e`},
			{`6`, `f`},
		})
		sqlDB.CheckQueryResults(t, `SELECT * FROM tbl_regional_by_table ORDER BY pk`, [][]string{
			{`7`, `g`},
			{`8`, `h`},
			{`9`, `i`},
		})
		sqlDB.CheckQueryResults(t, `SELECT crdb_region, * FROM tbl_regional_by_row ORDER BY pk`, [][]string{
			{`europe-west2`, `10`, `j`},
			{`us-east1`, `11`, `k`},
			{`us-west1`, `12`, `l`},
		})
	}
}

func restoreOldVersionTest(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		params := base.TestServerArgs{}
		const numAccounts = 1000
		_, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
			InitManualReplication, base.TestClusterArgs{ServerArgs: params})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(dir, "foo"))
		require.NoError(t, err)
		sqlDB.Exec(t, `CREATE DATABASE test`)
		var unused string
		var importedRows int
		sqlDB.QueryRow(t, `RESTORE test.* FROM $1`, localFoo).Scan(
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
		_, _, tmpDir, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
		defer cleanupFn()

		_, sqlDB, cleanup := backupRestoreTestSetupEmpty(t, singleNode, tmpDir,
			InitManualReplication, base.TestClusterArgs{})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(tmpDir, "foo"))
		require.NoError(t, err)
		sqlDB.Exec(t, `RESTORE FROM $1`, localFoo)
		testDBGrants := [][]string{
			{"test", "admin", "ALL", "true"},
			{"test", "root", "ALL", "true"},
			{"test", "testuser", "ZONECONFIG", "false"},
		}
		sqlDB.CheckQueryResults(t, `show grants on database test`, testDBGrants)

		testTableGrants := [][]string{
			{"test", "public", "test_table", "admin", "ALL", "true"},
			{"test", "public", "test_table", "root", "ALL", "true"},
			{"test", "public", "test_table", "testuser", "ZONECONFIG", "false"},
		}
		sqlDB.CheckQueryResults(t, `show grants on test.test_table`, testTableGrants)

		testTable2Grants := [][]string{
			{"test", "public", "test_table2", "admin", "ALL", "true"},
			{"test", "public", "test_table2", "root", "ALL", "true"},
			{"test", "public", "test_table2", "testuser", "ALL", "true"},
		}
		sqlDB.CheckQueryResults(t, `show grants on test.test_table2`, testTable2Grants)
	}
}

func restoreOldVersionFKRevTest(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		params := base.TestServerArgs{}
		const numAccounts = 1000
		_, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
			InitManualReplication, base.TestClusterArgs{ServerArgs: params})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(dir, "foo"))
		require.NoError(t, err)
		sqlDB.Exec(t, `CREATE DATABASE ts`)
		sqlDB.Exec(t, `RESTORE test.rev_times FROM $1 WITH into_db = 'ts'`, localFoo)
		for _, ts := range sqlDB.QueryStr(t, `SELECT logical_time FROM ts.rev_times`) {

			sqlDB.Exec(t, fmt.Sprintf(`RESTORE DATABASE test FROM $1 AS OF SYSTEM TIME %s`, ts[0]), localFoo)
			// Just rendering the constraints loads and validates schema.
			sqlDB.Exec(t, `SELECT * FROM pg_catalog.pg_constraint`)
			sqlDB.Exec(t, `DROP DATABASE test`)

			// Restore a couple tables, including parent but not child_pk.
			sqlDB.Exec(t, `CREATE DATABASE test`)
			sqlDB.Exec(t, fmt.Sprintf(`RESTORE test.circular FROM $1 AS OF SYSTEM TIME %s`, ts[0]), localFoo)
			sqlDB.Exec(t, fmt.Sprintf(`RESTORE test.parent, test.child FROM $1 AS OF SYSTEM TIME %s  WITH skip_missing_foreign_keys`, ts[0]), localFoo)
			sqlDB.Exec(t, `SELECT * FROM pg_catalog.pg_constraint`)
			sqlDB.Exec(t, `DROP DATABASE test`)

			// Now do each table on its own with skip_missing_foreign_keys.
			sqlDB.Exec(t, `CREATE DATABASE test`)
			for _, name := range []string{"child_pk", "child", "circular", "parent"} {
				sqlDB.Exec(t, fmt.Sprintf(`RESTORE test.%s FROM $1 AS OF SYSTEM TIME %s WITH skip_missing_foreign_keys`, name, ts[0]), localFoo)
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
		sqlDB.Exec(t, `RESTORE FROM $1`, localFoo)

		sqlDB.CheckQueryResults(t, "SHOW USERS", [][]string{
			{"admin", "", "{}"},
			{"craig", "", "{}"},
			{"root", "", "{admin}"},
		})
		sqlDB.CheckQueryResults(t, "SELECT * FROM system.comments", [][]string{
			{"0", "52", "0", "database comment string"},
			{"1", "53", "0", "table comment string"},
		})
		// In the backup, Public schemas for non-system databases have ID 29.
		// These should all be updated to explicit public schemas.
		sqlDB.CheckQueryResults(t, `SELECT
	if((id = 29), 'system', 'non-system') AS is_system_schema, count(*) as c
FROM
	system.namespace
WHERE
	"parentSchemaID" = 0 AND name = 'public'
GROUP BY
	is_system_schema
ORDER BY
	c ASC`, [][]string{
			{"system", "1"},
			{"non-system", "3"},
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

	badBackups, err := filepath.Abs(testutils.TestDataPath(t, "restore_old_versions", "inc_missing_addsst", "v20.2.7"))
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

func TestRestoreWithDroppedSchemaCorruption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	backupDir := testutils.TestDataPath(t, "restore_with_dropped_schema", "exports", "v20.2.7")
	const (
		dbName  = "foo"
		fromDir = "nodelocal://0/"
	)

	args := base.TestServerArgs{ExternalIODir: backupDir}
	s, sqlDB, _ := serverutils.StartServer(t, args)
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(ctx)

	tdb.Exec(t, fmt.Sprintf("RESTORE DATABASE %s FROM '%s'", dbName, fromDir))
	query := fmt.Sprintf("SELECT database_name FROM [SHOW DATABASES] WHERE database_name = '%s'", dbName)
	tdb.CheckQueryResults(t, query, [][]string{{dbName}})

	// Read descriptor without validation.
	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	hasSameNameSchema := func(dbName string) (exists bool) {
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(ctx context.Context, txn *kv.Txn, col *descs.Collection) error {
			// Using this method to avoid validation.
			id, err := col.Direct().LookupDatabaseID(ctx, txn, dbName)
			if err != nil {
				return err
			}
			res, err := txn.Get(ctx, catalogkeys.MakeDescMetadataKey(execCfg.Codec, id))
			if err != nil {
				return err
			}
			var desc descpb.Descriptor
			err = res.ValueProto(&desc)
			if err != nil {
				return err
			}
			_, dbDesc, _, _ := descpb.FromDescriptorWithMVCCTimestamp(&desc, res.Value.Timestamp)
			require.NotNil(t, dbDesc)
			for name := range dbDesc.Schemas {
				if name == dbName {
					exists = true
					break
				}
			}
			return nil
		}))
		return exists
	}
	require.Falsef(t, hasSameNameSchema(dbName), "corrupted descriptor exists")
}

// restorePublicSchemaRemap tests that if we're restoring a database from
// an older version where the database has a synthetic public schema, a real
// descriptor backed public schema is created and the tables in the schema
// are correctly mapped to the new public schema.
func restorePublicSchemaRemap(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		const numAccounts = 1000
		_, _, tmpDir, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
		defer cleanupFn()

		_, sqlDB, cleanup := backupRestoreTestSetupEmpty(t, singleNode, tmpDir,
			InitManualReplication, base.TestClusterArgs{})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(tmpDir, "foo"))
		require.NoError(t, err)

		sqlDB.Exec(t, fmt.Sprintf("RESTORE DATABASE d FROM '%s'", localFoo))

		var restoredDBID, publicSchemaID int
		row := sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name='d' AND "parentID"=0`)
		row.Scan(&restoredDBID)
		row = sqlDB.QueryRow(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name='public' AND "parentID"=%d`, restoredDBID))
		row.Scan(&publicSchemaID)

		if publicSchemaID == keys.PublicSchemaID {
			t.Fatalf("expected public schema id to not be %d", keys.PublicSchemaID)
		}

		row = sqlDB.QueryRow(t,
			fmt.Sprintf(`SELECT count(1) FROM system.namespace WHERE name='t' AND "parentID"=%d AND "parentSchemaID"=%d`, restoredDBID, publicSchemaID))
		require.NotNil(t, row)

		sqlDB.CheckQueryResults(t, `SELECT x FROM d.s.t`, [][]string{{"1"}, {"2"}})
		sqlDB.CheckQueryResults(t, `SELECT x FROM d.public.t`, [][]string{{"3"}, {"4"}})

		// Test restoring a single table and ensuring that d.public.t which
		// previously had a synthetic public schema gets correctly restored into the
		// descriptor backed public schema of database test.
		sqlDB.Exec(t, `CREATE DATABASE test`)
		sqlDB.Exec(t, `RESTORE d.public.t FROM $1 WITH into_db = 'test'`, localFoo)

		row = sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name='test' AND "parentID"=0`)
		var parentDBID int
		row.Scan(&parentDBID)

		row = sqlDB.QueryRow(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name='public' AND "parentID"=%d`, parentDBID))
		row.Scan(&publicSchemaID)

		if publicSchemaID == keys.PublicSchemaID || publicSchemaID == int(descpb.InvalidID) {
			t.Errorf(fmt.Sprintf("expected public schema id to not be %d or %d, found %d", keys.PublicSchemaID, descpb.InvalidID, publicSchemaID))
		}

		sqlDB.CheckQueryResults(t, `SELECT x FROM test.public.t`, [][]string{{"3"}, {"4"}})
	}
}

// restorePublicSchemaMixedVersion tests that if we are not on version
// PublicSchemaWithDescriptor, we do not create public schemas during restore.
func restorePublicSchemaMixedVersion(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		const numAccounts = 1000
		_, _, tmpDir, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
		defer cleanupFn()

		_, sqlDB, cleanup := backupRestoreTestSetupEmpty(t, singleNode, tmpDir,
			InitManualReplication, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
						Server: &server.TestingKnobs{
							DisableAutomaticVersionUpgrade: make(chan struct{}),
							BinaryVersionOverride:          clusterversion.ByKey(clusterversion.PublicSchemasWithDescriptors - 1),
						},
					},
				}})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(tmpDir, "foo"))
		require.NoError(t, err)

		sqlDB.Exec(t, fmt.Sprintf("RESTORE DATABASE d FROM '%s'", localFoo))

		var restoredDBID int
		row := sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name='d' AND "parentID"=0`)
		row.Scan(&restoredDBID)

		publicSchemaID := keys.PublicSchemaIDForBackup

		row = sqlDB.QueryRow(t,
			fmt.Sprintf(`SELECT count(1) FROM system.namespace WHERE name='t' AND "parentID"=%d AND "parentSchemaID"=%d`, restoredDBID, publicSchemaID))
		require.NotNil(t, row)

		sqlDB.CheckQueryResults(t, `SELECT x FROM d.s.t`, [][]string{{"1"}, {"2"}})
		sqlDB.CheckQueryResults(t, `SELECT x FROM d.public.t`, [][]string{{"3"}, {"4"}})

		// Test restoring a single table and ensuring that d.public.t which
		// previously had a synthetic public schema gets correctly restored into the
		// descriptor backed public schema of database test.
		sqlDB.Exec(t, `CREATE DATABASE test`)
		sqlDB.Exec(t, `RESTORE d.public.t FROM $1 WITH into_db = 'test'`, localFoo)

		row = sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name='test' AND "parentID"=0`)
		var parentDBID int
		row.Scan(&parentDBID)

		row = sqlDB.QueryRow(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name='public' AND "parentID"=%d`, parentDBID))
		row.Scan(&publicSchemaID)

		require.Equal(t, publicSchemaID, keys.PublicSchemaID)

		sqlDB.CheckQueryResults(t, `SELECT x FROM test.public.t`, [][]string{{"3"}, {"4"}})
	}
}

func restoreSyntheticPublicSchemaNamespaceEntry(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		const numAccounts = 1000
		_, _, tmpDir, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
		defer cleanupFn()

		_, sqlDB, cleanup := backupRestoreTestSetupEmpty(t, singleNode, tmpDir,
			InitManualReplication, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
						Server: &server.TestingKnobs{
							DisableAutomaticVersionUpgrade: make(chan struct{}),
							BinaryVersionOverride:          clusterversion.ByKey(clusterversion.PublicSchemasWithDescriptors - 1),
						},
					},
				}})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(tmpDir, "foo"))
		require.NoError(t, err)

		sqlDB.Exec(t, fmt.Sprintf("RESTORE DATABASE d FROM '%s'", localFoo))

		var dbID int
		row := sqlDB.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'd'`)
		row.Scan(&dbID)

		sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT id FROM system.namespace WHERE name = 'public' AND "parentID"=%d`, dbID), [][]string{{"29"}})
	}
}

func restoreSyntheticPublicSchemaNamespaceEntryCleanupOnFail(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		const numAccounts = 1000
		_, _, tmpDir, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
		defer cleanupFn()

		tc, sqlDB, cleanup := backupRestoreTestSetupEmpty(t, singleNode, tmpDir,
			InitManualReplication, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
						Server: &server.TestingKnobs{
							DisableAutomaticVersionUpgrade: make(chan struct{}),
							BinaryVersionOverride:          clusterversion.ByKey(clusterversion.PublicSchemasWithDescriptors - 1),
						},
					},
				}})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(tmpDir, "foo"))
		require.NoError(t, err)

		for _, server := range tc.Servers {
			registry := server.JobRegistry().(*jobs.Registry)
			registry.TestingResumerCreationKnobs = map[jobspb.Type]func(raw jobs.Resumer) jobs.Resumer{
				jobspb.TypeRestore: func(raw jobs.Resumer) jobs.Resumer {
					r := raw.(*restoreResumer)
					r.testingKnobs.beforePublishingDescriptors = func() error {
						return errors.New("boom")
					}
					return r
				},
			}
		}

		// Drop the default databases so only the system database remains.
		sqlDB.Exec(t, "DROP DATABASE defaultdb")
		sqlDB.Exec(t, "DROP DATABASE postgres")

		restoreQuery := fmt.Sprintf("RESTORE DATABASE d FROM '%s'", localFoo)
		sqlDB.ExpectErr(t, "boom", restoreQuery)

		// We should have no non-system database with a public schema name space
		// entry with id 29.
		sqlDB.CheckQueryResults(t, `SELECT id FROM system.namespace WHERE name = 'public' AND id=29 AND "parentID"!=1`, [][]string{})
	}
}
