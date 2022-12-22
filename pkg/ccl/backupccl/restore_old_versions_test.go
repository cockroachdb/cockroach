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
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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
//	VERSION=...
//	roachprod wipe local
//	roachprod stage local release ${VERSION}
//	roachprod start local
//	# If the version is v1.0.7 then you need to enable enterprise with the
//	# enterprise.enabled cluster setting.
//	roachprod sql local:1 -- -e "$(cat pkg/ccl/backupccl/testdata/restore_old_versions/create.sql)"
//	# Create an S3 bucket to store the backup.
//	roachprod sql local:1 -- -e "BACKUP DATABASE test TO 's3://<bucket-name>/${VERSION}?AWS_ACCESS_KEY_ID=<...>&AWS_SECRET_ACCESS_KEY=<...>'"
//	# Then download the backup from s3 and plop the files into the appropriate
//	# testdata directory.
func TestRestoreOldVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testdataBase := datapathutils.TestDataPath(t, "restore_old_versions")
	var (
		exportDirsWithoutInterleave = testdataBase + "/exports-without-interleaved"
		clusterDirs                 = testdataBase + "/cluster"
		systemUsersDirs             = testdataBase + "/system-users-restore"
	)

	t.Run("table-restore", func(t *testing.T) {
		dirs, err := os.ReadDir(exportDirsWithoutInterleave)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(exportDirsWithoutInterleave, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), restoreOldVersionTest(exportDir))
		}
	})

	t.Run("cluster-restore", func(t *testing.T) {
		dirs, err := os.ReadDir(clusterDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(clusterDirs, dir.Name()))
			require.NoError(t, err)

			t.Run(dir.Name(), restoreOldVersionClusterTest(exportDir))
		}
	})

	t.Run("system-users-restore", func(t *testing.T) {
		dirs, err := os.ReadDir(systemUsersDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(systemUsersDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), restoreSystemUsersWithoutIDs(exportDir))
		}
	})

	t.Run("full-cluster-restore-users-without-ids", func(t *testing.T) {
		dirs, err := os.ReadDir(systemUsersDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(systemUsersDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), fullClusterRestoreUsersWithoutIDs(exportDir))
		}
	})
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

func TestRestoreFKRevTest(t *testing.T) {
	params := base.TestServerArgs{}
	const numAccounts = 1000
	_, sqlDB, _, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
		InitManualReplication, base.TestClusterArgs{ServerArgs: params})
	defer cleanup()

	sqlDB.Exec(t, `CREATE DATABASE test`)
	sqlDB.Exec(t, `SET database = test`)
	sqlDB.Exec(t, `
CREATE TABLE circular (k INT8 PRIMARY KEY, selfid INT8 UNIQUE);
ALTER TABLE circular ADD CONSTRAINT self_fk FOREIGN KEY (selfid) REFERENCES circular (selfid);
CREATE TABLE parent (k INT8 PRIMARY KEY, j INT8 UNIQUE);
CREATE TABLE child (k INT8 PRIMARY KEY, parent_i INT8 REFERENCES parent, parent_j INT8 REFERENCES parent (j));
CREATE TABLE child_pk (k INT8 PRIMARY KEY REFERENCES parent);
`)

	sqlDB.Exec(t, `BACKUP INTO $1 WITH revision_history`, localFoo)

	sqlDB.Exec(t, `CREATE TABLE rev_times (id INT PRIMARY KEY, logical_time DECIMAL);`)
	sqlDB.Exec(t, `INSERT INTO rev_times VALUES (1, cluster_logical_timestamp());`)

	sqlDB.Exec(t, `
	CREATE USER newuser;
	GRANT ALL ON circular TO newuser;
	GRANT ALL ON parent TO newuser;
`)

	sqlDB.Exec(t, `BACKUP INTO LATEST IN $1 WITH revision_history`, localFoo)
	sqlDB.Exec(t, `INSERT INTO rev_times VALUES (2, cluster_logical_timestamp())`)

	sqlDB.Exec(t, `GRANT ALL ON child TO newuser;`)

	sqlDB.Exec(t, `BACKUP INTO LATEST IN $1 WITH revision_history`, localFoo)
	sqlDB.Exec(t, `INSERT INTO rev_times VALUES (3, cluster_logical_timestamp())`)

	sqlDB.Exec(t, `GRANT ALL ON child_pk TO newuser`)

	sqlDB.Exec(t, `BACKUP INTO LATEST IN $1 WITH revision_history`, localFoo)
	sqlDB.Exec(t, `DROP DATABASE test`)

	sqlDB.Exec(t, `CREATE DATABASE ts`)
	sqlDB.Exec(t, `RESTORE test.rev_times FROM LATEST IN $1 WITH into_db = 'ts'`, localFoo)
	for _, ts := range sqlDB.QueryStr(t, `SELECT logical_time FROM ts.rev_times`) {
		sqlDB.Exec(t, fmt.Sprintf(`RESTORE DATABASE test FROM LATEST IN $1 AS OF SYSTEM TIME %s`, ts[0]), localFoo)
		// Just rendering the constraints loads and validates schema.
		sqlDB.Exec(t, `SELECT * FROM pg_catalog.pg_constraint`)
		sqlDB.Exec(t, `DROP DATABASE test`)

		// Restore a couple tables, including parent but not child_pk.
		sqlDB.Exec(t, `CREATE DATABASE test`)
		sqlDB.Exec(t, fmt.Sprintf(`RESTORE test.circular FROM LATEST IN $1 AS OF SYSTEM TIME %s`, ts[0]), localFoo)
		sqlDB.Exec(t, fmt.Sprintf(`RESTORE test.parent, test.child FROM LATEST IN $1 AS OF SYSTEM TIME %s  WITH skip_missing_foreign_keys`, ts[0]), localFoo)
		sqlDB.Exec(t, `SELECT * FROM pg_catalog.pg_constraint`)
		sqlDB.Exec(t, `DROP DATABASE test`)

		// Now do each table on its own with skip_missing_foreign_keys.
		sqlDB.Exec(t, `CREATE DATABASE test`)
		for _, name := range []string{"child_pk", "child", "circular", "parent"} {
			sqlDB.Exec(t, fmt.Sprintf(`RESTORE test.%s FROM LATEST IN $1 AS OF SYSTEM TIME %s WITH skip_missing_foreign_keys`, name, ts[0]), localFoo)
		}
		sqlDB.Exec(t, `SELECT * FROM pg_catalog.pg_constraint`)
		sqlDB.Exec(t, `DROP DATABASE test`)
	}
}

func restoreOldVersionClusterTest(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		externalDir, dirCleanup := testutils.TempDir(t)
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				// Disabling the test tenant due to test failures. More
				// investigation is required. Tracked with #76378.
				DisableDefaultTestTenant: true,
				ExternalIODir:            externalDir,
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
		sqlDB.Exec(t, `RESTORE FROM LATEST IN $1`, localFoo)

		sqlDB.CheckQueryResults(t, "SHOW DATABASES", [][]string{
			{"data", "root", "NULL", "NULL", "{}", "NULL"},
			{"defaultdb", "root", "NULL", "NULL", "{}", "NULL"},
			{"postgres", "root", "NULL", "NULL", "{}", "NULL"},
			{"system", "node", "NULL", "NULL", "{}", "NULL"},
		})

		sqlDB.CheckQueryResults(t, "SHOW SCHEMAS", [][]string{
			{"crdb_internal", "NULL"},
			{"information_schema", "NULL"},
			{"pg_catalog", "NULL"},
			{"pg_extension", "NULL"},
			{"public", "admin"},
		})

		sqlDB.CheckQueryResults(t, "SHOW USERS", [][]string{
			{"admin", "", "{}"},
			{"craig", "", "{}"},
			{"root", "", "{admin}"},
		})

		sqlDB.Exec(t, `USE data;`)
		sqlDB.CheckQueryResults(t, "SHOW TYPES", [][]string{
			{"foo", "bat", "root"},
		})
		sqlDB.CheckQueryResults(t, "SELECT schema_name, table_name, type, owner FROM [SHOW TABLES]", [][]string{
			{"public", "bank", "table", "root"},
		})

		// Now validate that the namespace table doesn't have more than one entry
		// for the same ID.
		sqlDB.CheckQueryResults(t, `
SELECT 
CASE WHEN count(distinct id) = count(id)
THEN 'unique' ELSE 'duplicates' 
END
FROM system.namespace;`, [][]string{{"unique"}})

		sqlDB.CheckQueryResults(t, "SELECT comment FROM system.comments", [][]string{
			{"database comment string"},
			{"table comment string"},
		})

		sqlDB.CheckQueryResults(t, "SELECT \"localityKey\", \"localityValue\" FROM system.locations WHERE \"localityValue\" = 'nyc'", [][]string{
			{"city", "nyc"},
		})

		// In the backup, Public schemas for non-system databases have ID 29. These
		// should all be updated to explicit public schemas.
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

		sqlDB.CheckQueryResults(t, "SELECT * FROM data.bank",
			[][]string{{"1", "a"}, {"2", "b"}, {"3", "c"}})

		// Check that we can select from every known system table and haven't
		// clobbered any.
		for systemTableName, config := range systemTableBackupConfiguration {
			if !config.expectMissingInSystemTenant {
				sqlDB.Exec(t, fmt.Sprintf("SELECT * FROM system.%s", systemTableName))
			}
		}
	}
}

func TestRestoreWithDroppedSchemaCorruption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	backupDir := datapathutils.TestDataPath(t, "restore_with_dropped_schema", "exports", "v20.2.7")
	const (
		dbName  = "foo"
		fromDir = "nodelocal://0/"
	)

	args := base.TestServerArgs{
		ExternalIODir: backupDir,
		// Disabling the test tenant because this test case traps when run
		// from within a tenant. The problem occurs because we try to
		// reference a nil pointer below where we're expecting a database
		// descriptor to exist. More investigation is required.
		// Tracked with #76378.
		DisableDefaultTestTenant: true,
	}
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
			id, err := col.LookupDatabaseID(ctx, txn, dbName)
			if err != nil {
				return err
			}
			res, err := txn.Get(ctx, catalogkeys.MakeDescMetadataKey(execCfg.Codec, id))
			if err != nil {
				return err
			}
			b, err := descbuilder.FromSerializedValue(res.Value)
			if err != nil {
				return err
			}
			require.NotNil(t, b)
			require.Equal(t, catalog.Database, b.DescriptorType())
			db := b.BuildImmutable().(catalog.DatabaseDescriptor)
			exists = db.GetSchemaID(dbName) != descpb.InvalidID
			return nil
		}))
		return exists
	}
	require.Falsef(t, hasSameNameSchema(dbName), "corrupted descriptor exists")
}

func fullClusterRestoreUsersWithoutIDs(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		const numAccounts = 1000
		_, _, tmpDir, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
		defer cleanupFn()

		_, sqlDB, cleanup := backupRestoreTestSetupEmpty(t, singleNode, tmpDir,
			InitManualReplication, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
					},
				}})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(tmpDir, "foo"))
		require.NoError(t, err)

		sqlDB.Exec(t, fmt.Sprintf("RESTORE FROM '%s'", localFoo))

		sqlDB.CheckQueryResults(t, `SELECT username, "hashedPassword", "isRole", user_id FROM system.users`, [][]string{
			{"admin", "", "true", "2"},
			{"root", "", "false", "1"},
			{"testrole", "NULL", "true", "100"},
			{"testuser", "NULL", "false", "101"},
			{"testuser2", "NULL", "false", "102"},
			{"testuser3", "NULL", "false", "103"},
			{"testuser4", "NULL", "false", "104"},
		})

		sqlDB.CheckQueryResults(t, `SELECT * FROM system.role_options`, [][]string{
			{"testrole", "NOLOGIN", "NULL", "100"},
			{"testuser", "CREATEROLE", "NULL", "101"},
			{"testuser", "VALID UNTIL", "2021-01-10 00:00:00+00:00", "101"},
			{"testuser2", "CONTROLCHANGEFEED", "NULL", "102"},
			{"testuser2", "CONTROLJOB", "NULL", "102"},
			{"testuser2", "CREATEDB", "NULL", "102"},
			{"testuser2", "CREATELOGIN", "NULL", "102"},
			{"testuser2", "NOLOGIN", "NULL", "102"},
			{"testuser2", "VIEWACTIVITY", "NULL", "102"},
			{"testuser3", "CANCELQUERY", "NULL", "103"},
			{"testuser3", "MODIFYCLUSTERSETTING", "NULL", "103"},
			{"testuser3", "VIEWACTIVITYREDACTED", "NULL", "103"},
			{"testuser3", "VIEWCLUSTERSETTING", "NULL", "103"},
			{"testuser4", "NOSQLLOGIN", "NULL", "104"},
		})

		// Verify that the next user we create uses the next biggest ID.
		sqlDB.Exec(t, "CREATE USER testuser5")

		sqlDB.CheckQueryResults(t, `SELECT username, "hashedPassword", "isRole", user_id FROM system.users`, [][]string{
			{"admin", "", "true", "2"},
			{"root", "", "false", "1"},
			{"testrole", "NULL", "true", "100"},
			{"testuser", "NULL", "false", "101"},
			{"testuser2", "NULL", "false", "102"},
			{"testuser3", "NULL", "false", "103"},
			{"testuser4", "NULL", "false", "104"},
			{"testuser5", "NULL", "false", "105"},
		})
	}
}

func restoreSystemUsersWithoutIDs(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		const numAccounts = 1000
		_, _, tmpDir, cleanupFn := backupRestoreTestSetup(t, multiNode, numAccounts, InitManualReplication)
		defer cleanupFn()

		_, sqlDB, cleanup := backupRestoreTestSetupEmpty(t, singleNode, tmpDir,
			InitManualReplication, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					Knobs: base.TestingKnobs{
						JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
					},
				}})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(tmpDir, "foo"))
		require.NoError(t, err)

		sqlDB.Exec(t, fmt.Sprintf("RESTORE SYSTEM USERS FROM '%s'", localFoo))

		sqlDB.CheckQueryResults(t, `SELECT username, "hashedPassword", "isRole", user_id FROM system.users`, [][]string{
			{"admin", "", "true", "2"},
			{"root", "", "false", "1"},
			{"testrole", "NULL", "true", "100"},
			{"testuser", "NULL", "false", "101"},
			{"testuser2", "NULL", "false", "102"},
			{"testuser3", "NULL", "false", "103"},
			{"testuser4", "NULL", "false", "104"},
		})

		// Verify that the next user we create uses the next biggest ID.
		sqlDB.Exec(t, "CREATE USER testuser5")

		sqlDB.CheckQueryResults(t, `SELECT username, "hashedPassword", "isRole", user_id FROM system.users`, [][]string{
			{"admin", "", "true", "2"},
			{"root", "", "false", "1"},
			{"testrole", "NULL", "true", "100"},
			{"testuser", "NULL", "false", "101"},
			{"testuser2", "NULL", "false", "102"},
			{"testuser3", "NULL", "false", "103"},
			{"testuser4", "NULL", "false", "104"},
			{"testuser5", "NULL", "false", "105"},
		})

		// Drop some users and try restoring again.
		sqlDB.Exec(t, "DROP ROLE testrole")
		sqlDB.Exec(t, "DROP ROLE testuser2")
		sqlDB.Exec(t, "DROP ROLE testuser3")
		sqlDB.Exec(t, "DROP ROLE testuser4")

		sqlDB.Exec(t, fmt.Sprintf("RESTORE SYSTEM USERS FROM '%s'", localFoo))

		// testrole, testuser2, testuser3, testuser4 should be reassigned higher ids.
		sqlDB.CheckQueryResults(t, `SELECT username, "hashedPassword", "isRole", user_id FROM system.users`, [][]string{
			{"admin", "", "true", "2"},
			{"root", "", "false", "1"},
			{"testrole", "NULL", "true", "106"},
			{"testuser", "NULL", "false", "101"},
			{"testuser2", "NULL", "false", "107"},
			{"testuser3", "NULL", "false", "108"},
			{"testuser4", "NULL", "false", "109"},
			{"testuser5", "NULL", "false", "105"},
		})

		// Verify that the next user we create uses the next biggest ID.
		sqlDB.Exec(t, "CREATE USER testuser6")
		sqlDB.CheckQueryResults(t, `SELECT username, "hashedPassword", "isRole", user_id FROM system.users`, [][]string{
			{"admin", "", "true", "2"},
			{"root", "", "false", "1"},
			{"testrole", "NULL", "true", "106"},
			{"testuser", "NULL", "false", "101"},
			{"testuser2", "NULL", "false", "107"},
			{"testuser3", "NULL", "false", "108"},
			{"testuser4", "NULL", "false", "109"},
			{"testuser5", "NULL", "false", "105"},
			{"testuser6", "NULL", "false", "110"},
		})

	}
}
