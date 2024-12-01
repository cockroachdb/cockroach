// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
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
//	roachprod sql local:1 -- -e "$(cat pkg/backup/testdata/restore_old_versions/cluster/create.sql)"
//	# Create an S3 bucket to store the backup.
//	roachprod sql local:1 -- -e "BACKUP INTO 's3://<bucket-name>/${VERSION}?AWS_ACCESS_KEY_ID=<...>&AWS_SECRET_ACCESS_KEY=<...>'"
//	# Then download the backup from s3 and plop the files into the appropriate
//	# testdata directory.
func TestRestoreOldVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testdataBase := datapathutils.TestDataPath(t, "restore_old_versions")
	var (
		clusterDirs                    = testdataBase + "/cluster"
		systemRoleMembersDirs          = testdataBase + "/system-role-members-restore"
		systemPrivilegesDirs           = testdataBase + "/system-privileges-restore"
		systemDatabaseRoleSettingsDirs = testdataBase + "/system-database-role-settings-restore"
		systemExternalConnectionsDirs  = testdataBase + "/system-external-connections-restore"
		systemTenantSettingsDirs       = testdataBase + "/system-tenant-settings-version-override"
	)

	t.Run("cluster-restore", func(t *testing.T) {
		dirs, err := os.ReadDir(clusterDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			// Skip over the `create.sql` file.
			if !dir.IsDir() {
				continue
			}
			exportDir, err := filepath.Abs(filepath.Join(clusterDirs, dir.Name()))
			require.NoError(t, err)

			t.Run(dir.Name(), restoreOldVersionClusterTest(exportDir))
		}
	})

	t.Run("full-cluster-restore-system-role-members-without-ids", func(t *testing.T) {
		dirs, err := os.ReadDir(systemRoleMembersDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(systemRoleMembersDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), fullClusterRestoreSystemRoleMembersWithoutIDs(exportDir))
		}
	})

	t.Run("full-cluster-restore-system-privileges-without-ids", func(t *testing.T) {
		dirs, err := os.ReadDir(systemPrivilegesDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(systemPrivilegesDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), fullClusterRestoreSystemPrivilegesWithoutIDs(exportDir))
		}
	})

	t.Run("full-cluster-restore-system-database-role-settings-without-ids", func(t *testing.T) {
		dirs, err := os.ReadDir(systemDatabaseRoleSettingsDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(systemDatabaseRoleSettingsDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), fullClusterRestoreSystemDatabaseRoleSettingsWithoutIDs(exportDir))
		}
	})

	t.Run("full-cluster-restore-system-external-connections-without-ids", func(t *testing.T) {
		dirs, err := os.ReadDir(systemExternalConnectionsDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(systemExternalConnectionsDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), fullClusterRestoreSystemExternalConnectionsWithoutIDs(exportDir))
		}
	})

	t.Run("full cluster restore all-tenants version override is ignored", func(t *testing.T) {
		dirs, err := os.ReadDir(systemTenantSettingsDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(systemTenantSettingsDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), fullClusterRestoreSystemTenantSettingsSkipVersionOverride(exportDir))
		}
	})
}

func restoreOldVersionClusterTest(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		externalDir, dirCleanup := testutils.TempDir(t)
		ctx := context.Background()
		tc := testcluster.StartTestCluster(t, singleNode, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				// Disabling the test tenant due to test failures. More
				// investigation is required. Tracked with #76378.
				DefaultTestTenant: base.TODOTestTenantDisabled,
				ExternalIODir:     externalDir,
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
		//
		// The restore queries are run with `UNSAFE_RESTORE_INCOMPATIBLE_VERSION`
		// option to ensure the restore is successful on development branches. This
		// is because, while the backups were generated on release branches and have
		// versions such as 22.2 in their manifest, the development branch will have
		// a MinSupportedVersion offset by the clusterversion.DevOffset described in
		// `pkg/clusterversion/cockroach_versions.go`. This will mean that the
		// manifest version is always less than the MinSupportedVersion which will
		// in turn fail the restore unless we pass in the specified option to elide
		// the compatibility check.
		sqlDB.Exec(t, `RESTORE FROM LATEST IN $1 WITH UNSAFE_RESTORE_INCOMPATIBLE_VERSION`, localFoo)

		sqlDB.CheckQueryResults(t, "SHOW DATABASES", [][]string{
			{"data", "root", "NULL", "NULL", "{}", "NULL"},
			{"defaultdb", "root", "NULL", "NULL", "{}", "NULL"},
			{"postgres", "root", "NULL", "NULL", "{}", "NULL"},
			{"system", "node", "NULL", "NULL", "{}", "NULL"},
		})

		sqlDB.CheckQueryResults(t, "SHOW SCHEMAS", [][]string{
			{"crdb_internal", "node"},
			{"information_schema", "node"},
			{"pg_catalog", "node"},
			{"pg_extension", "node"},
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

		// The "craig" user should be able to use the function because the
		// EXECUTE privilege is added to the public role.
		sqlDB.Exec(t, "SET ROLE = craig")
		sqlDB.CheckQueryResults(t, "SELECT add(10, 3)", [][]string{{"13"}})

		sqlDB.CheckQueryResults(t, "SHOW GRANTS ON FUNCTION add", [][]string{
			{"data", "public", "100129", "add(int8, int8)", "admin", "ALL", "true"},
			{"data", "public", "100129", "add(int8, int8)", "public", "EXECUTE", "false"},
			{"data", "public", "100129", "add(int8, int8)", "root", "ALL", "true"},
		})
	}
}

func TestRestoreWithDroppedSchemaCorruption(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	const (
		dbName         = "foo"
		restoredDBName = "foorestored"
		fromDir        = "nodelocal://1/"
	)

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()
	args := base.TestServerArgs{
		ExternalIODir: dir,
		// Disabling the test tenant because this test case traps when run
		// from within a tenant. The problem occurs because we try to
		// reference a nil pointer below where we're expecting a database
		// descriptor to exist. More investigation is required.
		// Tracked with #76378.
		DefaultTestTenant: base.TODOTestTenantDisabled,
	}
	s, sqlDB, _ := serverutils.StartServer(t, args)
	tdb := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(ctx)

	tdb.Exec(t, `
CREATE DATABASE foo;
SET DATABASE = foo;
CREATE SCHEMA bar;
DROP SCHEMA bar;
`)
	tdb.Exec(t, `BACKUP DATABASE foo INTO 'nodelocal://1/'`)

	tdb.Exec(t, fmt.Sprintf("RESTORE DATABASE %s FROM LATEST IN '%s' WITH new_db_name = '%s'",
		dbName, fromDir, restoredDBName))
	query := fmt.Sprintf("SELECT database_name FROM [SHOW DATABASES] WHERE database_name = '%s'", restoredDBName)
	tdb.CheckQueryResults(t, query, [][]string{{restoredDBName}})

	// Read descriptor without validation.
	execCfg := s.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
	hasSameNameSchema := func(dbName string) (exists bool) {
		require.NoError(t, sql.DescsTxn(ctx, &execCfg, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
			// Using this method to avoid validation.
			id, err := col.LookupDatabaseID(ctx, txn.KV(), dbName)
			if err != nil {
				return err
			}
			res, err := txn.KV().Get(ctx, catalogkeys.MakeDescMetadataKey(execCfg.Codec, id))
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
	require.Falsef(t, hasSameNameSchema(restoredDBName), "corrupted descriptor exists")
}

func fullClusterRestoreSystemRoleMembersWithoutIDs(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		tmpDir, tempDirCleanupFn := testutils.TempDir(t)
		defer tempDirCleanupFn()

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

		// The restore queries are run with `UNSAFE_RESTORE_INCOMPATIBLE_VERSION`
		// option to ensure the restore is successful on development branches. This
		// is because, while the backups were generated on release branches and have
		// versions such as 22.2 in their manifest, the development branch will have
		// a MinSupportedVersion offset by the clusterversion.DevOffset described in
		// `pkg/clusterversion/cockroach_versions.go`. This will mean that the
		// manifest version is always less than the MinSupportedVersion which will
		// in turn fail the restore unless we pass in the specified option to elide
		// the compatibility check.
		sqlDB.Exec(t, fmt.Sprintf("RESTORE FROM '/' IN '%s' WITH UNSAFE_RESTORE_INCOMPATIBLE_VERSION", localFoo))

		sqlDB.CheckQueryResults(t, "SELECT * FROM system.role_members", [][]string{
			{"admin", "root", "true", "2", "1"},
			{"testrole", "testuser1", "false", "100", "101"},
			{"testrole", "testuser2", "true", "100", "102"},
		})
	}
}

func fullClusterRestoreSystemPrivilegesWithoutIDs(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		tmpDir, tempDirCleanupFn := testutils.TempDir(t)
		defer tempDirCleanupFn()

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

		// The restore queries are run with `UNSAFE_RESTORE_INCOMPATIBLE_VERSION`
		// option to ensure the restore is successful on development branches. This
		// is because, while the backups were generated on release branches and have
		// versions such as 22.2 in their manifest, the development branch will have
		// a MinSupportedVersion offset by the clusterversion.DevOffset described in
		// `pkg/clusterversion/cockroach_versions.go`. This will mean that the
		// manifest version is always less than the MinSupportedVersion which will
		// in turn fail the restore unless we pass in the specified option to elide
		// the compatibility check.
		sqlDB.Exec(t, fmt.Sprintf("RESTORE FROM '/' IN '%s' WITH UNSAFE_RESTORE_INCOMPATIBLE_VERSION", localFoo))

		sqlDB.CheckQueryResults(t, "SELECT * FROM system.privileges", [][]string{
			{"public", "/vtable/crdb_internal/tables", "{}", "{}", "4"},
			{"testuser1", "/global/", "{VIEWACTIVITY}", "{}", "100"},
			{"testuser2", "/global/", "{MODIFYCLUSTERSETTING}", "{}", "101"},
		})
	}
}

func fullClusterRestoreSystemDatabaseRoleSettingsWithoutIDs(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		tmpDir, tempDirCleanupFn := testutils.TempDir(t)
		defer tempDirCleanupFn()

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

		// The restore queries are run with `UNSAFE_RESTORE_INCOMPATIBLE_VERSION`
		// option to ensure the restore is successful on development branches. This
		// is because, while the backups were generated on release branches and have
		// versions such as 22.2 in their manifest, the development branch will have
		// a MinSupportedVersion offset by the clusterversion.DevOffset described in
		// `pkg/clusterversion/cockroach_versions.go`. This will mean that the
		// manifest version is always less than the MinSupportedVersion which will
		// in turn fail the restore unless we pass in the specified option to elide
		// the compatibility check.
		sqlDB.Exec(t, fmt.Sprintf("RESTORE FROM '/' IN '%s' WITH UNSAFE_RESTORE_INCOMPATIBLE_VERSION", localFoo))

		sqlDB.CheckQueryResults(t, "SELECT * FROM system.database_role_settings", [][]string{
			{"0", "", "{timezone=America/New_York}", "0"},
			{"0", "testuser1", "{application_name=roachdb}", "100"},
			{"0", "testuser2", "{disallow_full_table_scans=on}", "101"},
		})
	}
}

func fullClusterRestoreSystemExternalConnectionsWithoutIDs(exportDir string) func(t *testing.T) {
	return func(t *testing.T) {
		tmpDir, tempDirCleanupFn := testutils.TempDir(t)
		defer tempDirCleanupFn()

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

		// The restore queries are run with `UNSAFE_RESTORE_INCOMPATIBLE_VERSION`
		// option to ensure the restore is successful on development branches. This
		// is because, while the backups were generated on release branches and have
		// versions such as 22.2 in their manifest, the development branch will have
		// a MinSupportedVersion offset by the clusterversion.DevOffset described in
		// `pkg/clusterversion/cockroach_versions.go`. This will mean that the
		// manifest version is always less than the MinSupportedVersion which will
		// in turn fail the restore unless we pass in the specified option to elide
		// the compatibility check.
		sqlDB.Exec(t, fmt.Sprintf("RESTORE FROM '/' IN '%s' WITH UNSAFE_RESTORE_INCOMPATIBLE_VERSION", localFoo))

		sqlDB.CheckQueryResults(t, "SELECT * FROM system.external_connections", [][]string{
			{"connection1", "2023-03-20 01:26:50.174781 +0000 +0000", "2023-03-20 01:26:50.174781 +0000 +0000", "STORAGE",
				"\b\u0005\u0012\u0019\n\u0017userfile:///connection1", "testuser1", "100"},
			{"connection2", "2023-03-20 01:26:51.223986 +0000 +0000", "2023-03-20 01:26:51.223986 +0000 +0000", "STORAGE",
				"\b\u0005\u0012\u0019\n\u0017userfile:///connection2", "testuser2", "101"},
		})
	}
}

func fullClusterRestoreSystemTenantSettingsSkipVersionOverride(
	exportDir string,
) func(t *testing.T) {
	return func(t *testing.T) {
		tmpDir, tempDirCleanupFn := testutils.TempDir(t)
		defer tempDirCleanupFn()

		_, sqlDB, cleanup := backupRestoreTestSetupEmpty(t, singleNode, tmpDir,
			InitManualReplication, base.TestClusterArgs{
				ServerArgs: base.TestServerArgs{
					// This test exercises the restore behaviour on the system
					// tenant, so we disable non-system tenants.
					DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
					Knobs: base.TestingKnobs{
						JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
					},
				}})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(tmpDir, "foo"))
		require.NoError(t, err)

		// The restore queries are run with `UNSAFE_RESTORE_INCOMPATIBLE_VERSION`
		// option to ensure the restore is successful on development branches. This
		// is because, while the backups were generated on release branches and have
		// versions such as 22.2 in their manifest, the development branch will have
		// a MinSupportedVersion offset by the clusterversion.DevOffset described in
		// `pkg/clusterversion/cockroach_versions.go`. This will mean that the
		// manifest version is always less than the MinSupportedVersion which will
		// in turn fail the restore unless we pass in the specified option to elide
		// the compatibility check.
		sqlDB.Exec(t, fmt.Sprintf("RESTORE FROM LATEST IN '%s' WITH UNSAFE_RESTORE_INCOMPATIBLE_VERSION", localFoo))

		// No 'version' key for tenant_id = 0 in system.tenant_settings.
		sqlDB.CheckQueryResults(t, "SELECT * FROM system.tenant_settings", [][]string{
			{"2", "cluster.organization", "Cockroach Labs - Production Testing", "2024-06-24 20:11:21.433048 +0000 +0000", "s", "NULL"},
			{"2", "enterprise.license", "", "2024-06-24 20:11:21.441043 +0000 +0000", "s", "NULL"},
		})
	}
}
