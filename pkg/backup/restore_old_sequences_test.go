// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestRestoreOldSequences ensures that we can successfully restore tables
// and views created in older versions into a newer version.
//
// The files being restored live in testdata and are all made from the same
// input SQL which lives in <testdataBase>/create.sql.
//
// The SSTs were created via the following commands:
//
//	VERSION=...
//	roachprod create local
//	roachprod wipe local
//	roachprod stage local release ${VERSION}
//	roachprod start local
//	roachprod sql local:1 -- -e "$(cat pkg/backup/testdata/restore_old_sequences/create.sql)"
//	roachprod sql local:1 -- -e "BACKUP DATABASE test TO 'nodelocal://1/backup'"
//	# Then grab the backups and put the files into the appropriate
//	# testdata directory.
func TestRestoreOldSequences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	var (
		testdataBase = datapathutils.TestDataPath(t, "restore_old_sequences")
		exportDirs   = testdataBase + "/exports"
	)

	t.Run("sequences-restore", func(t *testing.T) {
		dirs, err := os.ReadDir(exportDirs)
		require.NoError(t, err)
		for _, isSchemaOnly := range []bool{true, false} {
			suffix := ""
			if isSchemaOnly {
				suffix = "-schema-only"
			}
			for _, dir := range dirs {
				require.True(t, dir.IsDir())
				exportDir, err := filepath.Abs(filepath.Join(exportDirs, dir.Name()))
				require.NoError(t, err)
				t.Run(dir.Name()+suffix, restoreOldSequencesTest(exportDir, isSchemaOnly))
			}
		}
	})
}

func restoreOldSequencesTest(exportDir string, isSchemaOnly bool) func(t *testing.T) {
	return func(t *testing.T) {
		params := base.TestServerArgs{}
		params.Settings = cluster.MakeTestingClusterSettingsWithVersions(clusterversion.Latest.Version(),
			clusterversion.MinSupported.Version(), false /* initializeVersion */)
		const numAccounts = 1000
		_, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
			InitManualReplication, base.TestClusterArgs{ServerArgs: params})
		defer cleanup()
		err := os.Symlink(exportDir, filepath.Join(dir, "foo"))
		require.NoError(t, err)
		sqlDB.Exec(t, `CREATE DATABASE test`)
		var unused string
		var importedRows int
		// The restore queries are run with `UNSAFE_RESTORE_INCOMPATIBLE_VERSION`
		// option to ensure the restore is successful on development branches. This
		// is because, while the backups were generated on release branches and have
		// versions such as 22.2 in their manifest, the development branch will have
		// a MinSupportedVersion offset by the clusterversion.DevOffset described in
		// `pkg/clusterversion/cockroach_versions.go`. This will mean that the
		// manifest version is always less than the MinSupportedVersion which will
		// in turn fail the restore unless we pass in the specified option to elide
		// the compatibility check.
		restoreQuery := `RESTORE test.* FROM LATEST IN $1 WITH UNSAFE_RESTORE_INCOMPATIBLE_VERSION`
		if isSchemaOnly {
			restoreQuery = restoreQuery + ", schema_only"
		}
		sqlDB.QueryRow(t, restoreQuery, localFoo).Scan(
			&unused, &unused, &unused, &importedRows,
		)
		totalRows := 4
		if isSchemaOnly {
			totalRows = 0
		}
		if importedRows != totalRows {
			t.Fatalf("expected %d rows, got %d", totalRows, importedRows)
		}

		// Verify that restored sequences are now referenced by ID.
		var createTable string
		sqlDB.QueryRow(t, `SHOW CREATE test.t1`).Scan(&unused, &createTable)
		require.Contains(t, createTable, "i INT8 NOT NULL DEFAULT nextval('test.public.t1_i_seq'::REGCLASS)")
		require.Contains(t, createTable, "j INT8 NOT NULL DEFAULT nextval('test.public.s'::REGCLASS)")
		sqlDB.QueryRow(t, `SHOW CREATE test.v`).Scan(&unused, &createTable)
		require.Contains(t, createTable, "SELECT nextval('test.public.s2'::REGCLASS)")
		sqlDB.QueryRow(t, `SHOW CREATE test.v2`).Scan(&unused, &createTable)
		require.Contains(t, createTable, "SELECT nextval('test.public.s2'::REGCLASS) AS k")

		// Verify that, as a result, all sequences can now be renamed.
		sqlDB.Exec(t, `ALTER SEQUENCE test.t1_i_seq RENAME TO test.t1_i_seq_new`)
		sqlDB.Exec(t, `ALTER SEQUENCE test.s RENAME TO test.s_new`)
		sqlDB.Exec(t, `ALTER SEQUENCE test.s2 RENAME TO test.s2_new`)

		// Finally, verify that sequences are correctly restored and can be used in tables/views.
		sqlDB.Exec(t, `INSERT INTO test.t1 VALUES (default, default)`)
		expectedRows := [][]string{
			{"1", "1"},
			{"2", "2"},
		}
		if isSchemaOnly {
			// In a schema_only RESTORE, the restored sequence will be empty
			expectedRows = [][]string{
				{"1", "1"},
			}
		}
		sqlDB.CheckQueryResults(t, `SELECT * FROM test.t1 ORDER BY i`, expectedRows)
		sqlDB.CheckQueryResults(t, `SELECT * FROM test.v`, [][]string{{"1"}})
		sqlDB.CheckQueryResults(t, `SELECT * FROM test.v2`, [][]string{{"2"}})
	}
}
