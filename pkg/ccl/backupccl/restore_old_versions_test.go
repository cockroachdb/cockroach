package backupccl_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

const (
	testdataBase = "testdata/restore_old_versions"
	exportDirs   = testdataBase + "/exports"
)

// TestRestoreOldVersions ensures that we can successfully restore tables
// and databases exported by old version.
//
// The files being restored live in testdata and are all made from the same
// input SQL.
func TestRestoreOldVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dirs, err := ioutil.ReadDir(exportDirs)
	require.NoError(t, err)
	for _, dir := range dirs {
		require.True(t, dir.IsDir())
		t.Run(dir.Name(), restoreOldVersionTest(dir.Name()))
	}
}

func restoreOldVersionTest(version string) func(t *testing.T) {
	return func(t *testing.T) {
		params := base.TestServerArgs{}
		const numAccounts = 1000
		_, _, sqlDB, dir, cleanup := backupRestoreTestSetupWithParams(t, singleNode, numAccounts,
			initNone, base.TestClusterArgs{ServerArgs: params})
		defer cleanup()
		restoreBaseDir, err := filepath.Abs(filepath.Join(exportDirs, version))
		require.NoError(t, err)
		err = os.Symlink(restoreBaseDir, filepath.Join(dir, "foo"))
		require.NoError(t, err)
		sqlDB.Exec(t, `CREATE DATABASE test`)
		var unused string
		var importedRows int
		sqlDB.QueryRow(t, `RESTORE test.* FROM $1`, localFoo).Scan(
			&unused, &unused, &unused, &importedRows, &unused, &unused, &unused,
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
