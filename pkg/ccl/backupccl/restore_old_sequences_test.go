// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
//  VERSION=...
//  roachprod create local
//  roachprod wipe local
//  roachprod stage local release ${VERSION}
//  roachprod start local
//  roachprod sql local:1 -- -e "$(cat pkg/ccl/backupccl/testdata/restore_old_sequences/create.sql)"
//  roachprod sql local:1 -- -e "BACKUP DATABASE test TO 'nodelocal://1/backup'"
//  # Then grab the backups and put the files into the appropriate
//  # testdata directory.
//
func TestRestoreOldSequences(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	var (
		testdataBase = testutils.TestDataPath(t, "restore_old_sequences")
		exportDirs   = testdataBase + "/exports"
	)

	t.Run("sequences-restore", func(t *testing.T) {
		dirs, err := ioutil.ReadDir(exportDirs)
		require.NoError(t, err)
		for _, dir := range dirs {
			require.True(t, dir.IsDir())
			exportDir, err := filepath.Abs(filepath.Join(exportDirs, dir.Name()))
			require.NoError(t, err)
			t.Run(dir.Name(), restoreOldSequencesTest(exportDir))
		}
	})
}

func restoreOldSequencesTest(exportDir string) func(t *testing.T) {
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
		const totalRows = 4
		if importedRows != totalRows {
			t.Fatalf("expected %d rows, got %d", totalRows, importedRows)
		}

		// Verify that sequences created in older versions cannot be renamed, nor can the
		// database they are referencing.
		sqlDB.ExpectErr(t,
			`pq: cannot rename relation "test.public.s" because view "t1" depends on it`,
			`ALTER SEQUENCE test.s RENAME TO test.s2`)
		sqlDB.ExpectErr(t,
			`pq: cannot rename relation "test.public.t1_i_seq" because view "t1" depends on it`,
			`ALTER SEQUENCE test.t1_i_seq RENAME TO test.t1_i_seq_new`)
		sqlDB.ExpectErr(t,
			`pq: cannot rename database because relation "test.public.t1" depends on relation "test.public.s"`,
			`ALTER DATABASE test RENAME TO new_test`)

		sequenceResults := [][]string{
			{"1", "1"},
			{"2", "2"},
		}

		// Verify that tables with old sequences aren't corrupted.
		sqlDB.Exec(t, `SET database = test; INSERT INTO test.t1 VALUES (default, default)`)
		sqlDB.CheckQueryResults(t, `SELECT * FROM test.t1 ORDER BY i`, sequenceResults)

		// Verify that the views are okay, and the sequences it depends on cannot be renamed.
		sqlDB.CheckQueryResults(t, `SET database = test; SELECT * FROM test.v`, [][]string{{"1"}})
		sqlDB.CheckQueryResults(t, `SET database = test; SELECT * FROM test.v2`, [][]string{{"2"}})
		sqlDB.ExpectErr(t,
			`pq: cannot rename relation "s2" because view "v" depends on it`,
			`ALTER SEQUENCE s2 RENAME TO s3`)
		sqlDB.CheckQueryResults(t, `SET database = test; SHOW CREATE VIEW test.v`, [][]string{{
			"test.public.v", "CREATE VIEW public.v (\n\tnextval\n) AS (SELECT nextval('s2':::STRING))",
		}})
	}
}
