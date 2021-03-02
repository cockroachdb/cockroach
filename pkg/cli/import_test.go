// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package cli

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// runImportCLICommand runs the import CLI command in a special way for the
// purpose of unit testing. Since import requires a CCL binary and the CLI test
// infrastructure has not yet been ported to test ccl backed CLI commands, we
// use this method to test everything except the actual IMPORT, which we
// consider a well tested feature on its own.
// Instead of running the import, test knobs return the complete IMPORT query
// that would have been run. Apart from checking the query's correctness, this
// method ensures that the upload and deletion semantics to userfile are working
// as expected.
func runImportCLICommand(
	ctx context.Context, t *testing.T, cliCmd string, dumpFilePath string, c TestCLI,
) string {
	knobs, unsetImportCLIKnobs := setImportCLITestingKnobs()
	defer unsetImportCLIKnobs()

	var out string
	var err error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		out, err = c.RunWithCapture(cliCmd)
		require.NoError(t, err)
		wg.Done()
	}()

	// The import will block after uploading to the userfile table, giving us
	// a chance to verify that the dump file has been uploaded successfully.
	<-knobs.uploadComplete
	data, err := ioutil.ReadFile(dumpFilePath)
	require.NoError(t, err)
	userfileURI := constructUserfileDestinationURI(dumpFilePath, "", security.RootUserName())
	checkUserFileContent(ctx, t, c.ExecutorConfig(), security.RootUserName(), userfileURI, data)
	knobs.pauseAfterUpload <- struct{}{}
	wg.Wait()

	// Check that the dump file has been cleaned up after the import CLI command
	// has completed.
	store, err := c.ExecutorConfig().(sql.ExecutorConfig).DistSQLSrv.ExternalStorageFromURI(ctx,
		userfileURI, security.RootUserName())
	require.NoError(t, err)
	_, err = store.ReadFile(ctx, "")
	testutils.IsError(err, "file doesn't exist")

	var output []string
	if out != "" {
		output = strings.Split(out, "\n")
	}

	require.Equal(t, 2, len(output))
	return output[1]
}

func TestImportCLI(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := NewCLITest(TestCLIParams{T: t})
	defer c.Cleanup()

	ctx := context.Background()

	for _, tc := range []struct {
		name                     string
		format                   string
		dumpFilePath             string
		args                     string
		expectedImportQuery      string
		expectedImportTableQuery string
	}{
		{
			"pgdump",
			"PGDUMP",
			"testdata/import/db.sql",
			"",
			"IMPORT PGDUMP 'userfile://defaultdb.public.userfiles_root/db." +
				"sql' WITH max_row_size='524288'",
			"IMPORT TABLE foo FROM PGDUMP " +
				"'userfile://defaultdb.public.userfiles_root/db.sql' WITH max_row_size='524288'",
		},
		{
			"pgdump-with-options",
			"PGDUMP",
			"testdata/import/db.sql",
			"--max-row-size=1000 --skip-foreign-keys=true --row-limit=10 " +
				"--ignore-unsupported-statements=true --log-ignored-statements='foo://bar'",
			"IMPORT PGDUMP 'userfile://defaultdb.public.userfiles_root/db." +
				"sql' WITH max_row_size='1000', skip_foreign_keys, row_limit='10', ignore_unsupported_statements, " +
				"log_ignored_statements='foo://bar'",
			"IMPORT TABLE foo FROM PGDUMP " +
				"'userfile://defaultdb.public.userfiles_root/db.sql' WITH max_row_size='1000', " +
				"skip_foreign_keys, row_limit='10', ignore_unsupported_statements, log_ignored_statements='foo://bar'",
		},
		{
			"mysql",
			"MYSQLDUMP",
			"testdata/import/db.sql",
			"",
			"IMPORT MYSQLDUMP 'userfile://defaultdb.public.userfiles_root/db.sql'",
			"IMPORT TABLE foo FROM MYSQLDUMP 'userfile://defaultdb.public.userfiles_root/db.sql'",
		},
		{
			"mysql-with-options",
			"MYSQLDUMP",
			"testdata/import/db.sql",
			"--skip-foreign-keys=true --row-limit=10",
			"IMPORT MYSQLDUMP 'userfile://defaultdb.public.userfiles_root/db." +
				"sql' WITH skip_foreign_keys, row_limit='10'",
			"IMPORT TABLE foo FROM MYSQLDUMP " +
				"'userfile://defaultdb.public.userfiles_root/db.sql' WITH skip_foreign_keys, row_limit='10'",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			importDumpCLICmd := fmt.Sprintf("import db %s %s", tc.format, tc.dumpFilePath)
			if tc.args != "" {
				importDumpCLICmd += " " + tc.args
			}

			output := runImportCLICommand(ctx, t, importDumpCLICmd, tc.dumpFilePath, c)

			require.Equal(t, tc.expectedImportQuery, output)
		})

		t.Run(tc.name+"_table", func(t *testing.T) {
			importDumpCLICmd := fmt.Sprintf("import table foo %s %s", tc.format, tc.dumpFilePath)
			if tc.args != "" {
				importDumpCLICmd += " " + tc.args
			}

			output := runImportCLICommand(ctx, t, importDumpCLICmd, tc.dumpFilePath, c)

			require.Equal(t, tc.expectedImportTableQuery, output)
		})
	}
}
