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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func Example_userfile_upload() {
	c := newCLITest(cliTestParams{})
	defer c.cleanup()

	file, cleanUp := createTestFile("test.csv", "content")
	defer cleanUp()

	c.Run(fmt.Sprintf("userfile upload %s /test/file1.csv", file))
	c.Run(fmt.Sprintf("userfile upload %s test/file2.csv", file))
	c.Run(fmt.Sprintf("userfile upload %s file2.csv", file))
	c.Run(fmt.Sprintf("userfile upload %s /test/file1.csv", file))
	// User passes no destination so we use the basename of the source as the
	// filename.
	c.Run(fmt.Sprintf("userfile upload %s", file))
	c.Run(fmt.Sprintf("userfile upload %s /test/../../file1.csv", file))
	c.Run(fmt.Sprintf("userfile upload %s /test/./file1.csv", file))
	c.Run(fmt.Sprintf("userfile upload %s test/file1.csv", file))
	c.Run(fmt.Sprintf("userfile upload notexist.csv /test/file1.csv"))
	c.Run(fmt.Sprintf("userfile upload %s /test/À.csv", file))
	// Test fully qualified URI specifying db.schema.tablename_prefix.
	c.Run(fmt.Sprintf("userfile upload %s userfile://defaultdb.public.foo/test/file1.csv", file))

	// Output:
	// userfile upload test.csv /test/file1.csv
	// successfully uploaded to userfile://defaultdb.public.userfiles_root/test/file1.csv
	// userfile upload test.csv test/file2.csv
	// successfully uploaded to userfile://defaultdb.public.userfiles_root/test/file2.csv
	// userfile upload test.csv file2.csv
	// successfully uploaded to userfile://defaultdb.public.userfiles_root/file2.csv
	// userfile upload test.csv /test/file1.csv
	// ERROR: destination file already exists for /test/file1.csv
	// userfile upload test.csv
	// successfully uploaded to userfile://defaultdb.public.userfiles_root/test-temp-prefix-test.csv
	// userfile upload test.csv /test/../../file1.csv
	// ERROR: path /test/../../file1.csv changes after normalization to /file1.csv. userfile upload does not permit such path constructs
	// userfile upload test.csv /test/./file1.csv
	// ERROR: path /test/./file1.csv changes after normalization to /test/file1.csv. userfile upload does not permit such path constructs
	// userfile upload test.csv test/file1.csv
	// ERROR: destination file already exists for /test/file1.csv
	// userfile upload notexist.csv /test/file1.csv
	// ERROR: open notexist.csv: no such file or directory
	// userfile upload test.csv /test/À.csv
	// successfully uploaded to userfile://defaultdb.public.userfiles_root/test/À.csv
	// userfile upload test.csv userfile://defaultdb.public.foo/test/file1.csv
	// successfully uploaded to userfile://defaultdb.public.foo/test/file1.csv
}

func checkUserFileContent(
	ctx context.Context,
	t *testing.T,
	execcCfg interface{},
	user, userfileURI string,
	expectedContent []byte,
) {
	store, err := execcCfg.(sql.ExecutorConfig).DistSQLSrv.ExternalStorageFromURI(ctx,
		userfileURI, user)
	require.NoError(t, err)
	reader, err := store.ReadFile(ctx, "")
	require.NoError(t, err)
	got, err := ioutil.ReadAll(reader)
	require.NoError(t, err)
	require.True(t, bytes.Equal(got, expectedContent))
}

func TestUserFileUpload(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	defer c.cleanup()

	dir, cleanFn := testutils.TempDir(t)
	defer cleanFn()
	ctx := context.Background()

	for i, tc := range []struct {
		name        string
		fileContent []byte
	}{
		{
			"empty",
			[]byte{},
		},
		{
			"exactly-one-chunk",
			make([]byte, chunkSize),
		},
		{
			"exactly-five-chunks",
			make([]byte, chunkSize*5),
		},
		{
			"less-than-one-chunk",
			make([]byte, chunkSize-100),
		},
		{
			"more-than-one-chunk",
			make([]byte, chunkSize+100),
		},
	} {
		// Write local file.
		filePath := filepath.Join(dir, fmt.Sprintf("file%d.csv", i))
		err := ioutil.WriteFile(filePath, tc.fileContent, 0666)
		require.NoError(t, err)
		t.Run(tc.name, func(t *testing.T) {
			destination := fmt.Sprintf("/test/file%d.csv", i)

			_, err = c.RunWithCapture(fmt.Sprintf("userfile upload %s %s", filePath, destination))
			require.NoError(t, err)

			checkUserFileContent(ctx, t, c.ExecutorConfig(), security.RootUser,
				constructUserfileDestinationURI("", destination, security.RootUser),
				tc.fileContent)
		})

		t.Run(tc.name+"_fullURI", func(t *testing.T) {
			destination := fmt.Sprintf("userfile://defaultdb.public.foo/test/file%d.csv", i)
			_, err = c.RunWithCapture(fmt.Sprintf("userfile upload %s %s", filePath,
				destination))
			require.NoError(t, err)

			checkUserFileContent(ctx, t, c.ExecutorConfig(), security.RootUser,
				destination, tc.fileContent)
		})
	}
}
