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
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
		if err != nil {
			t.Fatal(err)
		}
		t.Run(tc.name, func(t *testing.T) {
			destination := fmt.Sprintf("/test/file%d.csv", i)

			_, err = c.RunWithCapture(fmt.Sprintf("userfile upload %s %s", filePath, destination))
			if err != nil {
				t.Fatal(err)
			}

			checkUserFileContent(ctx, t, c.ExecutorConfig(), security.RootUser,
				constructUserfileDestinationURI("", destination, security.RootUser),
				tc.fileContent)
		})

		t.Run(tc.name+"_fullURI", func(t *testing.T) {
			destination := fmt.Sprintf("userfile://defaultdb.public.foo/test/file%d.csv", i)
			_, err = c.RunWithCapture(fmt.Sprintf("userfile upload %s %s", filePath,
				destination))
			if err != nil {
				t.Fatal(err)
			}

			checkUserFileContent(ctx, t, c.ExecutorConfig(), security.RootUser,
				destination, tc.fileContent)
		})

		require.NoError(t, os.RemoveAll(dir))
	}
}

func TestUserFileList(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	c.omitArgs = true
	defer c.cleanup()

	dir, cleanFn := testutils.TempDir(t)
	defer cleanFn()

	dataLetterFiles := []string{"file/letters/dataA.csv", "file/letters/dataB.csv"}
	dataNumberFiles := []string{"file/numbers/data1.csv", "file/numbers/data2.csv"}
	unicodeFile := []string{"file/unicode/á.csv"}
	fileNames := append(dataLetterFiles, dataNumberFiles...)
	fileNames = append(fileNames, unicodeFile...)
	sort.Strings(fileNames)

	localFilePath := filepath.Join(dir, "test.csv")
	err := ioutil.WriteFile(localFilePath, []byte("a"), 0666)
	require.NoError(t, err)

	defaultUserfileURLSchemeAndHost := url.URL{
		Scheme: defaultUserfileScheme,
		Host:   defaultQualifiedNamePrefix + security.RootUser,
	}

	abs := func(in []string) []string {
		out := make([]string, len(in))
		for i := range in {
			out[i] = defaultUserfileURLSchemeAndHost.String() + "/" + in[i]
		}
		return out
	}

	t.Run("ListFiles", func(t *testing.T) {
		// Upload files to default userfile URI.
		for _, file := range fileNames {
			_, err = c.RunWithCapture(fmt.Sprintf("userfile upload %s %s", localFilePath, file))
			require.NoError(t, err)
		}

		for _, tc := range []struct {
			name       string
			URI        string
			resultList []string
		}{
			{
				"no-uri-list-all-in-default",
				"",
				abs(fileNames),
			},
			{
				"no-glob-path-list-all-in-default",
				defaultUserfileURLSchemeAndHost.String(),
				abs(fileNames),
			},
			{
				"well-formed-userfile-uri",
				defaultUserfileURLSchemeAndHost.String() + "/file/letters/*.csv",
				abs(dataLetterFiles),
			},
			{
				"only-glob",
				"file/letters/*.csv",
				abs(dataLetterFiles),
			},
			{
				"list-data-num-csv",
				"file/numbers/data[0-9].csv",
				abs(dataNumberFiles),
			},
			{
				"wildcard-bucket-and-filename",
				"*/numbers/*.csv",
				abs(dataNumberFiles),
			},
			{
				"list-all-csv-skip-dir",
				// filepath.Glob() assumes that / is the separator, and enforces that it's there.
				// So this pattern would not actually match anything.
				"file/*.csv",
				[]string{},
			},
			{
				"list-no-matches",
				"file/letters/dataD.csv",
				[]string{},
			},
			{
				"list-escaped-star",
				"file/*/\\*.csv",
				[]string{},
			},
			{
				"list-escaped-range",
				"file/*/data\\[0-9\\].csv",
				[]string{},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				output, err := c.RunWithCaptureArgs([]string{"userfile", "ls", tc.URI})
				require.NoError(t, err)
				output = strings.TrimSpace(output)

				var listedFiles []string
				// Split returns a slice of len 1 if output is empty but sep is \n.
				if output == "" {
					listedFiles = []string{}
				} else {
					listedFiles = strings.Split(output, "\n")
				}

				if len(listedFiles) != len(tc.resultList) {
					t.Fatal(`listed incorrect number of files`, listedFiles)
				}
				for i, got := range listedFiles {
					if expected := tc.resultList[i]; got != expected {
						t.Fatal(`resulting list is incorrect. got: `, got, `expected: `, expected, "\n",
							listedFiles)
					}
				}
			})
		}
	})
	require.NoError(t, os.RemoveAll(dir))
}
