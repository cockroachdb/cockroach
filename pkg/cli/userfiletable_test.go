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
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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
	// Test URI with no db.schema.tablename_prefix.
	c.Run(fmt.Sprintf("userfile upload %s userfile:///test/file3.csv", file))

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
	// userfile upload test.csv userfile:///test/file3.csv
	// successfully uploaded to userfile://defaultdb.public.userfiles_root/test/file3.csv
}

func checkUserFileContent(
	ctx context.Context,
	t *testing.T,
	execcCfg interface{},
	user security.SQLUsername,
	userfileURI string,
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

			_, err = c.RunWithCapture(fmt.Sprintf("userfile upload %s %s", filePath,
				destination))
			require.NoError(t, err)

			checkUserFileContent(ctx, t, c.ExecutorConfig(), security.RootUserName(),
				constructUserfileDestinationURI("", destination, security.RootUserName()),
				tc.fileContent)
		})

		t.Run(tc.name+"_fullURI", func(t *testing.T) {
			destination := fmt.Sprintf("userfile://defaultdb.public.foo/test/file%d.csv", i)
			_, err = c.RunWithCapture(fmt.Sprintf("userfile upload %s %s", filePath,
				destination))
			require.NoError(t, err)

			checkUserFileContent(ctx, t, c.ExecutorConfig(), security.RootUserName(),
				destination, tc.fileContent)
		})

		// Not specifying a qualified table name should default to writing to
		// `defaultdb.public.userfiles_username`.
		t.Run(tc.name+"_no-host-uri", func(t *testing.T) {
			destination := fmt.Sprintf("userfile:///test/file%d.csv", i)
			_, err = c.RunWithCapture(fmt.Sprintf("userfile upload %s %s", filePath,
				destination))
			require.NoError(t, err)

			checkUserFileContent(ctx, t, c.ExecutorConfig(), security.RootUserName(),
				destination, tc.fileContent)
		})
	}
}

func checkListedFiles(t *testing.T, c cliTest, uri string, args string, expectedFiles []string) {
	cliOutput, err := c.RunWithCaptureArgs([]string{"userfile", "list", uri, args})
	require.NoError(t, err)
	cliOutput = strings.TrimSpace(cliOutput)

	var listedFiles []string
	// Split returns a slice of len 1 if output is empty but sep is \n.
	if cliOutput != "" {
		listedFiles = strings.Split(cliOutput, "\n")
	}

	require.Equal(t, expectedFiles, listedFiles)
}

func checkDeletedFiles(t *testing.T, c cliTest, uri, args string, expectedFiles []string) {
	cliOutput, err := c.RunWithCaptureArgs([]string{"userfile", "delete", uri, args})
	require.NoError(t, err)
	cliOutput = strings.TrimSpace(cliOutput)

	var deletedFiles []string
	// Split returns a slice of len 1 if output is empty but sep is \n.
	if cliOutput != "" {
		deletedFiles = strings.Split(cliOutput, "\n")
		for i, file := range deletedFiles {
			deletedFiles[i] = strings.TrimPrefix(file, "successfully deleted ")
		}
	}

	require.Equal(t, expectedFiles, deletedFiles)
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
				"list-all-in-default-using-star",
				"*",
				abs(fileNames),
			},
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
				"no-host-list-all-in-default",
				"userfile:///*/*/*.csv",
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
				nil,
			},
			{
				"list-no-matches",
				"file/letters/dataD.csv",
				nil,
			},
			{
				"list-escaped-star",
				"file/*/\\*.csv",
				nil,
			},
			{
				"list-escaped-range",
				"file/*/data\\[0-9\\].csv",
				nil,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				checkListedFiles(t, c, tc.URI, "", tc.resultList)
			})
		}
	})
	require.NoError(t, os.RemoveAll(dir))
}

func TestUserFileDelete(t *testing.T) {
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
		if in == nil {
			return nil
		}

		out := make([]string, len(in))
		for i := range in {
			out[i] = defaultUserfileURLSchemeAndHost.String() + "/" + in[i]
		}
		return out
	}

	t.Run("DeleteFiles", func(t *testing.T) {
		for _, tc := range []struct {
			name               string
			URI                string
			writeList          []string
			expectedDeleteList []string
			postDeleteList     []string
		}{
			{
				"delete-all-in-default",
				"*",
				fileNames,
				abs(fileNames),
				nil,
			},
			{
				"no-glob-path-delete-all-in-default",
				defaultUserfileURLSchemeAndHost.String(),
				fileNames,
				abs(fileNames),
				nil,
			},
			{
				"no-host-delete-all-in-default",
				"userfile:///*/*/*.*",
				fileNames,
				abs(fileNames),
				nil,
			},
			{
				"well-formed-userfile-uri",
				defaultUserfileURLSchemeAndHost.String() + "/file/letters/*.csv",
				fileNames,
				abs(dataLetterFiles),
				append(dataNumberFiles, unicodeFile...),
			},
			{
				"delete-unicode-file",
				defaultUserfileURLSchemeAndHost.String() + "/file/unicode/á.csv",
				fileNames,
				abs(unicodeFile),
				append(dataLetterFiles, dataNumberFiles...),
			},
			{
				"delete-data-num-csv",
				"file/numbers/data[0-9].csv",
				fileNames,
				abs(dataNumberFiles),
				append(dataLetterFiles, unicodeFile...),
			},
			{
				"wildcard-bucket-and-filename",
				"*/numbers/*.csv",
				fileNames,
				abs(dataNumberFiles),
				append(dataLetterFiles, unicodeFile...),
			},
			{
				"delete-all-csv-skip-dir",
				// filepath.Glob() assumes that / is the separator, and enforces that it's there.
				// So this pattern would not actually match anything.
				"file/*.csv",
				fileNames,
				nil,
				fileNames,
			},
			{
				"delete-no-matches",
				"file/letters/dataD.csv",
				fileNames,
				nil,
				fileNames,
			},
			{
				"delete-escaped-star",
				"file/*/\\*.csv",
				fileNames,
				nil,
				fileNames,
			},
			{
				"delete-escaped-range",
				"file/*/data\\[0-9\\].csv",
				fileNames,
				nil,
				fileNames,
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				// Upload files to default userfile URI.
				for _, file := range tc.writeList {
					_, err = c.RunWithCapture(fmt.Sprintf("userfile upload %s %s", localFilePath, file))
					require.NoError(t, err)
				}

				// List files prior to deletion.
				checkListedFiles(t, c, "", "", abs(tc.writeList))

				// Delete files.
				checkDeletedFiles(t, c, tc.URI, "", tc.expectedDeleteList)

				// List files after deletion.
				checkListedFiles(t, c, "", "", abs(tc.postDeleteList))

				// Cleanup all files for next test run.
				_, err = c.RunWithCaptureArgs([]string{"userfile", "delete", "*"})
				require.NoError(t, err)
			})
		}
	})
	require.NoError(t, os.RemoveAll(dir))
}

func TestUsernameUserfileInteraction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := newCLITest(cliTestParams{t: t})
	c.omitArgs = true
	defer c.cleanup()

	dir, cleanFn := testutils.TempDir(t)
	defer cleanFn()

	localFilePath := filepath.Join(dir, "test.csv")
	fileContent := []byte("a")
	err := ioutil.WriteFile(localFilePath, []byte("a"), 0666)
	require.NoError(t, err)

	rootURL, cleanup := sqlutils.PGUrl(t, c.ServingSQLAddr(), t.Name(),
		url.User(security.RootUser))
	defer cleanup()

	conn := makeSQLConn(rootURL.String())
	defer conn.Close()

	ctx := context.Background()

	t.Run("usernames", func(t *testing.T) {
		for _, tc := range []struct {
			name     string
			username string
		}{
			{
				"simple-username",
				"foo",
			},
			{
				"digit-username",
				"123foo",
			},
			{
				"special-char-username",
				"foo.foo",
			},
		} {
			createUserQuery := fmt.Sprintf(`CREATE USER "%s" WITH PASSWORD 'a'`, tc.username)
			err = conn.Exec(createUserQuery, nil)
			require.NoError(t, err)

			privsUserQuery := fmt.Sprintf(`GRANT CREATE ON DATABASE defaultdb TO "%s"`, tc.username)
			err = conn.Exec(privsUserQuery, nil)
			require.NoError(t, err)

			userURL, cleanup2 := sqlutils.PGUrlWithOptionalClientCerts(t, c.ServingSQLAddr(), t.Name(),
				url.UserPassword(tc.username, "a"), false)
			defer cleanup2()

			_, err := c.RunWithCapture(fmt.Sprintf("userfile upload %s %s --url=%s",
				localFilePath, tc.name, userURL.String()))
			require.NoError(t, err)

			user, err := security.MakeSQLUsernameFromUserInput(tc.username, security.UsernameCreation)
			require.NoError(t, err)
			uri := constructUserfileDestinationURI("", tc.name, user)
			checkUserFileContent(ctx, t, c.ExecutorConfig(), user, uri, fileContent)

			checkListedFiles(t, c, "", fmt.Sprintf("--url=%s", userURL.String()), []string{uri})

			checkDeletedFiles(t, c, "", fmt.Sprintf("--url=%s", userURL.String()),
				[]string{uri})
		}
	})
}
