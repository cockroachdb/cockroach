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
	c := NewCLITest(TestCLIParams{})
	defer c.Cleanup()

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
	c.Run("userfile upload notexist.csv /test/file1.csv")
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

	c := NewCLITest(TestCLIParams{T: t})
	defer c.Cleanup()

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

func checkListedFiles(t *testing.T, c TestCLI, uri string, args string, expectedFiles []string) {
	cmd := []string{"userfile", "list", uri, args}
	cliOutput, err := c.RunWithCaptureArgs(cmd)
	require.NoError(t, err)
	cliOutput = strings.TrimSpace(cliOutput)

	var listedFiles []string
	// Split returns a slice of len 1 if output is empty but sep is \n.
	if cliOutput != "" {
		listedFiles = strings.Split(cliOutput, "\n")
	}

	require.Equal(t, expectedFiles, listedFiles, "listed files from %v", cmd)
}

func checkDeletedFiles(t *testing.T, c TestCLI, uri, args string, expectedFiles []string) {
	cmd := []string{"userfile", "delete", uri, args}
	cliOutput, err := c.RunWithCaptureArgs(cmd)
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

	require.Equal(t, expectedFiles, deletedFiles, "deleted files when running %v", cmd)
}

func TestUserfile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := NewCLITest(TestCLIParams{T: t})
	c.omitArgs = true
	defer c.Cleanup()

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

	for tcNum, tc := range []struct {
		name            string
		URI             string
		writeList       []string
		expectedMatches []string
		postDeleteList  []string
	}{
		{
			"match-all",
			"",
			fileNames,
			fileNames,
			nil,
		},
		{
			"match-all-with-slash",
			"/",
			fileNames,
			fileNames,
			nil,
		},
		{
			"match-all-with-slash-prefix",
			"/file",
			fileNames,
			fileNames,
			nil,
		},
		{
			"match-all-with-prefix",
			"file",
			fileNames,
			fileNames,
			nil,
		},
		{
			"no-glob-path-match-all-in-default",
			defaultUserfileURLSchemeAndHost.String(),
			fileNames,
			fileNames,
			nil,
		},
		{
			"no-host-match-all-in-default",
			"userfile:///*/*/*.*",
			fileNames,
			fileNames,
			nil,
		},
		{
			"well-formed-userfile-uri",
			defaultUserfileURLSchemeAndHost.String() + "/file/letters/*.csv",
			fileNames,
			dataLetterFiles,
			append(dataNumberFiles, unicodeFile...),
		},
		{
			"match-unicode-file",
			defaultUserfileURLSchemeAndHost.String() + "/file/unicode/á.csv",
			fileNames,
			unicodeFile,
			append(dataLetterFiles, dataNumberFiles...),
		},
		{
			"match-data-num-csv",
			"file/numbers/data[0-9].csv",
			fileNames,
			dataNumberFiles,
			append(dataLetterFiles, unicodeFile...),
		},
		{
			"wildcard-bucket-and-filename",
			"*/numbers/*.csv",
			fileNames,
			dataNumberFiles,
			append(dataLetterFiles, unicodeFile...),
		},
		{
			"match-all-csv-skip-dir",
			// filepath.Glob() assumes that / is the separator, and enforces that it's there.
			// So this pattern would not actually match anything.
			"file/*.csv",
			fileNames,
			nil,
			fileNames,
		},
		{
			"match-no-matches",
			"file/letters/dataD.csv",
			fileNames,
			nil,
			fileNames,
		},
		{
			"match-escaped-star",
			"file/*/\\*.csv",
			fileNames,
			nil,
			fileNames,
		},
		{
			"match-escaped-range",
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

			t.Run("list", func(t *testing.T) {
				checkListedFiles(t, c, "", "", tc.writeList)
				checkListedFiles(t, c, tc.URI, "", tc.expectedMatches)
			})

			if tc.URI != "" {
				t.Run("get", func(t *testing.T) {
					dest := filepath.Join(dir, fmt.Sprintf("tc-%d", tcNum))
					cmd := []string{"userfile", "get", tc.URI, dest}
					cliOutput, err := c.RunWithCaptureArgs(cmd)
					require.NoError(t, err)
					if strings.Contains(cliOutput, "ERROR: no files matched requested path or path pattern") {
						if len(tc.expectedMatches) > 0 {
							t.Fatalf("unexpected error: %q", cliOutput)
						}
					} else {
						lines := strings.Split(strings.TrimSpace(cliOutput), "\n")

						var downloaded []string
						for i := range lines {
							downloaded = append(downloaded, strings.Fields(lines[i])[3])
						}
						require.Equal(t, tc.expectedMatches, downloaded, "get files from %v returned %q", cmd, cliOutput)
					}
				})
			}

			t.Run("delete", func(t *testing.T) {
				checkDeletedFiles(t, c, tc.URI, "", tc.expectedMatches)
				// List files after deletion.
				checkListedFiles(t, c, "", "", tc.postDeleteList)
			})

			// Cleanup all files for next test run.
			_, err = c.RunWithCaptureArgs([]string{"userfile", "delete", "/"})
			require.NoError(t, err)
		})
	}

	require.NoError(t, os.RemoveAll(dir))
}

func TestUsernameUserfileInteraction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	c := NewCLITest(TestCLIParams{T: t})
	c.omitArgs = true
	defer c.Cleanup()

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

			checkListedFiles(t, c, "", fmt.Sprintf("--url=%s", userURL.String()), []string{tc.name})

			checkDeletedFiles(t, c, "", fmt.Sprintf("--url=%s", userURL.String()),
				[]string{tc.name})
		}
	})
}
