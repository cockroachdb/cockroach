// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/ioctx"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const defaultQualifiedDBSchemaName = "defaultdb.public."
const filename = "/test/test_file_upload.csv"

var fileUploadModes = []string{sql.NodelocalFileUploadTable, sql.UserFileUploadTable}

func writeFile(t *testing.T, testSendFile string, fileContent []byte) {
	err := os.MkdirAll(filepath.Dir(testSendFile), 0755)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(testSendFile, fileContent, 0644)
	if err != nil {
		t.Fatal(err)
	}
}

func prepareFileUploadURI(
	user username.SQLUsername, testSendFile, copyInternalTable string,
) (string, error) {
	var uri string
	switch copyInternalTable {
	case sql.NodelocalFileUploadTable:
		testSendFile = strings.TrimPrefix(testSendFile, "/")
		uri = fmt.Sprintf("nodelocal://1/%s", testSendFile)
	case sql.UserFileUploadTable:
		if !strings.HasPrefix(testSendFile, "/") {
			return "", errors.New("userfile destination must start with a /")
		}
		uri = fmt.Sprintf("userfile://%s%s",
			// TODO(knz): This is suspicious; see
			// https://github.com/cockroachdb/cockroach/issues/55389
			defaultQualifiedDBSchemaName+user.Normalized(),
			testSendFile)
	default:
		return "", errors.New("unsupported upload destination")
	}

	return uri, nil
}

func runCopyFile(
	t *testing.T, db *gosql.DB, user username.SQLUsername, testSendFile, copyInternalTable string,
) error {
	// Make sure we can open this file first
	reader, err := os.Open(testSendFile)
	if err != nil {
		return err
	}

	txn, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if newErr := txn.Commit(); err == nil && newErr != nil {
			t.Fatal(newErr)
		}
	}()

	fileUploadURI, err := prepareFileUploadURI(user, testSendFile, copyInternalTable)
	if err != nil {
		return err
	}
	stmt, err := txn.Prepare(sql.CopyInFileStmt(fileUploadURI, sql.CrdbInternalName, copyInternalTable))
	if err != nil {
		return err
	}

	for {
		send := make([]byte, 1024)
		n, err := reader.Read(send)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		_, err = stmt.Exec(string(send[:n]))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = stmt.Close()
	if err != nil {
		t.Fatal(err)
	}
	return nil
}

func checkNodelocalContent(
	t *testing.T, localExternalDir, filename string, expectedContent []byte,
) {
	content, err := os.ReadFile(filepath.Join(localExternalDir, filename))
	require.NoError(t, err)
	if !bytes.Equal(expectedContent, content) {
		t.Fatalf("content not the same. expected: %s got: %s", expectedContent, content)
	}
}

func checkUserFileContent(
	ctx context.Context,
	t *testing.T,
	s serverutils.ApplicationLayerInterface,
	user username.SQLUsername,
	filename string,
	expectedContent []byte,
) {
	uri, err := prepareFileUploadURI(user, filename, sql.UserFileUploadTable)
	require.NoError(t, err)
	store, err := s.ExecutorConfig().(sql.ExecutorConfig).DistSQLSrv.ExternalStorageFromURI(ctx, uri,
		user)
	require.NoError(t, err)
	reader, _, err := store.ReadFile(ctx, "", cloud.ReadOptions{NoFileSize: true})
	require.NoError(t, err)
	got, err := ioctx.ReadAll(ctx, reader)
	require.NoError(t, err)
	require.True(t, bytes.Equal(got, expectedContent))
}

func TestFileUpload(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	localExternalDir, cleanup := testutils.TempDir(t)
	defer cleanup()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: localExternalDir,
	})
	defer s.Stopper().Stop(context.Background())
	tt := s.ApplicationLayer()

	testFileDir, cleanup2 := testutils.TempDir(t)
	defer cleanup2()
	testSendFile := filepath.Join(testFileDir, filename)
	fileContent := []byte("hello \n blah 1@#% some data hello \n @#%^&&*")
	writeFile(t, testSendFile, fileContent)

	for _, table := range fileUploadModes {
		err := runCopyFile(t, db, username.RootUserName(), testSendFile, table)
		require.NoError(t, err)
	}

	// Verify contents of the uploaded file.
	checkNodelocalContent(t, localExternalDir, testSendFile, fileContent)
	checkUserFileContent(ctx, t, tt, username.RootUserName(), testSendFile, fileContent)
}

func TestUploadEmptyFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	localExternalDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: localExternalDir,
	})
	defer s.Stopper().Stop(context.Background())
	tt := s.ApplicationLayer()

	testFileDir, cleanup2 := testutils.TempDir(t)
	defer cleanup2()
	testSendFile := filepath.Join(testFileDir, filename)
	fileContent := []byte("")
	writeFile(t, testSendFile, fileContent)

	for _, table := range fileUploadModes {
		err := runCopyFile(t, db, username.RootUserName(), testSendFile, table)
		require.NoError(t, err)
	}

	// Verify contents of the uploaded file.
	checkNodelocalContent(t, localExternalDir, testSendFile, fileContent)
	checkUserFileContent(ctx, t, tt, username.RootUserName(), testSendFile, fileContent)
}

func TestFileNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	localExternalDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: localExternalDir,
	})
	defer s.Stopper().Stop(context.Background())

	expectedErr := "no such file"
	for _, table := range fileUploadModes {
		err := runCopyFile(t, db, username.RootUserName(), filename, table)
		require.True(t, testutils.IsError(err, expectedErr))
	}
}

func TestFileExist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	localExternalDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: localExternalDir,
	})
	defer s.Stopper().Stop(context.Background())

	testFileDir, cleanup2 := testutils.TempDir(t)
	defer cleanup2()
	testSendFile := filepath.Join(testFileDir, filename)
	writeFile(t, testSendFile, []byte("file exists"))

	// Write successfully the first time.
	for _, table := range fileUploadModes {
		err := runCopyFile(t, db, username.RootUserName(), testSendFile, table)
		require.NoError(t, err)
	}

	// Writes fail the second time.
	for _, table := range fileUploadModes {
		require.True(t, testutils.IsError(runCopyFile(t, db, username.RootUserName(), testSendFile,
			table), "file already exists"))
	}
}

// TestNodelocalNotAdmin ensures that non-admin users cannot interact with
// nodelocal storage.
func TestNodelocalNotAdmin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	localExternalDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	s, rootDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: localExternalDir,
		Insecure:      true,
	})
	defer s.Stopper().Stop(context.Background())

	const smithUser = "jsmith"
	smithUserName := username.MakeSQLUsernameFromPreNormalizedString(smithUser)

	_, err := rootDB.Exec("CREATE USER " + smithUser)
	require.NoError(t, err)

	userDB, err := s.ApplicationLayer().SQLConnE(serverutils.User(smithUser), serverutils.CertsDirPrefix("notAdmin"), serverutils.ClientCerts(false))
	require.NoError(t, err)
	defer userDB.Close()

	testFileDir, cleanup2 := testutils.TempDir(t)
	defer cleanup2()
	testSendFile := filepath.Join(testFileDir, filename)
	fileContent := []byte("hello \n blah 1@#% some data hello \n @#%^&&*")
	writeFile(t, testSendFile, fileContent)

	err = runCopyFile(t, userDB, smithUserName, testSendFile, sql.NodelocalFileUploadTable)
	expectedErr := "only users with the admin role are allowed to upload to nodelocal"
	require.True(t, testutils.IsError(err, expectedErr))
}

// TestUserfileNotAdmin ensures that non-admin users with CREATE privileges can
// interact with the FileTable ExternalStorage.
func TestUserfileNotAdmin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	localExternalDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	s, rootDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: localExternalDir,
		Insecure:      true,
	})
	defer s.Stopper().Stop(context.Background())
	tt := s.ApplicationLayer()

	const smithUser = "jsmith"
	smithUserName := username.MakeSQLUsernameFromPreNormalizedString(smithUser)

	_, err := rootDB.Exec("CREATE USER " + smithUser)
	require.NoError(t, err)
	_, err = rootDB.Exec("GRANT CREATE ON DATABASE defaultdb TO " + smithUser)
	require.NoError(t, err)

	userDB, err := s.ApplicationLayer().SQLConnE(serverutils.User(smithUser), serverutils.CertsDirPrefix("notAdmin"), serverutils.ClientCerts(false))
	require.NoError(t, err)
	defer userDB.Close()

	testFileDir, cleanup2 := testutils.TempDir(t)
	defer cleanup2()
	testSendFile := filepath.Join(testFileDir, filename)
	fileContent := []byte("hello \n blah 1@#% some data hello \n @#%^&&*")
	writeFile(t, testSendFile, fileContent)

	err = runCopyFile(t, userDB, smithUserName, testSendFile, sql.UserFileUploadTable)
	require.NoError(t, err)
	checkUserFileContent(context.Background(), t, tt, smithUserName, testSendFile, fileContent)
}
