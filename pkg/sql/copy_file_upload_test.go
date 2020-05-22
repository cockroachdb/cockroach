// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	gosql "database/sql"
	"io"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

const filename = "/test/test_file_upload.csv"

func writeFile(t *testing.T, testSendFile string, fileContent []byte) {
	err := os.MkdirAll(filepath.Dir(testSendFile), 0755)
	if err != nil {
		t.Fatal(err)
	}
	err = ioutil.WriteFile(testSendFile, fileContent, 0644)
	if err != nil {
		t.Fatal(err)
	}
}

func runCopyFile(t *testing.T, db *gosql.DB, testSendFile string) error {
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

	stmt, err := txn.Prepare(CopyInFileStmt(filename, crdbInternalName, fileUploadTable))
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

func TestFileUpload(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	localExternalDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	params.ExternalIODir = localExternalDir

	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	testFileDir, cleanup2 := testutils.TempDir(t)
	defer cleanup2()
	testSendFile := filepath.Join(testFileDir, filename)
	fileContent := []byte("hello \n blah 1@#% some data hello \n @#%^&&*")
	writeFile(t, testSendFile, fileContent)

	err := runCopyFile(t, db, testSendFile)
	if err != nil {
		t.Fatal(err)
	}

	content, err := ioutil.ReadFile(filepath.Join(localExternalDir, filename))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fileContent, content) {
		t.Fatalf("content not the same. expected: %s got: %s", fileContent, content)
	}
}

func TestUploadEmptyFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	localExternalDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	params.ExternalIODir = localExternalDir
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	testFileDir, cleanup2 := testutils.TempDir(t)
	defer cleanup2()
	testSendFile := filepath.Join(testFileDir, filename)
	fileContent := []byte("")
	writeFile(t, testSendFile, fileContent)

	err := runCopyFile(t, db, testSendFile)
	if err != nil {
		t.Fatal(err)
	}

	content, err := ioutil.ReadFile(filepath.Join(localExternalDir, filename))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(fileContent, content) {
		t.Fatalf("content not the same. expected: %s got: %s", fileContent, content)
	}
}

func TestFileNotExist(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	localExternalDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	params.ExternalIODir = localExternalDir
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	err := runCopyFile(t, db, filepath.Join(localExternalDir, filename))
	expectedErr := "no such file"
	if !testutils.IsError(err, expectedErr) {
		t.Fatalf(`expected error: %s, got: %s`, expectedErr, err)
	}
}

func TestFileExist(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	localExternalDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	params.ExternalIODir = localExternalDir
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	destination := filepath.Join(localExternalDir, filename)
	writeFile(t, destination, []byte("file exists"))

	err := runCopyFile(t, db, destination)
	expectedErr := "file already exists"
	if !testutils.IsError(err, expectedErr) {
		t.Fatalf(`expected error: %s, got: %s`, expectedErr, err)
	}
}

func TestNotAdmin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	localExternalDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	params.ExternalIODir = localExternalDir
	params.Insecure = true
	s, rootDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	_, err := rootDB.Exec("CREATE USER jsmith")
	require.NoError(t, err)

	pgURL, cleanupGoDB := sqlutils.PGUrlWithOptionalClientCerts(
		t, s.ServingSQLAddr(), "notAdmin", url.User("jsmith"), false, /* withCerts */
	)
	defer cleanupGoDB()
	pgURL.RawQuery = "sslmode=disable"
	userDB, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer userDB.Close()

	testFileDir, cleanup2 := testutils.TempDir(t)
	defer cleanup2()
	testSendFile := filepath.Join(testFileDir, filename)
	fileContent := []byte("hello \n blah 1@#% some data hello \n @#%^&&*")
	writeFile(t, testSendFile, fileContent)

	err = runCopyFile(t, userDB, testSendFile)
	expectedErr := "only users with the admin role are allowed to upload"
	if !testutils.IsError(err, expectedErr) {
		t.Fatalf(`expected error: %s, got: %s`, expectedErr, err)
	}
}
