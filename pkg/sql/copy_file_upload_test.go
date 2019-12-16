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
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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

func runCopyFile(t *testing.T, serverParams base.TestServerArgs, testSendFile string) error {
	// Make sure we can open this file first
	reader, err := os.Open(testSendFile)
	if err != nil {
		return err
	}

	s, db, _ := serverutils.StartServer(t, serverParams)
	defer s.Stopper().Stop(context.TODO())

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

	testFileDir, cleanup2 := testutils.TempDir(t)
	defer cleanup2()
	testSendFile := filepath.Join(testFileDir, filename)
	fileContent := []byte("hello \n blah 1@#% some data hello \n @#%^&&*")
	writeFile(t, testSendFile, fileContent)

	err := runCopyFile(t, params, testSendFile)
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

	testFileDir, cleanup2 := testutils.TempDir(t)
	defer cleanup2()
	testSendFile := filepath.Join(testFileDir, filename)
	fileContent := []byte("")
	writeFile(t, testSendFile, fileContent)

	err := runCopyFile(t, params, testSendFile)
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

	err := runCopyFile(t, params, filepath.Join(localExternalDir, filename))
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
	destination := filepath.Join(localExternalDir, filename)
	writeFile(t, destination, []byte("file exists"))

	err := runCopyFile(t, params, destination)
	expectedErr := "file already exists"
	if !testutils.IsError(err, expectedErr) {
		t.Fatalf(`expected error: %s, got: %s`, expectedErr, err)
	}
}
