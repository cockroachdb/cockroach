// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package engine

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"golang.org/x/net/context"
)

// TestNewTempEngine verifies that during the initialization of a new TempEngine:
// 1. any abandoned temporary directories are cleaned up
// 2. a RocksDB instance is initialized and returned
// 3. a new temporary directory is initialized
// 4. the record file contains only the new temporary directory
func TestNewTempEngine(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	// Create a separate directory to house the abandoned temp store.
	abandonedDir, abandonedDirCleanup := testutils.TempDir(t)
	defer abandonedDirCleanup()
	// This will be cleaned up with abandonedTempCleanup.
	abandonedTemp, _ := tempStorageDir(t, abandonedDir)

	// Add the abandoned temporary directory to the record file for
	// cleanup.
	if err := appendTempStorageDir(dir, abandonedTemp); err != nil {
		t.Fatal(err)
	}

	// Create a separate directory to house the current temp store. THis
	// allows to test if cleanup works across different subtrees.
	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()

	// Initialize wait group to proceed after cleanup process has finished.
	wg := sync.WaitGroup{}
	engine, err := NewTempEngine(context.TODO(), base.TempStorage{ParentDir: tempDir, RecordPath: dir}, &wg)
	if err != nil {
		t.Fatalf("error encountered when invoking NewTempEngine: %v", err)
	}
	defer engine.Close()
	wg.Wait()

	// 1. Check the abandoned temp store is removed.
	_, err = os.Stat(abandonedTemp)
	// os.Stat returns a nil err if the file exists, so we need to check
	// the NOT of this condition instead of os.IsExist() (which returns
	// false and misses this error).
	if !os.IsNotExist(err) {
		if err == nil {

			t.Fatalf("abandoned temp store %s was not cleaned up during NewTempEngine", abandonedTemp)
		} else {
			// Unexpected error.
			t.Fatal(err)
		}
	}

	tempEngine := engine.(*tempEngine)
	// 2. RocksDB instance returned.
	if tempEngine.RocksDB == nil {
		t.Fatalf("a rocksdb instance was not returned in NewTempEngine")
	}

	// 3. A new temporary directory is initialized for the temp storage
	// which is nested under the given parent directory.
	if tempDir != filepath.Dir(tempEngine.RocksDB.cfg.Dir) {
		t.Fatalf("rocksdb instance initialized with unexpected parent directory.\nexpected %s\nactual %s", tempDir, filepath.Dir(tempEngine.RocksDB.cfg.Dir))
	}
	if _, err = os.Stat(tempEngine.RocksDB.cfg.Dir); err != nil {
		if os.IsNotExist(err) {
			t.Fatalf("failed to initialize temporary directory for temp store")
		} else {
			// Unexpected error.
			t.Fatal(err)
		}
	}

	// 4. Record file contains only the current temporary directory's path.
	expected := append([]byte(tempEngine.RocksDB.cfg.Dir), '\n')
	actual, err := ioutil.ReadFile(tempStorageDirsRecordFilepath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, actual) {
		t.Fatalf("record file contains incorrect records.\nexpected: %s\nactual: %s", expected, actual)
	}
}

// TestTempEngineClose verifies that Close indeeds removes the associated
// temporary directory for the engine and its entry in the record file.
func TestTempEngineClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	engine, err := NewTempEngine(context.TODO(), base.TempStorage{ParentDir: dir, RecordPath: dir}, nil)
	if err != nil {
		t.Fatalf("error encountered when invoking NewTempEngine: %v", err)
	}

	// Close the engine to cleanup temporary directory.
	engine.Close()

	tempEngine := engine.(*tempEngine)

	// Check that the temp store directory is removed.
	_, err = os.Stat(tempEngine.RocksDB.cfg.Dir)
	// os.Stat returns a nil err if the file exists, so we need to check
	// the NOT of this condition instead of os.IsExist() (which returns
	// false and misses this error).
	if !os.IsNotExist(err) {
		if err == nil {
			t.Fatalf("temp store directory %s was not cleaned up during Close", tempEngine.RocksDB.cfg.Dir)
		} else {
			// Unexpected error.
			t.Fatal(err)
		}
	}

	records, err := ioutil.ReadFile(tempStorageDirsRecordFilepath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) > 0 {
		t.Fatalf("temporary directory record was not remove after engine close, actual %s", records)
	}
}

// TestRemoveTempStorageDirRecord verifies that removeTestStoreDirRecord properly
// seeks and removes the specified temporary path record from the record
// file.
func TestRemoveTempStorageDirRecord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	content := []byte("dir1\ndir2\ndir3\n")
	if err := appendTempStorageDirs(dir, content); err != nil {
		t.Fatal(err)
	}

	if err := removeTempStorageDirRecord(dir, "dir2"); err != nil {
		t.Fatalf("error encountered when invoking removeTempStorageDirRecord: %v", err)
	}

	expected := []byte("dir1\ndir3\n")
	actual, err := ioutil.ReadFile(tempStorageDirsRecordFilepath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, actual) {
		t.Fatalf("record not properly removed from record file.\nexpected: %v\nactual %v", expected, actual)
	}
}

// TestAppendTempStorageDir verifies that appendTempStorageDir properly appends
// the temporary path string to the record file.
func TestAppendTempStorageDir(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	tempPathRecord := "dir1"
	if err := appendTempStorageDir(dir, tempPathRecord); err != nil {
		t.Fatalf("appendTempStorageDirs could not create new record file to append: %v", err)
	}

	expected := append([]byte(tempPathRecord), '\n')
	actual, err := ioutil.ReadFile(tempStorageDirsRecordFilepath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, actual) {
		t.Fatalf("record file not properly appended to.\nexpected: %s\nactual %s", expected, actual)
	}
}

// TestAppendTempStorageDirsNewFile verifies that appendTempStorageDirs may
// write the invoked byte slice of temporary paths to a non-existing
// record file.
func TestAppendTempStorageDirsNewFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	content := []byte("dir1\ndir2\n")
	if err := appendTempStorageDirs(dir, content); err != nil {
		t.Fatalf("appendTempStorageDirs could not create new record file to append: %v", err)
	}

	actual, err := ioutil.ReadFile(tempStorageDirsRecordFilepath(dir))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(content, actual) {
		t.Fatalf("record file not properly appended to.\nexpected: %s\nactual %s", content, actual)
	}
}

// TestAppendTempStorageDirs verifies that appendTempStorageDirs properly appends
// the invoked byte slice of temporary paths to an existing record file with
// existing records.
func TestAppendTempStorageDirs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	// Create an existing record file.
	curContent := []byte("dir1\ndir2\n")
	if err := ioutil.WriteFile(tempStorageDirsRecordFilepath(dir), curContent, 0644); err != nil {
		t.Fatal(err)
	}

	// Attempt to append to the record file.
	newContent := []byte("dir3\ndir4\n")
	if err := appendTempStorageDirs(dir, newContent); err != nil {
		t.Fatal(err)
	}

	expected := append(curContent, newContent...)
	actual, err := ioutil.ReadFile(tempStorageDirsRecordFilepath(dir))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(expected, actual) {
		t.Fatalf("record file not properly appended to.\nexpected: %s\nactual %s", expected, actual)
	}
}

// TestCleanupTempStorageDirsNoRecord verifies that cleanupTempStorageDirs does not
// error if there is no record file available.
func TestCleanupTempStorageDirsNoRecord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	if err := cleanupTempStorageDirs(context.TODO(), dir, nil); err != nil {
		t.Fatalf("error encountered when cleanupTempStorageDirs encountered a non-existing record file: %s", err.Error())
	}
}

// TestCleanupTempStorageDirsNoTempDir verifies that cleanupTempStorageDirs does not
// error if the temporary directory in the record file is missing.
func TestCleanupTempStorageDirsNoTempDir(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	// Create a temporary directory and add it to the record file.
	curTemp, curTempCleanup := tempStorageDir(t, dir)
	if err := appendTempStorageDir(dir, curTemp); err != nil {
		t.Fatal(err)
	}

	// Remove the temporary directory before calling cleanup.
	curTempCleanup()

	if err := cleanupTempStorageDirs(context.TODO(), dir, nil); err != nil {
		t.Fatalf("error encountered when cleanupTempStorageDirs encountered a missing temporary directory: %s", err.Error())
	}
}

// TestCleanupTempStorageDirs verifies that cleanupTempStorageDirs will remove all
// temporary directories recorded in TempStorageDirsRecordFilename and wipe the
// record file clean.
func TestCleanupTempStorageDirs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	// These will be the abandoned temporary directories left over
	// from a previous node startup.
	abandonedTemp1, abandonedTemp1Cleanup := tempStorageDir(t, dir)
	defer abandonedTemp1Cleanup()
	abandonedTemp2, abandonedTemp2Cleanup := tempStorageDir(t, dir)
	defer abandonedTemp2Cleanup()
	tempBytes := []byte{byte(1), byte(2), byte(3)}
	if err := ioutil.WriteFile(filepath.Join(abandonedTemp1, "FOO"), tempBytes, 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(filepath.Join(abandonedTemp1, "BAR"), tempBytes, 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(filepath.Join(abandonedTemp2, "BAZ"), tempBytes, 0777); err != nil {
		t.Fatal(err)
	}

	// Add our abandoned temporary directories to the record file for
	// cleanup.
	if err := appendTempStorageDir(dir, abandonedTemp1); err != nil {
		t.Fatal(err)
	}
	if err := appendTempStorageDir(dir, abandonedTemp2); err != nil {
		t.Fatal(err)
	}

	// This is our "current" temporary directory that will not be in the
	// record file and thus should  not be removed.
	curTemp, curTempCleanup := tempStorageDir(t, dir)
	defer curTempCleanup()

	wg := sync.WaitGroup{}
	if err := cleanupTempStorageDirs(context.TODO(), dir, &wg); err != nil {
		t.Fatalf("error encountered in cleanupTempStorageDirs: %v", err)
	}
	wg.Wait()

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Fatal(fmt.Sprintf("error reading entries in base directory: %v", err))
	}

	// The record file and the current temporary directory should still
	// remain.
	expectedN := 2
	if len(files) != expectedN {
		t.Fatalf("after calling cleanUpTempStorageDirs expected exactly %d files, have %d files", expectedN, len(files))
	}
	for _, file := range files {
		if file.Name() != filepath.Base(curTemp) && file.Name() != TempStorageDirsRecordFilename {
			t.Fatalf("expected either %s or %s, actual: %s", curTemp, TempStorageDirsRecordFilename, file.Name())
		}
	}

	// Ensure that the record file has been cleared.
	records, err := ioutil.ReadFile(tempStorageDirsRecordFilepath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) > 0 {
		t.Fatalf("expected record file to be empty, actual: %s", records)
	}
}

func tempStorageDir(t *testing.T, dir string) (string, func()) {
	tempStoragePath, err := ioutil.TempDir(dir, "temp-engine-storage")
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		if err := os.RemoveAll(tempStoragePath); err != nil {
			t.Fatal(err)
		}
	}
	return tempStoragePath, cleanup
}
