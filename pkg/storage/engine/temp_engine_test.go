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
	abandonedTemp, _ := tempStoreDir(t, abandonedDir)

	// Add the abandoned temporary directory to the record file for
	// cleanup.
	if err := appendTempStoreDir(dir, abandonedTemp); err != nil {
		t.Fatal(err)
	}

	// Create a separate directory to house the current temp store. THis
	// allows to test if cleanup works across different subtrees.
	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()
	// This will be cleaned up with tempDirCleanup.
	curTemp, _ := tempStoreDir(t, tempDir)

	// Initialize wait group to proceed after cleanup process has finished.
	wg := sync.WaitGroup{}
	engine, err := NewTempEngine(context.TODO(), base.StoreSpec{Path: dir}, base.StoreSpec{Path: curTemp}, &wg)
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

	// 3. A new temporary directory is initialized for the temp store.
	if curTemp != tempEngine.RocksDB.cfg.Dir {
		t.Fatalf("rocksdb instance initialized with different temporary directory. expected %s\nactual %s", curTemp, tempEngine.RocksDB.cfg.Dir)
	}
	if _, err = os.Stat(curTemp); err != nil {
		if os.IsNotExist(err) {
			t.Fatalf("failed to initialize temporary directory for temp store")
		} else {
			// Unexpected error.
			t.Fatal(err)
		}
	}

	// 4. Record file contains only the current temporary directory's path.
	expected := append([]byte(curTemp), '\n')
	actual, err := ioutil.ReadFile(tempStoreDirsRecordFilepath(dir))
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

	// Will be cleaned up with dirCleanup.
	tempDir, _ := tempStoreDir(t, dir)

	engine, err := NewTempEngine(context.TODO(), base.StoreSpec{Path: dir}, base.StoreSpec{Path: tempDir}, nil)
	if err != nil {
		t.Fatalf("error encountered when invoking NewTempEngine: %v", err)
	}

	// Close the engine to cleanup temporary directory.
	engine.Close()

	// Check that the temp store directory is removed.
	_, err = os.Stat(tempDir)
	// os.Stat returns a nil err if the file exists, so we need to check
	// the NOT of this condition instead of os.IsExist() (which returns
	// false and misses this error).
	if !os.IsNotExist(err) {
		if err == nil {
			t.Fatalf("temp store directory %s was not cleaned up during Close", tempDir)
		} else {
			// Unexpected error.
			t.Fatal(err)
		}
	}

	records, err := ioutil.ReadFile(tempStoreDirsRecordFilepath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) > 0 {
		t.Fatalf("temporary directory record was not remove after engine close, actual %s", records)
	}
}

// TestRemoveTempStoreDirRecord verifies that removeTestStoreDirRecord properly
// seeks and removes the specified temporary path record from the record
// file.
func TestRemoveTempStoreDirRecord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	content := []byte("dir1\ndir2\ndir3\n")
	if err := appendTempStoreDirs(dir, content); err != nil {
		t.Fatal(err)
	}

	if err := removeTempStoreDirRecord(dir, "dir2"); err != nil {
		t.Fatalf("error encountered when invoking removeTempStoreDirRecord: %v", err)
	}

	expected := []byte("dir1\ndir3\n")
	actual, err := ioutil.ReadFile(tempStoreDirsRecordFilepath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, actual) {
		t.Fatalf("record not properly removed from record file.\nexpected: %v\nactual %v", expected, actual)
	}
}

// TestAppendTempStoreDir verifies that appendTempStoreDir properly appends
// the temporary path string to the record file.
func TestAppendTempStoreDir(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	tempPathRecord := "dir1"
	if err := appendTempStoreDir(dir, tempPathRecord); err != nil {
		t.Fatalf("appendTempStoreDirs could not create new record file to append: %v", err)
	}

	expected := append([]byte(tempPathRecord), '\n')
	actual, err := ioutil.ReadFile(tempStoreDirsRecordFilepath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(expected, actual) {
		t.Fatalf("record file not properly appended to.\nexpected: %s\nactual %s", expected, actual)
	}
}

// TestAppendTempStoreDirsNewFile verifies that appendTempStoreDirs may
// write the invoked byte slice of temporary paths to a non-existing
// record file.
func TestAppendTempStoreDirsNewFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	content := []byte("dir1\ndir2\n")
	if err := appendTempStoreDirs(dir, content); err != nil {
		t.Fatalf("appendTempStoreDirs could not create new record file to append: %v", err)
	}

	actual, err := ioutil.ReadFile(tempStoreDirsRecordFilepath(dir))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(content, actual) {
		t.Fatalf("record file not properly appended to.\nexpected: %s\nactual %s", content, actual)
	}
}

// TestAppendTempStoreDirs verifies that appendTempStoreDirs properly appends
// the invoked byte slice of temporary paths to an existing record file with
// existing records.
func TestAppendTempStoreDirs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	// Create an existing record file.
	curContent := []byte("dir1\ndir2\n")
	if err := ioutil.WriteFile(tempStoreDirsRecordFilepath(dir), curContent, 0644); err != nil {
		t.Fatal(err)
	}

	// Attempt to append to the record file.
	newContent := []byte("dir3\ndir4\n")
	if err := appendTempStoreDirs(dir, newContent); err != nil {
		t.Fatal(err)
	}

	expected := append(curContent, newContent...)
	actual, err := ioutil.ReadFile(tempStoreDirsRecordFilepath(dir))
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(expected, actual) {
		t.Fatalf("record file not properly appended to.\nexpected: %s\nactual %s", expected, actual)
	}
}

// TestCleanupTempStoreDirsNoRecord verifies that cleanupTempStoreDirs does not
// error if there is no record file available.
func TestCleanupTempStoreDirsNoRecord(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	if err := cleanupTempStoreDirs(context.TODO(), dir, nil); err != nil {
		t.Fatalf("error encountered when cleanupTempStoreDirs encountered a non-existing record file: %s", err.Error())
	}
}

// TestCleanupTempStoreDirsNoTempDir verifies that cleanupTempStoreDirs does not
// error if the temporary directory in the record file is missing.
func TestCleanupTempStoreDirsNoTempDir(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	// Create a temporary directory and add it to the record file.
	curTemp, curTempCleanup := tempStoreDir(t, dir)
	if err := appendTempStoreDir(dir, curTemp); err != nil {
		t.Fatal(err)
	}

	// Remove the temporary directory before calling cleanup.
	curTempCleanup()

	if err := cleanupTempStoreDirs(context.TODO(), dir, nil); err != nil {
		t.Fatalf("error encountered when cleanupTempStoreDirs encountered a missing temporary directory: %s", err.Error())
	}
}

// TestCleanupTempStoreDirs verifies that cleanupTempStoreDirs will remove all
// temporary directories recorded in TempStoreDirsRecordFilename and wipe the
// record file clean.
func TestCleanupTempStoreDirs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	// These will be the abandoned temporary directories left over
	// from a previous node startup.
	abandonedTemp1, abandonedTemp1Cleanup := tempStoreDir(t, dir)
	defer abandonedTemp1Cleanup()
	abandonedTemp2, abandonedTemp2Cleanup := tempStoreDir(t, dir)
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
	if err := appendTempStoreDir(dir, abandonedTemp1); err != nil {
		t.Fatal(err)
	}
	if err := appendTempStoreDir(dir, abandonedTemp2); err != nil {
		t.Fatal(err)
	}

	// This is our "current" temporary directory that will not be in the
	// record file and thus should  not be removed.
	curTemp, curTempCleanup := tempStoreDir(t, dir)
	defer curTempCleanup()

	wg := sync.WaitGroup{}
	if err := cleanupTempStoreDirs(context.TODO(), dir, &wg); err != nil {
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
		t.Fatalf("after calling cleanUpTempStoreDirs expected exactly %d files, have %d files", expectedN, len(files))
	}
	for _, file := range files {
		if file.Name() != filepath.Base(curTemp) && file.Name() != TempStoreDirsRecordFilename {
			t.Fatalf("expected either %s or %s, actual: %s", curTemp, TempStoreDirsRecordFilename, file.Name())
		}
	}

	// Ensure that the record file has been cleared.
	records, err := ioutil.ReadFile(tempStoreDirsRecordFilepath(dir))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) > 0 {
		t.Fatalf("expected record file to be empty, actual: %s", records)
	}
}

func tempStoreDir(t *testing.T, dir string) (string, func()) {
	tempStorePath, err := ioutil.TempDir(dir, "temp-engine-store")
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		if err := os.RemoveAll(tempStorePath); err != nil {
			t.Fatal(err)
		}
	}
	return tempStorePath, cleanup
}
