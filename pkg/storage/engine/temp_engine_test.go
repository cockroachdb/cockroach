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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"golang.org/x/net/context"
)

// TestCleanupTempStoreDirs checks that cleanupTempStoreDirs will remove all
// temporary directories logged in TempStoreDirsLogFilename in a node's first
// store.
func TestCleanupTempStoreDirs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	tempDir, tempDirCleanup := tempStoreDir(t, dir, TempStorePrefix)
	defer tempDirCleanup()

	// These will be the abandoned temporary directories left over
	// from a previous node startup.
	abandonedDir1, abandonedDir1Cleanup := tempStoreDir(t, dir, TempStorePrefix)
	defer abandonedDir1Cleanup()
	abandonedDir2, abandonedDir2Cleanup := tempStoreDir(t, dir, TempStorePrefix)
	defer abandonedDir2Cleanup()

	tempBytes := []byte{byte(1), byte(2), byte(3)}
	if err := ioutil.WriteFile(filepath.Join(abandonedDir1, "FOO"), tempBytes, 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(filepath.Join(abandonedDir1, "BAR"), tempBytes, 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(filepath.Join(abandonedDir2, "BAZ"), tempBytes, 0777); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	if err := cleanupTempStoreDirs(context.TODO(), tempDir, &wg); err != nil {
		t.Fatal(fmt.Sprintf("error encountered in cleanupTempStorageDirs: %v", err))
	}
	wg.Wait()

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Fatal(fmt.Sprintf("error reading temporary directory: %v", err))
	}
	if len(files) != 0 {
		if files[0].Name() != filepath.Base(tempDir) {
			t.Fatalf("directory not cleaned up after calling cleanupTempStorageDirs, still have %d files", len(files))
		}
	}
}

func tempStoreDir(t *testing.T, path, prefix string) (string, func()) {
	tempDir, err := ioutil.TempDir(path, prefix)
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Fatal(err)
		}
	}
	return tempDir, cleanup
}
