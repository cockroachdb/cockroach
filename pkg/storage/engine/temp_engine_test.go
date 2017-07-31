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
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"golang.org/x/net/context"
)

func TestCleanupTempStorageDirs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	tempBytes := []byte{byte(1), byte(2), byte(3)}
	if err := ioutil.WriteFile(filepath.Join(dir, "FOO"), tempBytes, 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(filepath.Join(dir, "BAR"), tempBytes, 0777); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(filepath.Join(dir, "BAZ"), tempBytes, 0777); err != nil {
		t.Fatal(err)
	}

	wg := sync.WaitGroup{}
	if err := cleanupTempStorageDirs(context.TODO(), dir, &wg); err != nil {
		t.Fatal(fmt.Sprintf("error encountered in cleanupTempStorageDirs: %v", err))
	}
	wg.Wait()

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		t.Fatal(fmt.Sprintf("error reading temporary directory: %v", err))
	}
	if len(files) != 0 {
		t.Fatalf("directory not cleaned up after calling cleanupTempStorageDirs, still have %d files", len(files))
	}
}
