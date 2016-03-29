// Copyright 2016 The Cockroach Authors.
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
	"io/ioutil"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestVersions verifies that both getVersions() and writeVersionFile work
// correctly.
func TestVersions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, err := ioutil.TempDir("", "testing")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			t.Fatal(err)
		}
	}()

	// First test when no file exists yet.
	ver, err := getVersion(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ver != versionNoFile {
		t.Errorf("no version file version should be %d, got %d", versionNoFile, ver)
	}

	// Write the current versions to the file.
	if err := writeVersionFile(dir); err != nil {
		t.Fatal(err)
	}
	ver, err = getVersion(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ver != versionCurrent {
		t.Errorf("current versions do not match, expected %d got %d", versionCurrent, ver)
	}

	// Write gibberish to the file.
	filename := getVersionFilename(dir)
	if err := os.Remove(filename); err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(filename, []byte("cause an error please"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := getVersion(dir); !testutils.IsError(err, "is not formatted correctly") {
		t.Errorf("expected error contains '%s', got '%s'", "is not formatted correctly", err)
	}
}
