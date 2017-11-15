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
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
	if ver.Version != versionNoFile {
		t.Errorf("no version file version should be %d, got %+v", versionNoFile, ver)
	}

	// Write the same version to the file: not allowed.
	if err := writeVersionFile(dir, ver); !testutils.IsError(err, "writing version 0 is not allowed") {
		t.Errorf("expected error '%s', got '%v'", "writing version 0 is not allowed")
	}

	// Try again with current version.
	ver.Version = versionCurrent
	if err := writeVersionFile(dir, ver); err != nil {
		t.Fatal(err)
	}

	ver, err = getVersion(dir)
	if err != nil {
		t.Fatal(err)
	}
	if ver.Version != versionCurrent {
		t.Errorf("current versions do not match, expected %d got %+v", versionCurrent, ver)
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
		t.Errorf("expected error contains '%s', got '%v'", "is not formatted correctly", err)
	}
}

// TestOldVersionFormat verifies that new version format (not numbers, but actual json fields)
// parses properly when using the older version data structures.
func TestOldVersionFormat(t *testing.T) {
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

	newVersion := Version{Version: versionPreambleFormat, Format: formatPreamble}
	if err := writeVersionFile(dir, newVersion); err != nil {
		t.Fatal(err)
	}

	// We can't use the getVersion function as we need to use a custom data structure to parse into.
	b, err := ioutil.ReadFile(getVersionFilename(dir))
	if err != nil {
		t.Fatal(err)
	}

	// This is the version data structure in use before versionPreambleFormat was introduced.
	var oldFormatVersion struct {
		Version storageVersion
	}

	if err := json.Unmarshal(b, &oldFormatVersion); err != nil {
		t.Errorf("could not parse new version file using old version format: %v", err)
	}

	if oldFormatVersion.Version != newVersion.Version {
		t.Errorf("mismatched version number: got %d, expected %d", oldFormatVersion.Version, newVersion.Version)
	}

	// Now write a version using the old format.
	oldFormatVersion.Version = versionBeta20160331
	b, err = json.Marshal(oldFormatVersion)
	if err != nil {
		t.Fatal(err)
	}
	if err := ioutil.WriteFile(getVersionFilename(dir), b, 0644); err != nil {
		t.Fatal(err)
	}

	// Read it back using the current data structure.
	newVersion, err = getVersion(dir)
	if err != nil {
		t.Fatal(err)
	}

	if newVersion.Version != oldFormatVersion.Version {
		t.Errorf("mismatched version number: got %d, expected %d", newVersion.Version, oldFormatVersion.Version)
	}
	if newVersion.Format != formatClassic {
		t.Errorf("mismatched format number: got %d, expected %d", newVersion.Version, formatClassic)
	}
}
