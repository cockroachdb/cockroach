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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

type storageVersion int
type formatVersion int

const (
	versionNoFile storageVersion = iota
	versionBeta20160331
	versionPreambleFormat
)

const (
	// It's important to keep formatClassic at zero, meaning an omitted
	// json field means "classic".
	formatClassic formatVersion = iota
	formatPreamble
)

const (
	versionFilename     = "COCKROACHDB_VERSION"
	versionFilenameTemp = "COCKROACHDB_VERSION_TEMP"
	versionMinimum      = versionNoFile
	versionCurrent      = versionPreambleFormat
)

// Version stores all the version information for all stores and is used as
// the format for the version file.
type Version struct {
	Version storageVersion
	// Format is unknown prior to versionPreambleFormat and defaults to formatClassic.
	Format formatVersion
}

// getVersionFilename returns the filename for the version file stored in the
// data directory.
func getVersionFilename(dir string) string {
	return filepath.Join(dir, versionFilename)
}

// getVersion returns the current on disk cockroach version from the version
// file in the passed in directory. If there is no version file yet, it
// returns a version with a zero (NoFile) storage version.
func getVersion(dir string) (Version, error) {
	filename := getVersionFilename(dir)
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return Version{Version: versionNoFile}, nil
		}
		return Version{Version: versionNoFile}, err
	}
	var ver Version
	if err := json.Unmarshal(b, &ver); err != nil {
		return Version{Version: versionNoFile}, fmt.Errorf("version file %s is not formatted correctly; %s", filename, err)
	}
	return ver, nil
}

// writeVersionFile overwrites the version file to contain the latest version.
func writeVersionFile(dir string, ver Version) error {
	if ver.Version == versionNoFile {
		return fmt.Errorf("writing version %d is not allowed", ver.Version)
	}

	tempFilename := filepath.Join(dir, versionFilenameTemp)
	filename := getVersionFilename(dir)
	b, err := json.Marshal(ver)
	if err != nil {
		return err
	}
	// First write to a temp file.
	if err := ioutil.WriteFile(tempFilename, b, 0644); err != nil {
		return err
	}
	// Atomically rename the file to overwrite the version file on disk.
	return os.Rename(tempFilename, filename)
}
