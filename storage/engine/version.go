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

const (
	versionFilename = "COCKROACHDB_VERSION"
	versionNoFile   = 0
	versionCurrent  = 1
	versionMinimum  = 0
)

// Version stores all the version information for all stores and is used as
// the format for the version file.
type Version struct {
	Version int `json:"Version"`
}

// getVersionFilename returns the filename for the version file stored in the
// data directory.
func getVersionFilename(dir string) string {
	return filepath.Join(dir, versionFilename)
}

// getVersions returns the current on disk cockroach version from the version
// file from the passed in directory. If there is no version file yet, it
// returns 0.
func getVersion(dir string) (int, error) {
	filename := getVersionFilename(dir)
	b, err := ioutil.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var ver Version
	if err := json.Unmarshal(b, &ver); err != nil {
		return 0, fmt.Errorf("version file %s is not formatted correctly; %s", filename, err)
	}
	return ver.Version, nil
}

// writeVersionFile overwrites the version file to contain the latest version.
func writeVersionFile(dir string) error {
	filename := getVersionFilename(dir)
	if len(filename) == 0 {
		return nil
	}
	if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
		return err
	}
	b, err := json.Marshal(Version{versionCurrent})
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filename, b, 0644)
}
