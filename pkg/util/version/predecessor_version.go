// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package version

import (
	// Import embed for the version map
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/errors"
)

// You can update the values in predecessor_version.json to point at newer patch releases.
//
// NB: If a new key was added (e.g. if you're releasing a new major
// release), you'll also need to regenerate fixtures. To regenerate
// fixtures, you will need to run acceptance/version-upgrade with the
// checkpoint option enabled to create the missing store directory
// fixture (see runVersionUpgrade).
//
//go:embed predecessor_version.json
var verMapJSON []byte

type versionMap map[string]string

func unmarshalVersionMap() (versionMap, error) {
	var res versionMap
	err := json.Unmarshal(verMapJSON, &res)
	if err != nil {
		return versionMap{}, err
	}
	return res, nil
}

// PredecessorHistory returns the last consecutive `k` releases that
// precede the given buildVersion in the upgrade order (as dictated by
// predecessor_version.json). E.g., if buildVersion=22.2 and k=2, then
// this function will return ["21.2.7", "22.1.6"] (at the time of
// writing).
func PredecessorHistory(buildVersion Version, k int) ([]string, error) {
	if buildVersion == (Version{}) {
		return nil, errors.Errorf("buildVersion not set")
	}

	verMap, err := unmarshalVersionMap()
	if err != nil {
		return nil, errors.Wrap(err, "cannot load version map")
	}

	var versions []string
	currentVersion := &buildVersion
	for j := 0; j < k; j++ {
		currentVersionMajorMinor := fmt.Sprintf("%d.%d", currentVersion.Major(), currentVersion.Minor())
		v, ok := verMap[currentVersionMajorMinor]
		if !ok {
			return nil, errors.Errorf("prev version not set for version: %s", currentVersionMajorMinor)
		}

		versions = append([]string{v}, versions...)
		formattedVersion := fmt.Sprintf("v%s", v)
		currentVersion, err = Parse(formattedVersion)
		if err != nil {
			return nil, fmt.Errorf("invalid previous version %s: %w", formattedVersion, err)
		}
	}

	return versions, nil
}

// PredecessorVersion returns a recent predecessor of the build version (i.e.
// the build tag of the main binary). For example, if the running binary is from
// the master branch prior to releasing 19.2.0, this will return a recent
// (ideally though not necessarily the latest) 19.1 patch release.
func PredecessorVersion(buildVersion Version) (string, error) {
	history, err := PredecessorHistory(buildVersion, 1)
	if err != nil {
		return "", err
	}

	return history[0], nil
}
