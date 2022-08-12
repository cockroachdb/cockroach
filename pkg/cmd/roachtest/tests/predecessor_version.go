// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	// Import embed for the version map
	_ "embed"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/version"
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

// PredecessorVersion returns a recent predecessor of the build version (i.e.
// the build tag of the main binary). For example, if the running binary is from
// the master branch prior to releasing 19.2.0, this will return a recent
// (ideally though not necessarily the latest) 19.1 patch release.
func PredecessorVersion(buildVersion version.Version) (string, error) {
	if buildVersion == (version.Version{}) {
		return "", errors.Errorf("buildVersion not set")
	}

	verMap, err := unmarshalVersionMap()
	if err != nil {
		return "", errors.Wrap(err, "cannot load version map")
	}
	buildVersionMajorMinor := fmt.Sprintf("%d.%d", buildVersion.Major(), buildVersion.Minor())
	v, ok := verMap[buildVersionMajorMinor]
	if !ok {
		return "", errors.Errorf("prev version not set for version: %s", buildVersionMajorMinor)
	}
	return v, nil
}
