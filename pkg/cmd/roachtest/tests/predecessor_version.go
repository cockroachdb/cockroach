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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
)

// PredecessorVersion returns a recent predecessor of the build version (i.e.
// the build tag of the main binary). For example, if the running binary is from
// the master branch prior to releasing 19.2.0, this will return a recent
// (ideally though not necessarily the latest) 19.1 patch release.
func PredecessorVersion(buildVersion version.Version) (string, error) {
	if buildVersion == (version.Version{}) {
		return "", errors.Errorf("buildVersion not set")
	}

	buildVersionMajorMinor := fmt.Sprintf("%d.%d", buildVersion.Major(), buildVersion.Minor())

	// You can update the values in verMap to point at newer patch releases.
	//
	// NB: If a new key was added (e.g. if you're releasing a new major
	// release), you'll also need to regenerate fixtures. To regenerate
	// fixtures, you will need to run acceptance/version-upgrade with the
	// checkpoint option enabled to create the missing store directory
	// fixture (see runVersionUpgrade).
	verMap := map[string]string{
		"22.1": "21.2.6",
		"21.2": "21.1.12",
		"21.1": "20.2.12",
		"20.2": "20.1.16",
		"20.1": "19.2.11",
		"19.2": "19.1.11",
		"19.1": "2.1.9",
		"2.2":  "2.1.9",
		"2.1":  "2.0.7",
	}
	v, ok := verMap[buildVersionMajorMinor]
	if !ok {
		return "", errors.Errorf("prev version not set for version: %s", buildVersionMajorMinor)
	}
	return v, nil
}
