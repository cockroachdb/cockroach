// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package build

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputeBinaryVersion(t *testing.T) {
	testCases := []struct {
		name            string
		versionTxt      string
		revision        string
		buildType       string
		panicExpected   bool
		expectedVersion string
	}{
		{
			name:          "empty version.txt",
			versionTxt:    "",
			revision:      "abc123",
			panicExpected: true,
		},
		{
			name:          "invalid version.txt",
			versionTxt:    "vInvalid.23",
			revision:      "abc123",
			panicExpected: true,
		},
		{
			name:            "dev release",
			versionTxt:      "v21.2.0",
			revision:        "abc123",
			buildType:       "development",
			expectedVersion: "v21.2.0-dev-abc123",
		},
		{
			name:            "release binary",
			versionTxt:      "v21.2.0",
			revision:        "abc123",
			buildType:       "release",
			expectedVersion: "v21.2.0",
		},
		{
			name:            "dev pre-release",
			versionTxt:      "v21.2.0-alpha.2",
			revision:        "abc123",
			buildType:       "development",
			expectedVersion: "v21.2.0-alpha.2-dev-abc123",
		},
		{
			name:            "pre-release binary",
			versionTxt:      "v21.2.0-alpha.2",
			revision:        "abc123",
			buildType:       "release",
			expectedVersion: "v21.2.0-alpha.2",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			oldBuildType := typ
			typ = tc.buildType
			defer func() { typ = oldBuildType }()

			if tc.panicExpected {
				require.Panics(t, func() { computeBinaryVersion(tc.versionTxt, tc.revision) })
			} else {
				actualVersion := computeBinaryVersion(tc.versionTxt, tc.revision)
				require.Equal(t, tc.expectedVersion, actualVersion)
			}
		})
	}
}
