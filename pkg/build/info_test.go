// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package build

import (
	"testing"

	"github.com/cockroachdb/version"
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
				require.Panics(t, func() { parseCockroachVersion(tc.versionTxt) })
			} else {
				v := parseCockroachVersion(tc.versionTxt)
				actualVersion := computeBinaryVersion("" /* buildTagOverride */, v, tc.revision)
				require.Equal(t, tc.expectedVersion, actualVersion)
			}
		})
	}
}

func TestVersionForURLs(t *testing.T) {
	testCases := []struct {
		versionTxt string
		want       string
	}{
		{
			versionTxt: "v25.2.0",
			want:       "v25.2",
		},
		{
			versionTxt: "v25.2.0-alpha.000000",
			want:       "dev",
		},
		{
			versionTxt: "v25.2.0-alpha.1",
			want:       "v25.2",
		},
		{
			versionTxt: "v25.2.0-beta.1",
			want:       "v25.2",
		},
		{
			versionTxt: "v25.2.0-rc.2",
			want:       "v25.2",
		},
		{
			versionTxt: "v25.2.0-cloudonly.1",
			want:       "v25.2",
		},
		{
			versionTxt: "v25.2.0-12-gabcdef01234",
			want:       "dev",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.versionTxt, func(t *testing.T) {
			oldParsedVersionTxt := parsedVersionTxt
			defer func() { parsedVersionTxt = oldParsedVersionTxt }()
			v := version.MustParse(tc.versionTxt)
			parsedVersionTxt = &v

			require.Equal(t, tc.want, VersionForURLs())
		})
	}
}
