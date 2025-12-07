// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backuputils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAbsoluteBackupPathInCollectionURI(t *testing.T) {
	testcases := []struct {
		name                     string
		collectionURI, backupURI string
		expected                 string
		error                    string
	}{
		{
			name:          "collection URI rooted at /",
			collectionURI: "nodelocal://1/",
			backupURI:     "nodelocal://1/foo/bar/baz",
			expected:      "/foo/bar/baz",
		},
		{
			name:          "backup URI contains trailing slash",
			collectionURI: "nodelocal://1/",
			backupURI:     "nodelocal://1/foo/bar/baz/",
			expected:      "/foo/bar/baz",
		},
		{
			name:          "collection URI contains non-empty path",
			collectionURI: "s3://backup/foo/bar",
			backupURI:     "s3://backup/foo/bar/baz",
			expected:      "/baz",
		},
		{
			name:          "collection and backup URI ends in trailing slash",
			collectionURI: "external://backup/foo/bar/",
			backupURI:     "external://backup/foo/bar/baz/qux/",
			expected:      "/baz/qux",
		},
		{
			name:          "backupURI is the same as collection URI",
			collectionURI: "gs://backup/foo/bar",
			backupURI:     "gs://backup/foo/bar",
			expected:      "/",
		},
		{
			name:          "backup URI not in collection URI",
			collectionURI: "gs://backup/foo/bar",
			backupURI:     "gs://backup/baz",
			error:         "backup URI not contained within collection URI",
		},
		{
			name:          "backup URI does not share same scheme or host as collection URI",
			collectionURI: "nodelocal://1/foo/bar",
			backupURI:     "gs://backup/foo/bar/baz",
			error:         "backup URI does not share the same scheme and host as collection URI",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := AbsoluteBackupPathInCollectionURI(tc.collectionURI, tc.backupURI)
			if tc.error != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.error)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestNormalizeSubdir(t *testing.T) {
	testcases := []struct {
		name       string
		input      string
		normalized string
		error      string
	}{
		{
			name:       "subdir without leading slash",
			input:      "2025/08/26-112233.44",
			normalized: "/2025/08/26-112233.44",
		},
		{
			name:       "subdir with leading and trailing slashes",
			input:      "/2025/08/26-112233.44/",
			normalized: "/2025/08/26-112233.44",
		},
		{
			name:       "subdir with only trailing slash",
			input:      "2025/08/26-112233.44/",
			normalized: "/2025/08/26-112233.44",
		},
		{
			name:       "subdir already normalized",
			input:      "/2025/08/26-112233.44",
			normalized: "/2025/08/26-112233.44",
		},
		{
			name:  "invalid subdir format",
			input: "2023-10-05_123456.78",
			error: "does not match expected format YYYY/MM/DD-HHMMSS.SS",
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := NormalizeSubdir(tc.input)
			if tc.error != "" {
				require.Error(t, err)
				require.ErrorContains(t, err, tc.error)
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.normalized, result)
		})
	}
}
