// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cloud_test

import (
	"context"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSanitizeExternalStorageURI(t *testing.T) {
	// Register a scheme to test scheme-specific redaction.
	cloud.RegisterExternalStorageProvider(0, cloud.RegisteredProvider{
		ParseFn: func(cloud.ExternalStorageURIContext, *url.URL) (cloudpb.ExternalStorage, error) {
			return cloudpb.ExternalStorage{}, errors.Newf("unimplemented")
		},
		ConstructFn: func(context.Context, cloud.ExternalStorageContext, cloudpb.ExternalStorage) (cloud.ExternalStorage, error) {
			return nil, errors.Newf("unimplemented")
		},
		RedactedParams: cloud.RedactedParams("TEST_PARAM"),
		Schemes:        []string{"test-scheme"},
	})
	testCases := []struct {
		name             string
		inputURI         string
		inputExtraParams []string
		expected         string
	}{
		{
			name:     "redacts password",
			inputURI: "http://username:password@foo.com/something",
			expected: "http://username:redacted@foo.com/something",
		},
		{
			name:             "redacts given parameters",
			inputURI:         "http://foo.com/something?secret_key=uhoh",
			inputExtraParams: []string{"secret_key"},
			expected:         "http://foo.com/something?secret_key=redacted",
		},
		{
			name:     "redacts registered parameters",
			inputURI: "test-scheme://somehost/somepath?TEST_PARAM=uhoh",
			expected: "test-scheme://somehost/somepath?TEST_PARAM=redacted",
		},
		{
			name:     "preserves username",
			inputURI: "http://username@foo.com/something",
			expected: "http://username@foo.com/something",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actualOutput, err := cloud.SanitizeExternalStorageURI(tc.inputURI, tc.inputExtraParams)
			require.NoError(t, err)
			require.Equal(t, tc.expected, actualOutput)
		})
	}
}

func TestSanitizedJoin(t *testing.T) {
	tests := []struct {
		name         string
		basePath     string
		userPath     string
		expectedPath string
		expectedErr  string
	}{
		{
			name:         "subdirectory",
			basePath:     "/tenant1/backups",
			userPath:     "subdir",
			expectedPath: "/tenant1/backups/subdir",
		},
		{
			name:         "nested",
			basePath:     "/tenant1/backups",
			userPath:     "a/b/c",
			expectedPath: "/tenant1/backups/a/b/c",
		},
		{
			name:         "empty user path",
			basePath:     "/tenant1/backups",
			userPath:     "",
			expectedPath: "/tenant1/backups",
		},
		{
			name:         "dot",
			basePath:     "/tenant1/backups",
			userPath:     ".",
			expectedPath: "/tenant1/backups",
		},
		{
			name:         "benign dot-dot within base",
			basePath:     "/tenant1/backups",
			userPath:     "a/../b",
			expectedPath: "/tenant1/backups/b",
		},
		{
			name:         "root base with dot-dot",
			basePath:     "/",
			userPath:     "../anything",
			expectedPath: "/anything",
		},
		{
			name:         "root base with subdir",
			basePath:     "/",
			userPath:     "subdir",
			expectedPath: "/subdir",
		},
		{
			name:         "empty base with subdir",
			basePath:     "",
			userPath:     "subdir",
			expectedPath: "subdir",
		},
		{
			name:        "empty base with dot-dot escape",
			basePath:    "",
			userPath:    "../../secret",
			expectedErr: "escapes",
		},
		{
			name:         "trailing slash base with subdir",
			basePath:     "/tenant1/backups/",
			userPath:     "subdir",
			expectedPath: "/tenant1/backups/subdir",
		},
		{
			name:         "trailing slash base with empty user path",
			basePath:     "/tenant1/backups/",
			userPath:     "",
			expectedPath: "/tenant1/backups",
		},
		{
			name:        "trailing slash base with escape",
			basePath:    "/tenant1/backups/",
			userPath:    "../tenant2",
			expectedErr: "escapes",
		},
		{
			name:         "similar prefix not an escape",
			basePath:     "/tenant1",
			userPath:     "-admin/data",
			expectedPath: "/tenant1/-admin/data",
		},
		{
			name:        "simple escape",
			basePath:    "/tenant1/backups",
			userPath:    "../tenant2",
			expectedErr: "escapes",
		},
		{
			name:        "double escape",
			basePath:    "/tenant1/backups",
			userPath:    "../../secret",
			expectedErr: "escapes",
		},
		{
			name:        "leading slash with dot-dot",
			basePath:    "/tenant1/backups",
			userPath:    "/../escape",
			expectedErr: "escapes",
		},
		{
			name:        "escape with trailing content",
			basePath:    "/tenant1/backups",
			userPath:    "../tenant2/data",
			expectedErr: "escapes",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			joined, err := cloud.SanitizedJoin(tc.basePath, tc.userPath)
			if tc.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedPath, joined)
			}
		})
	}
}
