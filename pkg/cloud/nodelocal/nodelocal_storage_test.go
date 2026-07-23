// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nodelocal

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestPutLocal(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	dest := MakeLocalStorageURI(p)

	info := cloudtestutils.StoreInfo{
		URI:           dest,
		User:          username.RootUserName(),
		ExternalIODir: p,
	}
	cloudtestutils.CheckExportStore(t, info)
	info.URI = "nodelocal://1/listing-test/basepath"
	cloudtestutils.CheckListFiles(t, info)
}

func TestParseNodeLocalInstanceID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name        string
		uri         string
		expected    base.SQLInstanceID
		expectError bool
	}{
		{
			name:        "valid nodelocal URI with instance 1",
			uri:         "nodelocal://1/path/to/file.sst",
			expected:    1,
			expectError: false,
		},
		{
			name:        "valid nodelocal URI with instance 42",
			uri:         "nodelocal://42/job/123/merge/file.sst",
			expected:    42,
			expectError: false,
		},
		{
			name:        "valid nodelocal URI with high instance ID",
			uri:         "nodelocal://999/some/path.sst",
			expected:    999,
			expectError: false,
		},
		{
			name:        "nodelocal with self host - returns 0",
			uri:         "nodelocal://self/path",
			expected:    0,
			expectError: false,
		},
		{
			name:        "nodelocal with empty host - returns 0",
			uri:         "nodelocal:///path",
			expected:    0,
			expectError: true,
		},
		{
			name:        "s3 URI - error (not nodelocal)",
			uri:         "s3://bucket/path",
			expected:    0,
			expectError: true,
		},
		{
			name:        "gs URI - error (not nodelocal)",
			uri:         "gs://bucket/path",
			expected:    0,
			expectError: true,
		},
		{
			name:        "invalid URI - error",
			uri:         "://invalid",
			expected:    0,
			expectError: true,
		},
		{
			name:        "nodelocal with non-numeric host - error",
			uri:         "nodelocal://abc/path",
			expected:    0,
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseInstanceID(tc.uri)
			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, got)
			}
		})
	}
}
