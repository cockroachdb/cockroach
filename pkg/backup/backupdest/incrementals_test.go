// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backupdest_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/backup/backupdest"
	"github.com/cockroachdb/cockroach/pkg/backup/backuputils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCollectionsAndSubdir(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testcase struct {
		name                string
		paths               []string
		subdir              string
		expectedCollections []string
		expectedSubdir      string
		expectedError       string
	}
	testcases := []testcase{
		{
			name:                "non-empty subdir returns unmodified collection",
			paths:               []string{"nodelocal://1/backup-dest/2023/05/10-160331.72/"},
			subdir:              "foo",
			expectedCollections: []string{"nodelocal://1/backup-dest/2023/05/10-160331.72/"},
			expectedSubdir:      "foo",
		},
		{
			name: "non-empty subdir returns all parts of unmodified collection",
			paths: []string{
				"nodelocal://1/backup-dest/",
				"nodelocal://2/backup-dest/",
			},
			subdir: "foo",
			expectedCollections: []string{
				"nodelocal://1/backup-dest/",
				"nodelocal://2/backup-dest/",
			},
			expectedSubdir: "foo",
		},
		{
			name:                "date-based-path is returned as subdir if no subdir is provided",
			paths:               []string{"nodelocal://1/backup-dest/2023/05/10-160331.72/"},
			expectedCollections: []string{"nodelocal://1/backup-dest"},
			expectedSubdir:      "2023/05/10-160331.72/",
		},
		{
			name: "multiple date-based paths are returned as subdir if no subdir is provided",
			paths: []string{
				"nodelocal://1/backup-dest/2023/05/10-160331.72/",
				"nodelocal://2/backup-dest/2023/05/10-160331.72/",
			},
			expectedCollections: []string{
				"nodelocal://1/backup-dest",
				"nodelocal://2/backup-dest",
			},
			expectedSubdir: "2023/05/10-160331.72/",
		},
		{
			name: "paths that don't match are returned unmodified",
			paths: []string{
				"nodelocal://1/backup-dest/2023/05/9999/",
				"nodelocal://2/backup-dest/2023/05/9999/",
			},
			expectedCollections: []string{
				"nodelocal://1/backup-dest/2023/05/9999/",
				"nodelocal://2/backup-dest/2023/05/9999/",
			},
			expectedSubdir: "",
		},
		{
			name: "different date-based paths results in an error",
			paths: []string{
				"nodelocal://1/backup-dest/2023/05/10-160331.72/",
				"nodelocal://2/backup-dest/2023/05/10-160331.73/",
			},
			expectedError: "provided backup locations appear to reference different full backups",
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			collections, subdir, err := backupdest.CollectionsAndSubdir(tc.paths, tc.subdir)
			if tc.expectedError != "" {
				require.ErrorContains(t, err, tc.expectedError)
			} else {
				require.Equal(t, tc.expectedSubdir, subdir)
				require.Equal(t, tc.expectedCollections, collections)
			}
		})
	}
}

func TestJoinURLPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// path.Join has identical behavior for these inputs.
	require.Equal(t, "/top/path", backuputils.JoinURLPath("/top", "path"))
	require.Equal(t, "top/path", backuputils.JoinURLPath("top", "path"))

	require.Equal(t, "/path", backuputils.JoinURLPath("/top", "../path"))
	require.Equal(t, "path", backuputils.JoinURLPath("top", "../path"))

	require.Equal(t, "../path", backuputils.JoinURLPath("top", "../../path"))

	// path.Join has different behavior for this input.
	require.Equal(t, "/../path", backuputils.JoinURLPath("/top", "../../path"))

}
