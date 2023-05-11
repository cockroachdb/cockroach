// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestBackupManifestVersionCompatibility(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type testCase struct {
		name                    string
		backupVersion           roachpb.Version
		clusterVersion          roachpb.Version
		minimumSupportedVersion roachpb.Version
		expectedError           string
	}

	binaryVersion := roachpb.Version{Major: 23, Minor: 1}
	tests := []testCase{
		{
			name:                    "same-version-restore",
			backupVersion:           roachpb.Version{Major: 23, Minor: 1},
			clusterVersion:          roachpb.Version{Major: 23, Minor: 1},
			minimumSupportedVersion: roachpb.Version{Major: 22, Minor: 2},
		},
		{
			name:                    "previous-version-restore",
			backupVersion:           roachpb.Version{Major: 23, Minor: 1},
			clusterVersion:          roachpb.Version{Major: 23, Minor: 1},
			minimumSupportedVersion: roachpb.Version{Major: 22, Minor: 2},
		},
		{
			name:                    "unfinalized-restore",
			backupVersion:           roachpb.Version{Major: 23, Minor: 1},
			clusterVersion:          roachpb.Version{Major: 22, Minor: 2},
			minimumSupportedVersion: roachpb.Version{Major: 22, Minor: 2},
			expectedError:           "backup from version 23.1 is newer than current version 22.2",
		},
		{
			name:                    "alpha-restore",
			backupVersion:           roachpb.Version{Major: 100022, Minor: 2, Internal: 14},
			clusterVersion:          roachpb.Version{Major: 23, Minor: 1},
			minimumSupportedVersion: roachpb.Version{Major: 22, Minor: 2},
			expectedError:           "backup from version 100022.2-14 is newer than current version 23.1",
		},
		{
			name:                    "old-backup",
			backupVersion:           roachpb.Version{Major: 22, Minor: 1},
			clusterVersion:          roachpb.Version{Major: 23, Minor: 1},
			minimumSupportedVersion: roachpb.Version{Major: 22, Minor: 2},
			expectedError:           "backup from version 22.1 is older than the minimum restorable version 22.2",
		},
		{
			name:                    "legacy-version-backup",
			backupVersion:           roachpb.Version{},
			clusterVersion:          roachpb.Version{Major: 23, Minor: 1},
			minimumSupportedVersion: roachpb.Version{Major: 22, Minor: 2},
			expectedError:           "the backup is from a version older than our minimum restorable version 22.2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			settings := cluster.MakeTestingClusterSettingsWithVersions(binaryVersion, tc.minimumSupportedVersion, false)
			require.NoError(t, clusterversion.Initialize(context.Background(), tc.clusterVersion, &settings.SV))
			version := clusterversion.MakeVersionHandleWithOverride(&settings.SV, binaryVersion, tc.minimumSupportedVersion)
			manifest := []backuppb.BackupManifest{{ClusterVersion: tc.backupVersion}}

			err := checkBackupManifestVersionCompatability(context.Background(), version, manifest /*unsafe=*/, false)
			if tc.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.expectedError)
			}

			require.NoError(t, checkBackupManifestVersionCompatability(context.Background(), version, manifest /*unsafe=*/, true))
		})
	}
}
