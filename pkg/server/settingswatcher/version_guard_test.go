// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settingswatcher_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestVersionGuard(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	if clusterversion.MinSupported == clusterversion.PreviousRelease {
		skip.IgnoreLint(t, "MinSupported = PreviousRelease")
	}

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()
	tDB := sqlutils.MakeSQLRunner(sqlDB)

	type testCase struct {
		name            string
		storageVersion  *clusterversion.Key
		settingsVersion clusterversion.Key
		checkVersions   map[clusterversion.Key]bool
	}

	initialVersion := clusterversion.MinSupported
	startVersion := clusterversion.MinSupported + 1
	maxVersion := clusterversion.PreviousRelease

	tests := []testCase{
		{
			name:            "bootstrap-old-version",
			storageVersion:  nil,
			settingsVersion: initialVersion,
			checkVersions: map[clusterversion.Key]bool{
				initialVersion: true,
				startVersion:   false,
				maxVersion:     false,
			},
		},
		{
			name:            "bootstrap-current-version",
			storageVersion:  nil,
			settingsVersion: maxVersion,
			checkVersions: map[clusterversion.Key]bool{
				initialVersion: true,
				startVersion:   true,
				maxVersion:     true,
			},
		},
		{
			name:            "unfinalized",
			storageVersion:  &initialVersion,
			settingsVersion: initialVersion,
			checkVersions: map[clusterversion.Key]bool{
				initialVersion: true,
				startVersion:   false,
				maxVersion:     false,
			},
		},
		{
			name:            "mid-finalize",
			storageVersion:  &startVersion,
			settingsVersion: initialVersion,
			checkVersions: map[clusterversion.Key]bool{
				initialVersion: true,
				startVersion:   true,
				maxVersion:     false,
			},
		},
		{
			name:            "finalized",
			storageVersion:  &maxVersion,
			settingsVersion: maxVersion,
			checkVersions: map[clusterversion.Key]bool{
				initialVersion: true,
				startVersion:   true,
				maxVersion:     true,
			},
		},
		{
			// Once the version guard's max version is active, it no longer
			// consults the stored value. This allows us to remove the overhead
			// of the version guard after finalization completes. A storage
			// version behind the settings version should not exist in
			// production.
			name:            "verify-optimization",
			storageVersion:  &initialVersion,
			settingsVersion: maxVersion,
			checkVersions: map[clusterversion.Key]bool{
				initialVersion: true,
				startVersion:   true,
				maxVersion:     true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			settings := cluster.MakeTestingClusterSettingsWithVersions(
				maxVersion.Version(),
				initialVersion.Version(),
				false,
			)
			require.NoError(t, clusterversion.Initialize(ctx, test.settingsVersion.Version(), &settings.SV))
			settingVersion := clusterversion.ClusterVersion{Version: test.settingsVersion.Version()}
			require.NoError(t, settings.Version.SetActiveVersion(ctx, settingVersion))

			if test.storageVersion == nil {
				tDB.Exec(t, `DELETE FROM system.settings WHERE name = 'version'`)
			} else {
				storageVersion := clusterversion.ClusterVersion{Version: test.storageVersion.Version()}
				marshaledVersion, err := protoutil.Marshal(&storageVersion)
				require.NoError(t, err)
				tDB.Exec(t, `
					UPSERT INTO system.settings (name, value, "lastUpdated", "valueType")
					VALUES ('version', $1, now(), 'm')`,
					marshaledVersion)

			}

			watcher := settingswatcher.New(nil, s.Codec(), settings, nil, nil, nil)
			require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				guard, err := watcher.MakeVersionGuard(ctx, txn, maxVersion)
				if err != nil {
					return err
				}

				for version, expect := range test.checkVersions {
					require.Equal(t, expect, guard.IsActive(version), "expect guard.IsActive(%v) to be %t", version, expect)
				}

				return nil
			}))
		})
	}
}
