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
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestVersionGuard(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)

	type testCase struct {
		name            string
		storageVersion  clusterversion.Key
		settingsVersion clusterversion.Key
		checkVersions   map[clusterversion.Key]bool
	}

	initialVersion := clusterversion.V22_2
	maxVersion := clusterversion.V23_1
	tests := []testCase{
		{
			name:            "unfinalized",
			storageVersion:  initialVersion,
			settingsVersion: initialVersion,
			checkVersions: map[clusterversion.Key]bool{
				initialVersion:            true,
				clusterversion.V23_1Start: false,
				maxVersion:                false,
			},
		},
		{
			name:            "mid-finalize",
			storageVersion:  clusterversion.V23_1Start,
			settingsVersion: initialVersion,
			checkVersions: map[clusterversion.Key]bool{
				initialVersion:            true,
				clusterversion.V23_1Start: true,
				maxVersion:                false,
			},
		},
		{
			name:            "finalized",
			storageVersion:  maxVersion,
			settingsVersion: maxVersion,
			checkVersions: map[clusterversion.Key]bool{
				initialVersion:            true,
				clusterversion.V23_1Start: true,
				maxVersion:                true,
			},
		},
		{
			// Once the version guard's max version is active, it no longer
			// consults the stored value. This allows us to remove the overhead
			// of the version guard after finalization completes. A storage
			// version behind the settings version should not exist in
			// production.
			name:            "verify-optimization",
			storageVersion:  initialVersion,
			settingsVersion: maxVersion,
			checkVersions: map[clusterversion.Key]bool{
				initialVersion:            true,
				maxVersion:                true,
				clusterversion.V23_1Start: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			settings := cluster.MakeTestingClusterSettingsWithVersions(
				clusterversion.ByKey(maxVersion),
				clusterversion.ByKey(initialVersion),
				false,
			)
			require.NoError(t, clusterversion.Initialize(ctx, clusterversion.ByKey(test.settingsVersion), &settings.SV))
			settingVersion := clusterversion.ClusterVersion{Version: clusterversion.ByKey(test.settingsVersion)}
			require.NoError(t, settings.Version.SetActiveVersion(ctx, settingVersion))

			storageVersion := clusterversion.ClusterVersion{Version: clusterversion.ByKey(test.storageVersion)}
			marshaledVersion, err := storageVersion.Marshal()

			require.NoError(t, err)
			tDB.Exec(t, `
				UPDATE system.settings
				SET value = $1
				WHERE name = 'version'`, marshaledVersion)

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
