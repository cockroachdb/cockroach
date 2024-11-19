// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestValidateTargetTenantClusterVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	prev := clusterversion.ClusterVersion{Version: clusterversion.MinSupported.Version()}
	cur := clusterversion.ClusterVersion{Version: clusterversion.Latest.Version()}
	// In cases where we use prev as the binary version for the test, set the
	// minimum supported version to prev's binary version - 1 Major version.
	prevMsv := clusterversion.ClusterVersion{
		Version: roachpb.Version{
			Major: prev.Version.Major - 1,
			Minor: prev.Version.Minor,
		},
	}

	var tests = []struct {
		binaryVersion             roachpb.Version
		binaryMinSupportedVersion roachpb.Version
		targetClusterVersion      clusterversion.ClusterVersion
		expErrMatch               string // empty if expecting a nil error
	}{
		{
			binaryVersion:             cur.Version,
			binaryMinSupportedVersion: prev.Version,
			targetClusterVersion:      prev,
			expErrMatch:               "",
		},
		{
			binaryVersion:             cur.Version,
			binaryMinSupportedVersion: prev.Version,
			targetClusterVersion:      cur,
			expErrMatch:               "",
		},
		{
			binaryVersion:             prev.Version,
			binaryMinSupportedVersion: prevMsv.Version,
			targetClusterVersion:      cur,
			expErrMatch:               fmt.Sprintf("sql server 1 is running a binary version %s which is less than the attempted upgrade version %s", prev.String(), cur.String()),
		},
		{
			binaryVersion:             cur.Version,
			binaryMinSupportedVersion: prev.Version,
			targetClusterVersion:      prevMsv,
			expErrMatch:               fmt.Sprintf("requested tenant cluster upgrade version %s is less than the binary's minimum supported version %s for SQL server instance 1", prevMsv.String(), prev.String()),
		},
	}

	// tenant's minimum supported version <= target version <= node's binary version
	for i, test := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			defer log.Scope(t).Close(t)

			makeSettings := func() *cluster.Settings {
				st := cluster.MakeTestingClusterSettingsWithVersions(
					test.binaryVersion,
					test.binaryMinSupportedVersion,
					false, /* initializeVersion */
				)
				return st
			}

			s := serverutils.StartServerOnly(t, base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
				Settings:          makeSettings(),
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						ClusterVersionOverride: test.binaryVersion,
						// We're bumping cluster versions manually ourselves. We
						// want to avoid racing with the auto-upgrade process.
						DisableAutomaticVersionUpgrade: make(chan struct{}),
					},
				},
			})
			defer s.Stopper().Stop(context.Background())

			ctx := context.Background()
			upgradePod, err := s.TenantController().StartTenant(ctx,
				base.TestTenantArgs{
					Settings: makeSettings(),
					TenantID: serverutils.TestTenantID(),
					TestingKnobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							ClusterVersionOverride: test.binaryVersion,
						},
					},
				})
			if err != nil {
				t.Fatal(err)
			}
			defer upgradePod.AppStopper().Stop(context.Background())

			tmServer := upgradePod.MigrationServer().(*server.TenantMigrationServer)
			req := &serverpb.ValidateTargetClusterVersionRequest{
				ClusterVersion: &test.targetClusterVersion,
			}

			_, err = tmServer.ValidateTargetClusterVersion(context.Background(), req)
			if !testutils.IsError(err, test.expErrMatch) {
				t.Fatalf("test %d: got error %s, wanted error matching '%s'", i, err, test.expErrMatch)
			}
		})
	}
}

// TestBumpTenantClusterVersion verifies that the tenant BumpClusterVersion call
// correctly updates the active cluster version for the tenant pod. Unlike the
// server version of this test, we don't need to check that the updated value
// is persisted properly to disk, since there is only one on-disk copy of the
// tenant version (stored in the settings table).
func TestBumpTenantClusterVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	prev := clusterversion.ClusterVersion{Version: clusterversion.MinSupported.Version()}
	cur := clusterversion.ClusterVersion{Version: clusterversion.Latest.Version()}
	// In cases where we use prev as the binary version for the test, set the
	// minimum supported version to prev's binary version - 1 Major version.
	prevMsv := clusterversion.ClusterVersion{
		Version: roachpb.Version{
			Major: prev.Version.Major - 1,
			Minor: prev.Version.Minor,
		},
	}

	var tests = []struct {
		binaryVersion           clusterversion.ClusterVersion
		minSupportedVersion     clusterversion.ClusterVersion
		initialClusterVersion   clusterversion.ClusterVersion
		attemptedClusterVersion clusterversion.ClusterVersion
		expectedClusterVersion  clusterversion.ClusterVersion
	}{
		{
			binaryVersion:           cur,
			minSupportedVersion:     prev,
			initialClusterVersion:   prev,
			attemptedClusterVersion: prev,
			expectedClusterVersion:  prev,
		},
		{
			binaryVersion:           cur,
			minSupportedVersion:     prev,
			initialClusterVersion:   prev,
			attemptedClusterVersion: cur,
			expectedClusterVersion:  cur,
		},
		{
			binaryVersion:           cur,
			minSupportedVersion:     prev,
			initialClusterVersion:   cur,
			attemptedClusterVersion: prev,
			expectedClusterVersion:  cur,
		},
		{
			binaryVersion:           prev,
			minSupportedVersion:     prevMsv,
			initialClusterVersion:   prev,
			attemptedClusterVersion: cur,
			expectedClusterVersion:  prev,
		},
	}

	ctx := context.Background()
	for i, test := range tests {
		t.Run(fmt.Sprintf("config=%d", i), func(t *testing.T) {
			defer log.Scope(t).Close(t)

			makeSettings := func() *cluster.Settings {
				st := cluster.MakeTestingClusterSettingsWithVersions(
					test.binaryVersion.Version,
					test.minSupportedVersion.Version,
					false, /* initializeVersion */
				)
				return st
			}

			s := serverutils.StartServerOnly(t, base.TestServerArgs{
				DefaultTestTenant: base.TestControlsTenantsExplicitly,
				Settings:          makeSettings(),
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						// This test wants to bootstrap at the previously active
						// cluster version, so we can actually bump the cluster
						// version to the binary version.
						ClusterVersionOverride: test.initialClusterVersion.Version,
						// We're bumping cluster versions manually ourselves. We
						// want to avoid racing with the auto-upgrade process.
						DisableAutomaticVersionUpgrade: make(chan struct{}),
					},
				},
			})
			defer s.Stopper().Stop(context.Background())

			tenant, err := s.TenantController().StartTenant(ctx,
				base.TestTenantArgs{
					Settings: makeSettings(),
					TenantID: serverutils.TestTenantID(),
					TestingKnobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							ClusterVersionOverride: test.initialClusterVersion.Version,
						},
					},
				})
			if err != nil {
				t.Fatal(err)
			}
			defer tenant.AppStopper().Stop(context.Background())

			// Check to see our initial active cluster versions are what we
			// expect.
			got := tenant.ClusterSettings().Version.ActiveVersion(ctx)
			require.Equal(t, got, test.initialClusterVersion)

			// Do the bump.
			tmServer := tenant.MigrationServer().(*server.TenantMigrationServer)
			req := &serverpb.BumpClusterVersionRequest{
				ClusterVersion: &test.attemptedClusterVersion,
			}
			if _, err = tmServer.BumpClusterVersion(ctx, req); err != nil {
				// Accept failed upgrade errors. Fatal at the rest.
				require.ErrorContains(t, err, "less than the attempted upgrade version")
			}

			// Check to see if our post-bump active cluster versions are what we
			// expect.
			got = tenant.ClusterSettings().Version.ActiveVersion(ctx)
			require.Equal(t, got, test.expectedClusterVersion)
		})
	}
}
