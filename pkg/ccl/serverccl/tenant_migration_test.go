// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package serverccl

import (
	"context"
	"fmt"
	"strings"
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
)

func TestValidateTargetTenantClusterVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := func(major, minor int32) roachpb.Version {
		return roachpb.Version{Major: major, Minor: minor}
	}
	cv := func(major, minor int32) clusterversion.ClusterVersion {
		return clusterversion.ClusterVersion{Version: v(major, minor)}
	}

	var tests = []struct {
		binaryVersion             roachpb.Version
		binaryMinSupportedVersion roachpb.Version
		targetClusterVersion      clusterversion.ClusterVersion
		expErrMatch               string // empty if expecting a nil error
	}{
		{
			binaryVersion:             v(21, 2),
			binaryMinSupportedVersion: v(21, 1),
			targetClusterVersion:      cv(21, 1),
			expErrMatch:               "",
		},
		{
			binaryVersion:             v(21, 2),
			binaryMinSupportedVersion: v(21, 1),
			targetClusterVersion:      cv(21, 2),
			expErrMatch:               "",
		},
		{
			binaryVersion:             v(21, 2),
			binaryMinSupportedVersion: v(21, 1),
			targetClusterVersion:      cv(22, 1),
			expErrMatch:               "sql pod 1 is running a binary version 21.2 which is less than the attempted upgrade version 22.1",
		},
		{
			binaryVersion:             v(21, 2),
			binaryMinSupportedVersion: v(21, 1),
			targetClusterVersion:      cv(20, 2),
			expErrMatch:               "requested tenant cluster upgrade version 20.2 is less than the tenant binary's minimum supported version 21.1",
		},
	}

	// tenant's minimum supported version <= target version <= node's binary version
	for i, test := range tests {
		st := cluster.MakeTestingClusterSettingsWithVersions(
			test.binaryVersion,
			test.binaryMinSupportedVersion,
			false, /* initializeVersion */
		)

		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
			// Disable the default test tenant, since we create one explicitly
			// below.
			DisableDefaultTestTenant: true,
			Settings:                 st,
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					BinaryVersionOverride: test.binaryVersion,
					// We're bumping cluster versions manually ourselves. We
					// want to avoid racing with the auto-upgrade process.
					DisableAutomaticVersionUpgrade: make(chan struct{}),
				},
			},
		})

		ctx := context.Background()
		upgradePod, err := s.StartTenant(ctx,
			base.TestTenantArgs{
				Settings: st,
				TenantID: serverutils.TestTenantID(),
				TestingKnobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						BinaryVersionOverride: test.binaryVersion,
					},
				},
			})
		if err != nil {
			t.Fatal(err)
		}

		tmServer := upgradePod.MigrationServer().(*server.TenantMigrationServer)
		req := &serverpb.ValidateTargetClusterVersionRequest{
			ClusterVersion: &test.targetClusterVersion,
		}

		_, err = tmServer.ValidateTargetClusterVersion(context.Background(), req)
		if !testutils.IsError(err, test.expErrMatch) {
			t.Fatalf("test %d: got error %s, wanted error matching '%s'", i, err, test.expErrMatch)
		}

		upgradePod.Stopper().Stop(context.Background())
		s.Stopper().Stop(context.Background())
	}
}

// TestBumpTenantClusterVersion verifies that the tenant BumpClusterVersion call
// correctly updates the active cluster version for the tenant pod. Unlike the
// server version of this test, we don't need to check that the updated value
// is persisted properly to disk, since there is only one on-disk copy of the
// tenant version (stored in the settings table).
func TestBumpTenantClusterVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	v := func(major, minor int32) roachpb.Version {
		return roachpb.Version{Major: major, Minor: minor}
	}
	cv := func(major, minor int32) clusterversion.ClusterVersion {
		return clusterversion.ClusterVersion{Version: v(major, minor)}
	}

	var tests = []struct {
		binaryVersion           roachpb.Version
		initialClusterVersion   clusterversion.ClusterVersion
		attemptedClusterVersion clusterversion.ClusterVersion
		expectedClusterVersion  clusterversion.ClusterVersion
	}{
		{
			binaryVersion:           v(21, 1),
			initialClusterVersion:   cv(20, 2),
			attemptedClusterVersion: cv(20, 2),
			expectedClusterVersion:  cv(20, 2),
		},
		{
			binaryVersion:           v(21, 1),
			initialClusterVersion:   cv(20, 2),
			attemptedClusterVersion: cv(21, 1),
			expectedClusterVersion:  cv(21, 1),
		},
		{
			binaryVersion:           v(21, 1),
			initialClusterVersion:   cv(21, 1),
			attemptedClusterVersion: cv(20, 2),
			expectedClusterVersion:  cv(21, 1),
		},
		{
			binaryVersion:           v(21, 1),
			initialClusterVersion:   cv(21, 1),
			attemptedClusterVersion: cv(22, 1),
			expectedClusterVersion:  cv(21, 1),
		},
	}

	ctx := context.Background()
	for i, test := range tests {
		t.Run(fmt.Sprintf("config=%d", i), func(t *testing.T) {
			st := cluster.MakeTestingClusterSettingsWithVersions(
				test.binaryVersion,
				test.initialClusterVersion.Version,
				false, /* initializeVersion */
			)

			s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
				// Disable the default tenant because we're creating one
				// explicitly below.
				DisableDefaultTestTenant: true,
				Settings:                 st,
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						// This test wants to bootstrap at the previously active
						// cluster version, so we can actually bump the cluster
						// version to the binary version.
						BinaryVersionOverride: test.initialClusterVersion.Version,
						// We're bumping cluster versions manually ourselves. We
						// want to avoid racing with the auto-upgrade process.
						DisableAutomaticVersionUpgrade: make(chan struct{}),
					},
				},
			})
			defer s.Stopper().Stop(context.Background())

			tenant, err := s.StartTenant(ctx,
				base.TestTenantArgs{
					Settings: st,
					TenantID: serverutils.TestTenantID(),
					TestingKnobs: base.TestingKnobs{
						Server: &server.TestingKnobs{
							BinaryVersionOverride: test.initialClusterVersion.Version,
						},
					},
				})
			if err != nil {
				t.Fatal(err)
			}
			defer tenant.Stopper().Stop(context.Background())

			// Check to see our initial active cluster versions are what we
			// expect.
			if got := tenant.ClusterSettings().Version.ActiveVersion(ctx); got != test.initialClusterVersion {
				t.Fatalf("tenant: expected active cluster version %s, got %s", test.initialClusterVersion, got)
			}

			// Do the bump.
			tmServer := tenant.MigrationServer().(*server.TenantMigrationServer)
			req := &serverpb.BumpClusterVersionRequest{
				ClusterVersion: &test.attemptedClusterVersion,
			}
			if _, err = tmServer.BumpClusterVersion(ctx, req); err != nil {
				// Accept failed upgrade errors. Fatal at the rest.
				if !strings.Contains(err.Error(), "cannot upgrade") {
					t.Fatal(err)
				}
			}

			// Check to see if our post-bump active cluster versions are what we
			// expect.
			if got := tenant.ClusterSettings().Version.ActiveVersion(ctx); got != test.expectedClusterVersion {
				t.Fatalf("tenant: expected active cluster version %s, got %s", test.expectedClusterVersion, got)
			}
		})
	}
}
