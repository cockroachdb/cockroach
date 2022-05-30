// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

func registerAcceptance(r registry.Registry) {
	testCases := map[registry.Owner][]struct {
		name              string
		fn                func(ctx context.Context, t test.Test, c cluster.Cluster)
		skip              string
		minVersion        string
		numNodes          int
		timeout           time.Duration
		encryptionSupport registry.EncryptionSupport
	}{
		registry.OwnerKV: {
			{name: "decommission-self", fn: runDecommissionSelf},
			{name: "event-log", fn: runEventLog},
			{name: "gossip/peerings", fn: runGossipPeerings},
			{name: "gossip/restart", fn: runGossipRestart},
			{
				name:              "gossip/restart-node-one",
				fn:                runGossipRestartNodeOne,
				encryptionSupport: registry.EncryptionAlwaysDisabled,
			},
			{name: "gossip/locality-address", fn: runCheckLocalityIPAddress},
			{
				name: "multitenant",
				skip: "https://github.com/cockroachdb/cockroach/issues/81506",
				fn:   runAcceptanceMultitenant,
			},
			{name: "reset-quorum", fn: runResetQuorum, numNodes: 8},
			{
				name: "many-splits", fn: runManySplits,
				minVersion:        "v19.2.0", // SQL syntax unsupported on 19.1.x
				encryptionSupport: registry.EncryptionMetamorphic,
			},
			{
				name: "version-upgrade",
				fn: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runVersionUpgrade(ctx, t, c)
				},
				// This test doesn't like running on old versions because it upgrades to
				// the latest released version and then it tries to "head", where head is
				// the cockroach binary built from the branch on which the test is
				// running. If that branch corresponds to an older release, then upgrading
				// to head after 19.2 fails.
				minVersion: "v19.2.0",
				timeout:    30 * time.Minute,
			},
			{
				name: "generate-fixtures",
				skip: "This test is skipped in CI and can be run manually",
				fn: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					// Run this test to create a new fixture for the version upgrade test. This
					// is necessary after every release. For example, the day `master` becomes
					// the 20.2 release, this test will fail because it is missing a fixture for
					// 20.1; run the test (on 20.1) with the bool flipped to create the fixture.
					// Check it in (instructions will be logged below) and off we go.

					// The version to create/update the fixture for. Must be released (i.e.
					// can download it from the homepage); if that is not the case use the
					// empty string which uses the local cockroach binary. Make sure that
					// this binary then has the correct version. For example, to make a
					// "v20.2" fixture, you will need a binary that has "v20.2" in the
					// output of `./cockroach version`, and this process will end up
					// creating fixtures that have "v20.2" in them. This would be part
					// of tagging the master branch as v21.1 in the process of going
					// through the major release for v20.2.
					//
					// In the common case, one should populate this with the version (instead of
					// using the empty string) as this is the most straightforward and least
					// error-prone way to generate the fixtures.
					//
					// Please note that you do *NOT* need to update the fixtures in a patch
					// release. This only happens as part of preparing the master branch for the
					// next release. The release team runbooks, at time of writing, reflect
					// this.
					makeFixtureVersion := "22.1.0-beta.3" // for 22.1 release
					makeVersionFixtureAndFatal(ctx, t, c, makeFixtureVersion)
				},
				// This test doesn't like running on old versions because it upgrades to
				// the latest released version and then it tries to "head", where head is
				// the cockroach binary built from the branch on which the test is
				// running. If that branch corresponds to an older release, then upgrading
				// to head after 19.2 fails.
				minVersion: "v19.2.0",
				timeout:    30 * time.Minute,
			},
		},
		registry.OwnerServer: {
			{name: "build-info", fn: RunBuildInfo},
			{name: "build-analyze", fn: RunBuildAnalyze},
			{name: "cli/node-status", fn: runCLINodeStatus},
			{name: "cluster-init", fn: runClusterInit},
			{name: "rapid-restart", fn: runRapidRestart},
			{name: "status-server", fn: runStatusServer},
		},
	}
	tags := []string{"default", "quick"}
	specTemplate := registry.TestSpec{
		// NB: teamcity-post-failures.py relies on the acceptance tests
		// being named acceptance/<testname> and will avoid posting a
		// blank issue for the "acceptance" parent test. Make sure to
		// teach that script (if it's still used at that point) should
		// this naming scheme ever change (or issues such as #33519)
		// will be posted.
		Name:    "acceptance",
		Timeout: 10 * time.Minute,
		Tags:    tags,
	}

	for owner, tests := range testCases {
		for _, tc := range tests {
			tc := tc // copy for closure
			numNodes := 4
			if tc.numNodes != 0 {
				numNodes = tc.numNodes
			}

			spec := specTemplate
			spec.Owner = owner
			spec.Cluster = r.MakeClusterSpec(numNodes)
			spec.Skip = tc.skip
			spec.Name = specTemplate.Name + "/" + tc.name
			if tc.timeout != 0 {
				spec.Timeout = tc.timeout
			}
			spec.EncryptionSupport = tc.encryptionSupport
			spec.Run = func(ctx context.Context, t test.Test, c cluster.Cluster) {
				tc.fn(ctx, t, c)
			}
			r.Add(spec)
		}
	}
}
