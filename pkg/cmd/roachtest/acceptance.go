// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"time"
)

func registerAcceptance(r *testRegistry) {
	testCases := []struct {
		name       string
		fn         func(ctx context.Context, t *test, c *cluster)
		skip       string
		minVersion string
		timeout    time.Duration
	}{
		// Sorted. Please keep it that way.
		{name: "bank/cluster-recovery", fn: runBankClusterRecovery},
		{name: "bank/node-restart", fn: runBankNodeRestart},
		{
			name: "bank/zerosum-splits", fn: runBankNodeZeroSum,
			skip: "https://github.com/cockroachdb/cockroach/issues/33683 (runs into " +
				" various errors during its rebalances, see isExpectedRelocateError)",
		},
		// {"bank/zerosum-restart", runBankZeroSumRestart},
		{name: "build-info", fn: runBuildInfo},
		{name: "build-analyze", fn: runBuildAnalyze},
		{name: "cli/node-status", fn: runCLINodeStatus},
		{name: "decommission", fn: runDecommissionAcceptance},
		{name: "cluster-init", fn: runClusterInit},
		{name: "event-log", fn: runEventLog},
		{name: "gossip/peerings", fn: runGossipPeerings},
		{name: "gossip/restart", fn: runGossipRestart},
		{name: "gossip/restart-node-one", fn: runGossipRestartNodeOne},
		{name: "gossip/locality-address", fn: runCheckLocalityIPAddress},
		{
			name:       "multitenant",
			minVersion: "v20.2.0", // multitenancy is introduced in this cycle
			fn:         runAcceptanceMultitenant,
		},
		{name: "rapid-restart", fn: runRapidRestart},
		{
			name: "many-splits", fn: runManySplits,
			minVersion: "v19.2.0", // SQL syntax unsupported on 19.1.x
		},
		{name: "status-server", fn: runStatusServer},
		{
			name: "version-upgrade",
			fn: func(ctx context.Context, t *test, c *cluster) {
				runVersionUpgrade(ctx, t, c, r.buildVersion)
			},
			// This test doesn't like running on old versions because it upgrades to
			// the latest released version and then it tries to "head", where head is
			// the cockroach binary built from the branch on which the test is
			// running. If that branch corresponds to an older release, then upgrading
			// to head after 19.2 fails.
			minVersion: "v19.2.0",
			timeout:    30 * time.Minute},
	}
	tags := []string{"default", "quick"}
	const numNodes = 4
	specTemplate := testSpec{
		// NB: teamcity-post-failures.py relies on the acceptance tests
		// being named acceptance/<testname> and will avoid posting a
		// blank issue for the "acceptance" parent test. Make sure to
		// teach that script (if it's still used at that point) should
		// this naming scheme ever change (or issues such as #33519)
		// will be posted.
		Name:    "acceptance",
		Owner:   OwnerKV,
		Timeout: 10 * time.Minute,
		Tags:    tags,
		Cluster: makeClusterSpec(numNodes),
	}

	for _, tc := range testCases {
		tc := tc // copy for closure
		spec := specTemplate
		spec.Skip = tc.skip
		spec.Name = specTemplate.Name + "/" + tc.name
		spec.MinVersion = tc.minVersion
		if tc.timeout != 0 {
			spec.Timeout = tc.timeout
		}
		spec.Run = func(ctx context.Context, t *test, c *cluster) {
			tc.fn(ctx, t, c)
		}
		r.Add(spec)
	}
}
