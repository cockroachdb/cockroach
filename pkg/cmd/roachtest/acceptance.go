// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/version"
)

func registerAcceptance(r *registry) {
	// The acceptance tests all share a cluster and run sequentially. In
	// local mode the acceptance tests should be configured to run within a
	// minute or so as these tests are run on every merge to master.

	testCases := []struct {
		name string
		fn   func(ctx context.Context, t *test, c *cluster)
		skip string
		// roachtest needs to be taught about MinVersion for subtests.
		// See https://github.com/cockroachdb/cockroach/issues/36752.
		//
		// minVersion string
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
		{name: "rapid-restart", fn: runRapidRestart},
		{name: "status-server", fn: runStatusServer},
		{
			name: "version-upgrade",
			fn:   runVersionUpgrade,
			// NB: this is hacked back in below.
			// minVersion: "v19.2.0",
		},
	}
	tags := []string{"default", "quick"}
	const numNodes = 4
	spec := testSpec{
		// NB: teamcity-post-failures.py relies on the acceptance tests
		// being named acceptance/<testname> and will avoid posting a
		// blank issue for the "acceptance" parent test. Make sure to
		// teach that script (if it's still used at that point) should
		// this naming scheme ever change (or issues such as #33519)
		// will be posted.
		Name:    "acceptance",
		Tags:    tags,
		Cluster: makeClusterSpec(numNodes),
	}

	for _, tc := range testCases {
		tc := tc
		minV := "v19.2.0-0"
		if tc.name == "version-upgrade" && !r.buildVersion.AtLeast(version.MustParse(minV)) {
			tc.skip = fmt.Sprintf("skipped on %s (want at least %s)", r.buildVersion, minV)
		}
		spec.SubTests = append(spec.SubTests, testSpec{
			Skip:    tc.skip,
			Name:    tc.name,
			Timeout: 10 * time.Minute,
			Tags:    tags,
			Run: func(ctx context.Context, t *test, c *cluster) {
				c.Wipe(ctx)
				tc.fn(ctx, t, c)
			},
		})
	}
	r.Add(spec)
}
