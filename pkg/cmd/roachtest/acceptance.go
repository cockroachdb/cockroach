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
	"time"
)

func registerAcceptance(r *registry) {
	testCases := []struct {
		name       string
		fn         func(ctx context.Context, t *test, c *cluster)
		skip       string
		minVersion string
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
		{name: "version-upgrade", fn: runVersionUpgrade, minVersion: "v19.1.0"},
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
		Timeout: 10 * time.Minute,
		Tags:    tags,
		Cluster: makeClusterSpec(numNodes),
	}

	for _, tc := range testCases {
		tc := tc // copy for closure
		spec := specTemplate
		spec.Name = specTemplate.Name + "/" + tc.name
		spec.Run = func(ctx context.Context, t *test, c *cluster) {
			// TODO(andrei): !!! remove this wipe once the test runner starts doing it.
			c.Wipe(ctx)
			tc.fn(ctx, t, c)
		}
		r.Add(spec)
	}
}
