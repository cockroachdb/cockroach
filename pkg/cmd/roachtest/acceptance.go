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
)

func registerAcceptance(r *registry) {
	// The acceptance tests all share a cluster and run sequentially. In
	// local mode the acceptance tests should be configured to run within a
	// minute or so as these tests are run on every merge to master.

	testCases := map[int][]struct {
		name string
		fn   func(ctx context.Context, t *test, c *cluster)
	}{
		4: {
			// Sorted. Please keep it that way.
			{"bank/cluster-recovery", runBankClusterRecovery},
			{"bank/node-restart", runBankNodeRestart},
			{"bank/zerosum-splits", runBankNodeZeroSum},
			//TODO(masha) This is flaking for same reasons as bank/node-restart
			// enable after #30064 is fixed.
			//{"bank/zerosum-restart", runBankZeroSumRestart},
			{"build-info", runBuildInfo},
			{"cli/node-status", runCLINodeStatus},
			{"decommission", runDecommissionAcceptance},
			{"cluster-init", runClusterInit},
			{"event-log", runEventLog},
			{"gossip/peerings", runGossipPeerings},
			{"gossip/restart", runGossipRestart},
			{"gossip/restart-node-one", runGossipRestartNodeOne},
			{"gossip/locality-address", runCheckLocalityIPAddress},
			{"rapid-restart", runRapidRestart},
			{"status-server", runStatusServer},
			{"version-upgrade", runVersionUpgrade},
		},
	}
	tags := []string{"default", "quick"}
	for numNodes, cases := range testCases {
		spec := testSpec{
			Name:  fmt.Sprintf("acceptance/nodes=%d", numNodes),
			Tags:  tags,
			Nodes: nodes(numNodes),
		}

		for _, tc := range cases {
			tc := tc
			spec.SubTests = append(spec.SubTests, testSpec{
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
}
