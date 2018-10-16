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
	specTemplate := testSpec{
		Name:               "acceptance",
		Nodes:              nodes(4),
		Stable:             true, // DO NOT COPY to new tests
		Timeout:            10 * time.Minute,
		ClusterReusePolicy: Any,
	}

	testCases := []struct {
		name string
		fn   func(ctx context.Context, t *test, c *cluster)
	}{
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
