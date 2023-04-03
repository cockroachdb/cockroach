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
			{name: "reset-quorum", fn: runResetQuorum, numNodes: 8},
			{
				name: "many-splits", fn: runManySplits,
				encryptionSupport: registry.EncryptionMetamorphic,
			},
			{name: "cli/node-status", fn: runCLINodeStatus},
			{name: "cluster-init", fn: runClusterInit},
			{name: "rapid-restart", fn: runRapidRestart},
		},
		registry.OwnerMultiTenant: {
			{
				name: "multitenant",
				fn:   runAcceptanceMultitenant,
			},
		},
		registry.OwnerObsInf: {
			{name: "status-server", fn: runStatusServer},
		},
		registry.OwnerDevInf: {
			{name: "build-info", fn: RunBuildInfo},
			{name: "build-analyze", fn: RunBuildAnalyze},
		},
		registry.OwnerTestEng: {
			{
				name:    "version-upgrade",
				fn:      runVersionUpgrade,
				timeout: 30 * time.Minute,
			},
		},
		registry.OwnerDisasterRecovery: {
			{
				name:     "c2c",
				fn:       runAcceptanceClusterReplication,
				numNodes: 3,
			},
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
