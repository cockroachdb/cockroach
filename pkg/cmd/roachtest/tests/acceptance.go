// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		defaultLeases     bool
		randomized        bool
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
				name: "many-splits", fn: runManySplits,
				encryptionSupport: registry.EncryptionMetamorphic,
			},
			{name: "cli/node-status", fn: runCLINodeStatus},
			{name: "cluster-init", fn: runClusterInit},
			{name: "rapid-restart", fn: runRapidRestart},
		},
		registry.OwnerObservability: {
			{name: "status-server", fn: runStatusServer},
		},
		registry.OwnerDevInf: {
			{name: "build-info", fn: RunBuildInfo},
			{name: "build-analyze", fn: RunBuildAnalyze},
		},
		registry.OwnerTestEng: {
			{
				name:          "version-upgrade",
				fn:            runVersionUpgrade,
				timeout:       2 * time.Hour, // actually lower in local runs; see `runVersionUpgrade`
				defaultLeases: true,
				randomized:    true,
			},
		},
		registry.OwnerDisasterRecovery: {
			{
				name:     "c2c",
				fn:       runAcceptanceClusterReplication,
				numNodes: 3,
			},
			{
				name: "multitenant",
				fn:   runAcceptanceMultitenant,
			},
		},
		registry.OwnerSQLFoundations: {
			{
				name:          "validate-system-schema-after-version-upgrade",
				fn:            runValidateSystemSchemaAfterVersionUpgrade,
				timeout:       60 * time.Minute,
				defaultLeases: true,
				randomized:    true,
				numNodes:      1,
			},
			{
				name:     "mismatched-locality",
				fn:       runMismatchedLocalityTest,
				numNodes: 3,
			},
		},
	}
	for owner, tests := range testCases {
		for _, tc := range tests {
			tc := tc // copy for closure
			numNodes := 4
			if tc.numNodes != 0 {
				numNodes = tc.numNodes
			}

			spec := registry.TestSpec{
				Name:              "acceptance/" + tc.name,
				Owner:             owner,
				Cluster:           r.MakeClusterSpec(numNodes),
				Skip:              tc.skip,
				EncryptionSupport: tc.encryptionSupport,
				Timeout:           10 * time.Minute,
				CompatibleClouds:  registry.AllExceptAWS,
				Suites:            registry.Suites(registry.Nightly, registry.Quick),
				Randomized:        tc.randomized,
			}

			if tc.timeout != 0 {
				spec.Timeout = tc.timeout
			}
			if !tc.defaultLeases {
				spec.Leases = registry.MetamorphicLeases
			}
			spec.Run = func(ctx context.Context, t test.Test, c cluster.Cluster) {
				tc.fn(ctx, t, c)
			}
			r.Add(spec)
		}
	}
}
