// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/errors"
)

func registerAcceptance(r registry.Registry) {
	cloudsWithoutServiceRegistration := registry.AllClouds.Remove(registry.CloudsWithServiceRegistration)

	testCases := map[registry.Owner][]struct {
		name               string
		fn                 func(ctx context.Context, t test.Test, c cluster.Cluster)
		skip               string
		numNodes           int
		nodeRegions        []string
		timeout            time.Duration
		encryptionSupport  registry.EncryptionSupport
		defaultLeases      bool
		randomized         bool
		workloadNode       bool
		incompatibleClouds registry.CloudSet
		suites             []string
	}{
		// NOTE: acceptance tests are lightweight tests that run as part
		// of CI. As such, they must:
		//
		// 1. finish quickly
		// 2. be compatible with roachtest local
		//
		// If you are adding a test that does not satisfy either of these
		// properties, please register it separately (not as an acceptance
		// test).
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
				suites:        []string{registry.MixedVersion},
			},
		},
		registry.OwnerDisasterRecovery: {
			{
				name:               "c2c",
				fn:                 runAcceptanceClusterReplication,
				numNodes:           3,
				incompatibleClouds: cloudsWithoutServiceRegistration,
				workloadNode:       true,
			},
			{
				name:               "multitenant",
				fn:                 runAcceptanceMultitenant,
				timeout:            time.Minute * 20,
				incompatibleClouds: cloudsWithoutServiceRegistration,
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
				suites:        []string{registry.MixedVersion},
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

			var extraOptions []spec.Option
			if tc.nodeRegions != nil {
				// Sanity: Ensure the region counts are sane.
				if len(tc.nodeRegions) != numNodes {
					panic(errors.AssertionFailedf("region list doesn't match number of nodes"))
				}
				extraOptions = append(extraOptions, spec.Geo())
				extraOptions = append(extraOptions, spec.GCEZones(strings.Join(tc.nodeRegions, ",")))
			}

			if tc.workloadNode {
				extraOptions = append(extraOptions, spec.WorkloadNode())
			}

			if tc.incompatibleClouds.IsInitialized() && tc.incompatibleClouds.Contains(spec.Local) {
				panic(errors.AssertionFailedf(
					"acceptance tests must be able to run on roachtest local, but %q is incompatible",
					tc.name,
				))
			}
			suites := append([]string{registry.Nightly, registry.Quick, registry.Acceptance}, tc.suites...)
			testSpec := registry.TestSpec{
				Name:              "acceptance/" + tc.name,
				Owner:             owner,
				Cluster:           r.MakeClusterSpec(numNodes, extraOptions...),
				Skip:              tc.skip,
				EncryptionSupport: tc.encryptionSupport,
				Timeout:           10 * time.Minute,
				CompatibleClouds:  registry.AllClouds.Remove(tc.incompatibleClouds),
				Suites:            registry.Suites(suites...),
				Randomized:        tc.randomized,
			}

			if tc.timeout != 0 {
				testSpec.Timeout = tc.timeout
			}
			if !tc.defaultLeases {
				testSpec.Leases = registry.MetamorphicLeases
			}
			testSpec.Run = func(ctx context.Context, t test.Test, c cluster.Cluster) {
				tc.fn(ctx, t, c)
			}
			r.Add(testSpec)
		}
	}
}
