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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/errors"
)

func registerAcceptance(r registry.Registry) {
	testCases := map[registry.Owner][]struct {
		name               string
		fn                 func(ctx context.Context, t test.Test, c cluster.Cluster)
		skip               string
		numNodes           int
		nodeRegions        []string
		timeout            time.Duration
		encryptionSupport  registry.EncryptionSupport
		defaultLeases      bool
		requiresLicense    bool
		nativeLibs         []string
		incompatibleClouds []string // Already assumes AWS is incompatible.
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
				nativeLibs:    registry.LibGEOS,
			},
		},
		registry.OwnerDisasterRecovery: {
			{
				name:     "c2c",
				fn:       runAcceptanceClusterReplication,
				numNodes: 3,
			},
			{
				name:               "multitenant",
				fn:                 runAcceptanceMultitenant,
				timeout:            time.Minute * 20,
				incompatibleClouds: []string{spec.Azure}, // Requires service registration.
			},
		},
		registry.OwnerSQLFoundations: {
			{
				name:          "validate-system-schema-after-version-upgrade",
				fn:            runValidateSystemSchemaAfterVersionUpgrade,
				timeout:       30 * time.Minute,
				defaultLeases: true,
				numNodes:      1,
			},
			{
				name:     "mismatched-locality",
				fn:       runMismatchedLocalityTest,
				numNodes: 3,
			},
			{
				name:     "multitenant-multiregion",
				fn:       runAcceptanceMultitenantMultiRegion,
				timeout:  20 * time.Minute,
				numNodes: 9,
				nodeRegions: []string{"us-west1-b", "us-west1-b", "us-west1-b",
					"us-west1-b", "us-west1-b", "us-west1-b",
					"us-east1-b", "us-east1-b", "us-east1-b"},
				requiresLicense:    true,
				incompatibleClouds: []string{spec.Local, spec.Azure}, // Requires service registration.
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

			testSpec := registry.TestSpec{
				Name:              "acceptance/" + tc.name,
				Owner:             owner,
				Cluster:           r.MakeClusterSpec(numNodes, extraOptions...),
				Skip:              tc.skip,
				EncryptionSupport: tc.encryptionSupport,
				Timeout:           10 * time.Minute,
				CompatibleClouds:  registry.AllExceptAWS.Remove(tc.incompatibleClouds...),
				Suites:            registry.Suites(registry.Nightly, registry.Quick, registry.Acceptance),
				RequiresLicense:   tc.requiresLicense,
			}

			if tc.timeout != 0 {
				testSpec.Timeout = tc.timeout
			}
			if !tc.defaultLeases {
				testSpec.Leases = registry.MetamorphicLeases
			}
			if len(tc.nativeLibs) > 0 {
				testSpec.NativeLibs = tc.nativeLibs
			}
			testSpec.Run = func(ctx context.Context, t test.Test, c cluster.Cluster) {
				tc.fn(ctx, t, c)
			}
			r.Add(testSpec)
		}
	}
}
