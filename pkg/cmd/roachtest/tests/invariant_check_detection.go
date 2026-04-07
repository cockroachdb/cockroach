// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func registerInvariantCheckDetection(r registry.Registry) {
	// Tests for maybeSaveClusterDueToInvariantProblems. These don't verify
	// that everything works as it should, but they can be run to verify
	// manually that the cluster is saved correctly and the log output is
	// helpful.

	for _, failed := range []bool{false, true} {
		r.Add(registry.TestSpec{
			CompatibleClouds: registry.AllClouds,
			Name:             fmt.Sprintf("invariant-check-detection/failed=%t", failed),
			Owner:            registry.OwnerTestEng,
			Suites:           registry.ManualOnly,
			Cluster:          r.MakeClusterSpec(1, spec.CPU(4), spec.ReuseNone(), spec.VolumeSize(100), spec.VolumeType("pd-ssd")),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runInvariantCheckDetection(ctx, t, c, failed)
			},
		})
	}

	// Test that local corruption (checksum mismatch) is also detected.
	r.Add(registry.TestSpec{
		CompatibleClouds: registry.AllClouds,
		Name:             "invariant-check-detection/local-corruption",
		Owner:            registry.OwnerTestEng,
		Suites:           registry.ManualOnly,
		Cluster:          r.MakeClusterSpec(1, spec.CPU(4), spec.ReuseNone(), spec.VolumeSize(100), spec.VolumeType("pd-ssd")),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runLocalCorruptionDetection(ctx, t, c)
		},
	})
}

func runInvariantCheckDetection(ctx context.Context, t test.Test, c cluster.Cluster, failed bool) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(1))
	require.NoError(t, c.PutString(ctx, `
foo br baz
F250502 11:37:20.387424 1036 raft/raft.go:2411 ⋮ [T1,Vsystem,n1,s1,r155/1:‹/Table/113/1/{43/578…-51/201…}›] 80 match(30115) is out of range [lastIndex(30114)]. Was the raft log corrupted, truncated, or lost?
asdasds
`, "logs/foo.log", 0644, c.Node(1)))
	if failed {
		t.Error("boom")
	}
}

// runLocalCorruptionDetection plants a fake "local corruption detected"
// fatal log line and lets teardownTest -> maybeSaveClusterDueToInvariantProblems
// detect it. The test is expected to fail — maybeSaveClusterDueToInvariantProblems
// calls t.Error when it preserves the cluster, which marks the test as failed.
func runLocalCorruptionDetection(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(1))
	require.NoError(t, c.PutString(ctx, `
F260402 03:11:00.515510 2215881 storage/pebble.go:1436 ⋮ [n3,s3,pebble] 2364  local corruption detected: pebble: file 039091: block 8375813/30902: ‹crc32c› checksum mismatch b136961c != ed105766
`, "logs/cockroach.log", 0644, c.Node(1)))
}
