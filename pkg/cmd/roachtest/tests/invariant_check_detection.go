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

	// End-to-end test: triggers a real log.Fatal and verifies the marker
	// file is written.
	r.Add(registry.TestSpec{
		CompatibleClouds: registry.AllClouds,
		Name:             "invariant-check-detection/fatal-exit/e2e",
		Owner:            registry.OwnerTestEng,
		Suites:           registry.ManualOnly,
		Cluster:          r.MakeClusterSpec(1, spec.CPU(4), spec.ReuseNone(), spec.VolumeSize(100), spec.VolumeType("pd-ssd")),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runFatalExitE2E(ctx, t, c)
		},
	})

	// Tests for fatal exit marker file detection.
	for _, tc := range []struct {
		name string
		msg  string
	}{
		{
			name: "local-corruption",
			msg:  "local corruption detected: pebble: file 008647: block 35700474/12980: crc32c checksum mismatch 0 != f99fc415",
		},
		{
			name: "replica-corruption",
			msg:  "replica is corrupted: replica corruption (processed=true): unexpected inconsistency",
		},
		{
			name: "disk-stall-skipped",
			msg:  "disk stall detected: pebble: sync /mnt/data1/cockroach/000042.log: duration 25s",
		},
	} {
		tc := tc
		r.Add(registry.TestSpec{
			CompatibleClouds: registry.AllClouds,
			Name:             fmt.Sprintf("invariant-check-detection/fatal-exit/%s", tc.name),
			Owner:            registry.OwnerTestEng,
			Suites:           registry.ManualOnly,
			Cluster:          r.MakeClusterSpec(1, spec.CPU(4), spec.ReuseNone(), spec.VolumeSize(100), spec.VolumeType("pd-ssd")),
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runFatalExitMarkerDetection(ctx, t, c, tc.msg)
			},
		})
	}
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

// runFatalExitMarkerDetection plants a _FATAL_EXIT.txt marker file
// and lets teardownTest -> maybeSaveClusterDueToInvariantProblems
// detect it.
func runFatalExitMarkerDetection(ctx context.Context, t test.Test, c cluster.Cluster, msg string) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(1))
	require.NoError(t, c.RunE(ctx, option.WithNodes(c.Node(1)),
		"mkdir -p /mnt/data1/cockroach/auxiliary"))
	require.NoError(t, c.PutString(ctx, msg,
		"/mnt/data1/cockroach/auxiliary/_FATAL_EXIT.txt", 0644, c.Node(1)))
}

// runFatalExitE2E triggers a real log.Fatal via SQL and verifies that
// the _FATAL_EXIT.txt marker file was written with the expected
// message.
func runFatalExitE2E(ctx context.Context, t test.Test, c cluster.Cluster) {
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(1))

	// Trigger a real log.Fatal via SQL. This will kill the node.
	conn := c.Conn(ctx, t.L(), 1)
	defer conn.Close()
	_, _ = conn.ExecContext(ctx,
		"SELECT crdb_internal.force_log_fatal('fatal-exit-e2e-test: simulated corruption')")

	// The node should be dead now. Verify the marker file was written.
	result, err := c.RunWithDetailsSingleNode(ctx, t.L(), option.WithNodes(c.Node(1)),
		"cat /mnt/data1/cockroach/auxiliary/_FATAL_EXIT.txt 2>/dev/null")
	require.NoError(t, err)
	require.Contains(t, result.Stdout, "fatal-exit-e2e-test: simulated corruption",
		"marker file should contain the fatal message")
	t.L().Printf("marker file content: %s", result.Stdout)
}
