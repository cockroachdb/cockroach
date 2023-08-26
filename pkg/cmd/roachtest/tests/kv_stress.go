// Copyright 2023 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

// The goal of this test is to run the most vanilla setup possible and make sure
// it passes under a stressful workload. If you are tempted to change any of the
// start parameters, don't. Instead, change the code so this test passes with
// default parameters.o
// This simple test stresses splits while writing, allocator, network timeouts,
// AC impact on writes as well as transaction handling of lease moves under a
// high-latency cluster.
func runKvStress(ctx context.Context, t test.Test, c cluster.Cluster) {
	nodes := c.Spec().NodeCount - 1

	c.Put(ctx, t.Cockroach(), "./cockroach")
	settings := install.MakeClusterSettings()
	c.Start(ctx, t.L(), option.DefaultStartOpts(), settings, c.Range(1, nodes))

	// We test split behavior under stress, so don't pre-split.
	c.Run(ctx, c.Node(1), "./cockroach workload init kv")

	db := c.Conn(ctx, t.L(), 1)
	defer db.Close()

	require.NoError(t, WaitFor3XReplication(ctx, t, db))

	// Run a stressful KV workload for 5 minutes. This should pass. In a healthy
	// 6 node cluster, it can do ~5000 QPS, so mean latency is ~2s.
	runCmd := fmt.Sprintf("./cockroach workload run kv  --duration=5m --max-block-bytes 16384 --concurrency 10000 {pgurl:1-%d}", nodes)
	c.Run(ctx, c.Node(nodes+1), runCmd)

	// TODO(baptist): Verify we never have 0 QPS. For now no errors is enough.
}
