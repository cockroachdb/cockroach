// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"fmt"
	"strings"
)

type randomLoadBenchSpec struct {
	Nodes       int
	Ops         int
	Concurrency int
}

func registerSchemaChangeRandomLoad(r *testRegistry) {
	r.Add(testSpec{
		Name:    "schemachange/random-load",
		Owner:   OwnerSQLSchema,
		Cluster: makeClusterSpec(3),
		Run: func(ctx context.Context, t *test, c *cluster) {
			maxOps := 5000
			concurrency := 20
			if local {
				maxOps = 1000
				concurrency = 2
			}
			runSchemaChangeRandomLoad(ctx, t, c, maxOps, concurrency)
		},
	})

	// Run a few representative scbench specs in CI.
	registerRandomLoadBenchSpec(r, randomLoadBenchSpec{
		Nodes:       3,
		Ops:         2000,
		Concurrency: 1,
	})

	registerRandomLoadBenchSpec(r, randomLoadBenchSpec{
		Nodes:       3,
		Ops:         10000,
		Concurrency: 20,
	})
}

func registerRandomLoadBenchSpec(r *testRegistry, b randomLoadBenchSpec) {
	nameParts := []string{
		"scbench",
		"randomload",
		fmt.Sprintf("nodes=%d", b.Nodes),
		fmt.Sprintf("ops=%d", b.Ops),
		fmt.Sprintf("conc=%d", b.Concurrency),
	}
	name := strings.Join(nameParts, "/")

	r.Add(testSpec{
		Name:       name,
		Owner:      OwnerSQLSchema,
		Cluster:    makeClusterSpec(b.Nodes),
		MinVersion: "v20.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			runSchemaChangeRandomLoad(ctx, t, c, b.Ops, b.Concurrency)
		},
	})
}

func runSchemaChangeRandomLoad(ctx context.Context, t *test, c *cluster, maxOps, concurrency int) {
	loadNode := c.Node(1)
	roachNodes := c.Range(1, c.spec.NodeCount)
	t.Status("copying binaries")
	c.Put(ctx, cockroach, "./cockroach", roachNodes)
	c.Put(ctx, workload, "./workload", loadNode)

	t.Status("starting cockroach nodes")
	c.Start(ctx, t, roachNodes)
	c.Run(ctx, loadNode, "./workload init schemachange")

	runCmd := []string{
		"./workload run schemachange --verbose=1",
		// The workload is still in development and occasionally discovers schema
		// change errors so for now we don't fail on them but only on panics, server
		// crashes, deadlocks, etc.
		// TODO(spaskob): remove when https://github.com/cockroachdb/cockroach/issues/47430
		// is closed.
		"--tolerate-errors=true",
		// Save the histograms so that they can be reported to https://roachperf.crdb.dev/.
		" --histograms=" + perfArtifactsDir + "/stats.json",
		fmt.Sprintf("--max-ops %d", maxOps),
		fmt.Sprintf("--concurrency %d", concurrency),
	}
	t.Status("running schemachange workload")
	c.Run(ctx, loadNode, runCmd...)
}
