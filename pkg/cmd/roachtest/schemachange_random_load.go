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
)

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
		"./workload run",
		fmt.Sprintf("schemachange  --concurrency %d --max-ops %d --verbose=1", maxOps,
			concurrency),
		fmt.Sprintf("{pgurl:1-%d}", c.spec.NodeCount),
	}
	t.Status("running schemachange workload")
	c.Run(ctx, loadNode, runCmd...)
}
