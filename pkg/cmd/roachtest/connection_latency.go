// Copyright 2021 The Cockroach Authors.
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

func registerConnectionLatencyTest(r *testRegistry) {
	numNodes := 3
	r.Add(testSpec{
		MinVersion: "v20.1.0",
		Name:       "connection_latency",
		Owner:      OwnerSQLExperience,
		Cluster:    makeClusterSpec(numNodes),
		Run: func(ctx context.Context, t *test, c *cluster) {
			c.Put(ctx, cockroach, "./cockroach")
			c.Put(ctx, workload, "./workload")
			c.Start(ctx, t)
			c.Run(ctx, c.Node(1), `./workload init connectionlatency`)
			c.Run(ctx, c.Node(1),
				fmt.Sprintf(
					`./workload run connectionlatency --duration 30s --histograms=%s/stats.json`,
					perfArtifactsDir),
			)
		},
	})
}
