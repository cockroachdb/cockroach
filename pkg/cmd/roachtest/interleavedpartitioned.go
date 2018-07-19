// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package main

import (
	"context"
	"fmt"
)

func registerInterleaved(r *registry) {
	runInterleaved := func(
		ctx context.Context,
		t *test,
		c *cluster,
		east bool,
		sessions int,
		customersPerSession int,
		devicesPerSession int,
		variantsPerSession int,
		parametersPerSession int,
		queriesPerSession int,
		insertPercent int,
		insertLocalPercent int,
		retrievePercent int,
		retrieveLocalPercent int,
		updatePercent int,
		updateLocalPercent int,
		deletePercent int,
		deleteBatchSize int,
	) {
		nodes := c.nodes - 1
		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Node(nodes+1))
		c.Start(ctx, c.Range(1, nodes))

		t.Status("running workload")
		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			duration := " --duration " + ifLocal("10s", "10m")
			cmdEast := fmt.Sprintf(
				"./workload run interleavedpartitioned --init --east --sessions %d --customers-per-session %d --devices-per-session %d --variants-per-session %d --parameters-per-session %d "+
					"--insert-percent %d --insert-local-percent %d --retrieve-percent %d --retrieve-local-percent %d --update-percent %d --update-local-percent %d --delete-percent %d --delete-batch-size %d"+duration+" {pgurl:4-6} &",
				sessions,
				customersPerSession,
				devicesPerSession,
				variantsPerSession,
				parametersPerSession,
				insertPercent,
				insertLocalPercent,
				retrievePercent,
				retrieveLocalPercent,
				updatePercent,
				updateLocalPercent,
				deletePercent,
				deleteBatchSize,
			)

			cmdWest := fmt.Sprintf(
				"./workload run interleavedpartitioned --insert-percent %d --insert-local-percent %d --retrieve-percent %d --retrieve-local-percent %d --update-percent %d --update-local-percent %d --delete-percent %d --delete-batch-size %d"+duration+" {pgurl:1-3}",
				insertPercent,
				insertLocalPercent,
				retrievePercent,
				retrieveLocalPercent,
				updatePercent,
				updateLocalPercent,
				deletePercent,
				deleteBatchSize,
			)

			c.Run(ctx, c.Node(nodes+1), cmdEast)
			c.Run(ctx, c.Node(nodes+1), cmdWest)
			return nil
		})
		m.Wait()
	}

	r.Add(testSpec{
		Name:   "interleavedpartitioned",
		Nodes:  nodes(9, geo(), zones("us-west1-b,us-east4-b,us-central1-a")),
		Stable: true,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runInterleaved(ctx, t, c,
				true,  /*east*/
				10000, /*sessions*/
				2,     /*customersPerSession*/
				2,     /*devicesPerSession*/
				5,     /*variantsPerSession*/
				1,     /*parametersPerSession*/
				1,     /*queriesPerSession*/
				40,    /*insertPercent*/
				90,    /*insertLocalPercent*/
				20,    /*retrievePercent*/
				90,    /*retrieveLocalPercent*/
				20,    /*updatePercent*/
				90,    /*updateLocalPercent*/
				20,    /*deletePercent*/
				20,    /*deleteBatchSize*/
			)
		},
	})
}
