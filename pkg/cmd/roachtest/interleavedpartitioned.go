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
	"sync"
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
		deleteBatchSize int,
	) {
		nodes := c.nodes
		c.Put(ctx, cockroach, "./cockroach", c.Range(1, nodes))
		c.Put(ctx, workload, "./workload", c.Range(1, nodes))
		c.Start(ctx, c.Range(1, nodes))

		m := newMonitor(ctx, c, c.Range(1, nodes))
		m.Go(func(ctx context.Context) error {
			duration := " --duration " + ifLocal("10s", "10m")

			// Just to initialize the database
			cmdInit := "./workload run interleavedpartitioned --init --east --sessions 0" +
				" --insert-percent 0 --retrieve-percent 100 --update-percent 0 --delete-percent 0 --duration 5s {pgurl:4}"

			cmdEast := fmt.Sprintf(
				"./workload run interleavedpartitioned --customers-per-session %d --devices-per-session %d --variants-per-session %d --parameters-per-session %d --queries-per-session %d --insert-percent %d --insert-local-percent %d --retrieve-percent %d --retrieve-local-percent %d --update-percent %d --update-local-percent %d --delete-percent 0"+duration+" {pgurl:4-6}",
				customersPerSession,
				devicesPerSession,
				variantsPerSession,
				parametersPerSession,
				queriesPerSession,
				insertPercent,
				insertLocalPercent,
				retrievePercent,
				retrieveLocalPercent,
				updatePercent,
				updateLocalPercent,
			)

			cmdWest := fmt.Sprintf(
				"./workload run interleavedpartitioned --customers-per-session %d --devices-per-session %d --variants-per-session %d --parameters-per-session %d --queries-per-session %d --insert-percent %d --insert-local-percent %d --retrieve-percent %d --retrieve-local-percent %d --update-percent %d --update-local-percent %d --delete-percent 0"+duration+" {pgurl:1-3}",
				customersPerSession,
				devicesPerSession,
				variantsPerSession,
				parametersPerSession,
				queriesPerSession,
				insertPercent,
				insertLocalPercent,
				retrievePercent,
				retrieveLocalPercent,
				updatePercent,
				updateLocalPercent,
			)

			cmdCentral := fmt.Sprintf(
				"./workload run interleavedpartitioned --insert-percent 0 --retrieve-percent 0 --update-percent 0  --delete-percent 100 --delete-batch-size %d"+duration+" {pgurl:7-9}",
				deleteBatchSize,
			)

			t.Status("initializing database")
			c.Run(ctx, c.Node(1), cmdInit)
			var wg sync.WaitGroup
			wg.Add(3)
			t.Status("running workload jobs")
			go func() {
				c.Run(ctx, c.Node(1), cmdWest)
				wg.Done()
			}()
			go func() {
				c.Run(ctx, c.Node(4), cmdEast)
				wg.Done()
			}()
			go func() {
				c.Run(ctx, c.Node(7), cmdCentral)
				wg.Done()
			}()

			// This will only finish when all the workload jobs have finished.
			wg.Wait()
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
				80,    /*insertPercent*/
				100,   /*insertLocalPercent*/
				10,    /*retrievePercent*/
				100,   /*retrieveLocalPercent*/
				10,    /*updatePercent*/
				100,   /*updateLocalPercent*/
				20,    /*deleteBatchSize*/
			)
		},
	})
}
