// Copyright 2018 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

func registerImportTPCC(r *testRegistry) {
	runImportTPCC := func(ctx context.Context, t *test, c *cluster, warehouses int) {
		c.Put(ctx, cockroach, "./cockroach")
		c.Put(ctx, workload, "./workload")
		t.Status("starting csv servers")
		c.Start(ctx, t)
		c.Run(ctx, c.All(), `./workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)

		t.Status("running workload")
		m := newMonitor(ctx, c)
		dul := NewDiskUsageLogger(c)
		m.Go(dul.Runner)
		hc := NewHealthChecker(c, c.All())
		m.Go(hc.Runner)

		m.Go(func(ctx context.Context) error {
			defer dul.Done()
			defer hc.Done()
			cmd := fmt.Sprintf(
				`./workload fixtures import tpcc --warehouses=%d --csv-server='http://localhost:8081'`,
				warehouses)
			c.Run(ctx, c.Node(1), cmd)
			return nil
		})
		m.Wait()
	}

	const warehouses = 1000
	for _, numNodes := range []int{4, 32} {
		r.Add(testSpec{
			Name:    fmt.Sprintf("import/tpcc/warehouses=%d/nodes=%d", warehouses, numNodes),
			Owner:   OwnerBulkIO,
			Cluster: makeClusterSpec(numNodes),
			Timeout: 5 * time.Hour,
			Run: func(ctx context.Context, t *test, c *cluster) {
				runImportTPCC(ctx, t, c, warehouses)
			},
		})
	}
	const geoWarehouses = 4000
	const geoZones = "europe-west2-b,europe-west4-b,asia-northeast1-b,us-west1-b"
	r.Add(testSpec{
		Skip:    "#37349 - OOMing",
		Name:    fmt.Sprintf("import/tpcc/warehouses=%d/geo", geoWarehouses),
		Owner:   OwnerBulkIO,
		Cluster: makeClusterSpec(8, cpu(16), geo(), zones(geoZones)),
		Timeout: 5 * time.Hour,
		Run: func(ctx context.Context, t *test, c *cluster) {
			runImportTPCC(ctx, t, c, geoWarehouses)
		},
	})
}

func registerImportTPCH(r *testRegistry) {
	for _, item := range []struct {
		nodes   int
		timeout time.Duration
	}{
		// TODO(dt): this test seems to have become slower as of 19.2. It previously
		// had 4, 8 and 32 node configurations with comments claiming they ran in in
		// 4-5h for 4 node and 3h for 8 node. As of 19.2, it seems to be timing out
		// -- potentially because 8 secondary indexes is worst-case for direct
		// ingestion and seems to cause a lot of compaction, but further profiling
		// is required to confirm this. Until then, the 4 and 32 node configurations
		// are removed (4 is too slow and 32 is pretty expensive) while 8-node is
		// given a 50% longer timeout (which running by hand suggests should be OK).
		// (10/30/19) The timeout was increased again to 8 hours.
		{8, 8 * time.Hour},
	} {
		item := item
		r.Add(testSpec{
			Name:    fmt.Sprintf(`import/tpch/nodes=%d`, item.nodes),
			Owner:   OwnerBulkIO,
			Cluster: makeClusterSpec(item.nodes),
			Timeout: item.timeout,
			Run: func(ctx context.Context, t *test, c *cluster) {
				c.Put(ctx, cockroach, "./cockroach")
				c.Start(ctx, t)
				conn := c.Conn(ctx, 1)
				if _, err := conn.Exec(`
					CREATE DATABASE csv;
					SET CLUSTER SETTING jobs.registry.leniency = '5m';
				`); err != nil {
					t.Fatal(err)
				}
				if _, err := conn.Exec(
					`SET CLUSTER SETTING kv.bulk_ingest.max_index_buffer_size = '2gb'`,
				); err != nil && !strings.Contains(err.Error(), "unknown cluster setting") {
					t.Fatal(err)
				}
				// Wait for all nodes to be ready.
				if err := retry.ForDuration(time.Second*30, func() error {
					var nodes int
					if err := conn.
						QueryRowContext(ctx, `select count(*) from crdb_internal.gossip_liveness where updated_at > now() - interval '8s'`).
						Scan(&nodes); err != nil {
						t.Fatal(err)
					} else if nodes != item.nodes {
						return errors.Errorf("expected %d nodes, got %d", item.nodes, nodes)
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				m := newMonitor(ctx, c)
				dul := NewDiskUsageLogger(c)
				m.Go(dul.Runner)
				hc := NewHealthChecker(c, c.All())
				m.Go(hc.Runner)

				// TODO(peter): This currently causes the test to fail because we see a
				// flurry of valid merges when the import finishes.
				//
				// m.Go(func(ctx context.Context) error {
				// 	// Make sure the merge queue doesn't muck with our import.
				// 	return verifyMetrics(ctx, c, map[string]float64{
				// 		"cr.store.queue.merge.process.success": 10,
				// 		"cr.store.queue.merge.process.failure": 10,
				// 	})
				// })

				m.Go(func(ctx context.Context) error {
					defer dul.Done()
					defer hc.Done()
					t.WorkerStatus(`running import`)
					defer t.WorkerStatus()
					_, err := conn.Exec(`
				IMPORT TABLE csv.lineitem
				CREATE USING 'gs://cockroach-fixtures/tpch-csv/schema/lineitem.sql'
				CSV DATA (
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.1',
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.2',
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.3',
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.4',
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.5',
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.6',
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.7',
				'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.8'
				) WITH  delimiter='|'
			`)
					return errors.Wrap(err, "import failed")
				})

				t.Status("waiting")
				m.Wait()
			},
		})
	}
}
