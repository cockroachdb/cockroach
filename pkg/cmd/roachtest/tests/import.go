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
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

func registerImportNodeShutdown(r registry.Registry) {
	getImportRunner := func(ctx context.Context, gatewayNode int) jobStarter {
		startImport := func(c cluster.Cluster) (jobID string, err error) {
			// partsupp is 11.2 GiB.
			tableName := "partsupp"
			if c.IsLocal() {
				// part is 2.264 GiB.
				tableName = "part"
			}
			importStmt := fmt.Sprintf(`
				IMPORT TABLE %[1]s
				CREATE USING 'gs://cockroach-fixtures/tpch-csv/schema/%[1]s.sql?AUTH=implicit'
				CSV DATA (
				'gs://cockroach-fixtures/tpch-csv/sf-100/%[1]s.tbl.1?AUTH=implicit',
				'gs://cockroach-fixtures/tpch-csv/sf-100/%[1]s.tbl.2?AUTH=implicit',
				'gs://cockroach-fixtures/tpch-csv/sf-100/%[1]s.tbl.3?AUTH=implicit',
				'gs://cockroach-fixtures/tpch-csv/sf-100/%[1]s.tbl.4?AUTH=implicit',
				'gs://cockroach-fixtures/tpch-csv/sf-100/%[1]s.tbl.5?AUTH=implicit',
				'gs://cockroach-fixtures/tpch-csv/sf-100/%[1]s.tbl.6?AUTH=implicit',
				'gs://cockroach-fixtures/tpch-csv/sf-100/%[1]s.tbl.7?AUTH=implicit',
				'gs://cockroach-fixtures/tpch-csv/sf-100/%[1]s.tbl.8?AUTH=implicit'
				) WITH  delimiter='|', detached
			`, tableName)
			gatewayDB := c.Conn(ctx, gatewayNode)
			defer gatewayDB.Close()

			err = gatewayDB.QueryRowContext(ctx, importStmt).Scan(&jobID)
			return
		}

		return startImport
	}

	r.Add(registry.TestSpec{
		Name:    "import/nodeShutdown/worker",
		Owner:   registry.OwnerBulkIO,
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Put(ctx, t.Cockroach(), "./cockroach")
			c.Start(ctx)
			gatewayNode := 2
			nodeToShutdown := 3
			startImport := getImportRunner(ctx, gatewayNode)

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, startImport)
		},
	})
	r.Add(registry.TestSpec{
		Name:    "import/nodeShutdown/coordinator",
		Owner:   registry.OwnerBulkIO,
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Put(ctx, t.Cockroach(), "./cockroach")
			c.Start(ctx)
			gatewayNode := 2
			nodeToShutdown := 2
			startImport := getImportRunner(ctx, gatewayNode)

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, startImport)
		},
	})
}

func registerImportTPCC(r registry.Registry) {
	runImportTPCC := func(ctx context.Context, t test.Test, c cluster.Cluster, testName string,
		timeout time.Duration, warehouses int) {
		// Randomize starting with encryption-at-rest enabled.
		c.EncryptAtRandom(true)
		c.Put(ctx, t.Cockroach(), "./cockroach")
		c.Put(ctx, t.DeprecatedWorkload(), "./workload")
		t.Status("starting csv servers")
		c.Start(ctx)
		c.Run(ctx, c.All(), `./workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)

		t.Status("running workload")
		m := c.NewMonitor(ctx)
		dul := NewDiskUsageLogger(t, c)
		m.Go(dul.Runner)
		hc := NewHealthChecker(t, c, c.All())
		m.Go(hc.Runner)

		tick := initBulkJobPerfArtifacts(ctx, t, testName, timeout)
		workloadStr := `./cockroach workload fixtures import tpcc --warehouses=%d --csv-server='http://localhost:8081'`
		m.Go(func(ctx context.Context) error {
			defer dul.Done()
			defer hc.Done()
			cmd := fmt.Sprintf(workloadStr, warehouses)
			// Tick once before starting the import, and once after to capture the
			// total elapsed time. This is used by roachperf to compute and display
			// the average MB/sec per node.
			tick()
			c.Run(ctx, c.Node(1), cmd)
			tick()

			// Upload the perf artifacts to any one of the nodes so that the test
			// runner copies it into an appropriate directory path.
			if err := c.PutE(ctx, t.L(), t.PerfArtifactsDir(), t.PerfArtifactsDir(), c.Node(1)); err != nil {
				log.Errorf(ctx, "failed to upload perf artifacts to node: %s", err.Error())
			}
			return nil
		})
		m.Wait()
	}

	const warehouses = 1000
	for _, numNodes := range []int{4, 32} {
		testName := fmt.Sprintf("import/tpcc/warehouses=%d/nodes=%d", warehouses, numNodes)
		timeout := 5 * time.Hour
		r.Add(registry.TestSpec{
			Name:    testName,
			Owner:   registry.OwnerBulkIO,
			Cluster: r.MakeClusterSpec(numNodes),
			Timeout: timeout,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runImportTPCC(ctx, t, c, testName, timeout, warehouses)
			},
		})
	}
	const geoWarehouses = 4000
	const geoZones = "europe-west2-b,europe-west4-b,asia-northeast1-b,us-west1-b"
	r.Add(registry.TestSpec{
		Name:    fmt.Sprintf("import/tpcc/warehouses=%d/geo", geoWarehouses),
		Owner:   registry.OwnerBulkIO,
		Cluster: r.MakeClusterSpec(8, spec.CPU(16), spec.Geo(), spec.Zones(geoZones)),
		Timeout: 5 * time.Hour,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runImportTPCC(ctx, t, c, fmt.Sprintf("import/tpcc/warehouses=%d/geo", geoWarehouses),
				5*time.Hour, geoWarehouses)
		},
	})
}

func registerImportTPCH(r registry.Registry) {
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
		r.Add(registry.TestSpec{
			Name:    fmt.Sprintf(`import/tpch/nodes=%d`, item.nodes),
			Owner:   registry.OwnerBulkIO,
			Cluster: r.MakeClusterSpec(item.nodes),
			Timeout: item.timeout,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				tick := initBulkJobPerfArtifacts(ctx, t, t.Name(), item.timeout)

				// Randomize starting with encryption-at-rest enabled.
				c.EncryptAtRandom(true)
				c.Put(ctx, t.Cockroach(), "./cockroach")
				c.Start(ctx)
				conn := c.Conn(ctx, 1)
				if _, err := conn.Exec(`CREATE DATABASE csv;`); err != nil {
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
				m := c.NewMonitor(ctx)
				dul := NewDiskUsageLogger(t, c)
				m.Go(dul.Runner)
				hc := NewHealthChecker(t, c, c.All())
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

					// Tick once before starting the import, and once after to capture the
					// total elapsed time. This is used by roachperf to compute and display
					// the average MB/sec per node.
					tick()
					_, err := conn.Exec(`
						IMPORT TABLE csv.lineitem
						CREATE USING 'gs://cockroach-fixtures/tpch-csv/schema/lineitem.sql?AUTH=implicit'
						CSV DATA (
						'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.1?AUTH=implicit',
						'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.2?AUTH=implicit',
						'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.3?AUTH=implicit',
						'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.4?AUTH=implicit',
						'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.5?AUTH=implicit',
						'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.6?AUTH=implicit',
						'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.7?AUTH=implicit',
						'gs://cockroach-fixtures/tpch-csv/sf-100/lineitem.tbl.8?AUTH=implicit'
						) WITH  delimiter='|'
					`)
					if err != nil {
						return errors.Wrap(err, "import failed")
					}
					tick()

					// Upload the perf artifacts to any one of the nodes so that the test
					// runner copies it into an appropriate directory path.
					if err := c.PutE(ctx, t.L(), t.PerfArtifactsDir(), t.PerfArtifactsDir(), c.Node(1)); err != nil {
						log.Errorf(ctx, "failed to upload perf artifacts to node: %s", err.Error())
					}
					return nil
				})

				t.Status("waiting")
				m.Wait()
			},
		})
	}
}

func successfulImportStep(warehouses, nodeID int) versionStep {
	return func(ctx context.Context, t test.Test, u *versionUpgradeTest) {
		u.c.Run(ctx, u.c.Node(nodeID), tpccImportCmd(warehouses))
	}
}

func runImportMixedVersion(
	ctx context.Context, t test.Test, c cluster.Cluster, warehouses int, predecessorVersion string,
) {
	// An empty string means that the cockroach binary specified by flag
	// `cockroach` will be used.
	const mainVersion = ""
	roachNodes := c.All()

	t.Status("starting csv servers")

	u := newVersionUpgradeTest(c,
		uploadAndStartFromCheckpointFixture(roachNodes, predecessorVersion),
		waitForUpgradeStep(roachNodes),
		preventAutoUpgradeStep(1),

		// Upgrade some of the nodes.
		binaryUpgradeStep(c.Node(1), mainVersion),
		binaryUpgradeStep(c.Node(2), mainVersion),

		successfulImportStep(warehouses, 1 /* nodeID */),
	)
	u.run(ctx, t)
}

func registerImportMixedVersion(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "import/mixed-versions",
		Owner: registry.OwnerBulkIO,
		// Mixed-version support was added in 21.1.
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			predV, err := PredecessorVersion(*t.BuildVersion())
			if err != nil {
				t.Fatal(err)
			}
			warehouses := 100
			if c.IsLocal() {
				warehouses = 10
			}
			runImportMixedVersion(ctx, t, c, warehouses, predV)
		},
	})
}

func registerImportDecommissioned(r registry.Registry) {
	runImportDecommissioned := func(ctx context.Context, t test.Test, c cluster.Cluster) {
		warehouses := 100
		if c.IsLocal() {
			warehouses = 10
		}

		c.Put(ctx, t.Cockroach(), "./cockroach")
		c.Put(ctx, t.DeprecatedWorkload(), "./workload")
		t.Status("starting csv servers")
		c.Start(ctx)
		c.Run(ctx, c.All(), `./workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)

		// Decommission a node.
		nodeToDecommission := 2
		t.Status(fmt.Sprintf("decommissioning node %d", nodeToDecommission))
		c.Run(ctx, c.Node(nodeToDecommission), `./cockroach node decommission --insecure --self --wait=all`)

		// Wait for a bit for node liveness leases to expire.
		time.Sleep(10 * time.Second)

		t.Status("running workload")
		c.Run(ctx, c.Node(1), tpccImportCmd(warehouses))
	}

	r.Add(registry.TestSpec{
		Name:    "import/decommissioned",
		Owner:   registry.OwnerBulkIO,
		Cluster: r.MakeClusterSpec(4),
		Run:     runImportDecommissioned,
	})
}
