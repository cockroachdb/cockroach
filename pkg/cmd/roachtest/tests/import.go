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
	gosql "database/sql"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/version"
	"github.com/cockroachdb/errors"
)

func readCreateTableFromFixture(fixtureURI string, gatewayDB *gosql.DB) (string, error) {
	row := make([]byte, 0)
	err := gatewayDB.QueryRow(fmt.Sprintf(`SELECT crdb_internal.read_file('%s')`, fixtureURI)).Scan(&row)
	if err != nil {
		return "", err
	}
	return string(row), err
}

func registerImportNodeShutdown(r registry.Registry) {
	getImportRunner := func(ctx context.Context, t test.Test, gatewayNode int) jobStarter {
		startImport := func(c cluster.Cluster, t test.Test) (jobID string, err error) {
			// partsupp is 11.2 GiB.
			tableName := "partsupp"
			if c.IsLocal() {
				// part is 2.264 GiB.
				tableName = "part"
			}
			importStmt := fmt.Sprintf(`
				IMPORT INTO %[1]s
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
			gatewayDB := c.Conn(ctx, t.L(), gatewayNode)
			defer gatewayDB.Close()

			createStmt, err := readCreateTableFromFixture(
				fmt.Sprintf("gs://cockroach-fixtures/tpch-csv/schema/%s.sql?AUTH=implicit", tableName), gatewayDB)
			if err != nil {
				return "", err
			}

			// Create the table to be imported into.
			if _, err = gatewayDB.ExecContext(ctx, createStmt); err != nil {
				return jobID, err
			}

			err = gatewayDB.QueryRowContext(ctx, importStmt).Scan(&jobID)
			return
		}

		return startImport
	}

	r.Add(registry.TestSpec{
		Name:    "import/nodeShutdown/worker",
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Put(ctx, t.Cockroach(), "./cockroach")
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
			gatewayNode := 2
			nodeToShutdown := 3
			startImport := getImportRunner(ctx, t, gatewayNode)

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, startImport)
		},
	})
	r.Add(registry.TestSpec{
		Name:    "import/nodeShutdown/coordinator",
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Put(ctx, t.Cockroach(), "./cockroach")
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
			gatewayNode := 2
			nodeToShutdown := 2
			startImport := getImportRunner(ctx, t, gatewayNode)

			jobSurvivesNodeShutdown(ctx, t, c, nodeToShutdown, startImport)
		},
	})
}

func registerImportTPCC(r registry.Registry) {
	runImportTPCC := func(ctx context.Context, t test.Test, c cluster.Cluster, testName string,
		timeout time.Duration, warehouses int) {
		c.Put(ctx, t.Cockroach(), "./cockroach")
		c.Put(ctx, t.DeprecatedWorkload(), "./workload")
		t.Status("starting csv servers")
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
		c.Run(ctx, c.All(), `./workload csv-server --port=8081 &> logs/workload-csv-server.log < /dev/null &`)

		t.Status("running workload")
		m := c.NewMonitor(ctx)
		dul := roachtestutil.NewDiskUsageLogger(t, c)
		m.Go(dul.Runner)
		hc := roachtestutil.NewHealthChecker(t, c, c.All())
		m.Go(hc.Runner)

		tick, perfBuf := initBulkJobPerfArtifacts(testName, timeout)
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
			dest := filepath.Join(t.PerfArtifactsDir(), "stats.json")
			if err := c.RunE(ctx, c.Node(1), "mkdir -p "+filepath.Dir(dest)); err != nil {
				log.Errorf(ctx, "failed to create perf dir: %+v", err)
			}
			if err := c.PutString(ctx, perfBuf.String(), dest, 0755, c.Node(1)); err != nil {
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
			Name:              testName,
			Owner:             registry.OwnerSQLQueries,
			Cluster:           r.MakeClusterSpec(numNodes),
			Timeout:           timeout,
			EncryptionSupport: registry.EncryptionMetamorphic,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runImportTPCC(ctx, t, c, testName, timeout, warehouses)
			},
		})
	}
	const geoWarehouses = 4000
	const geoZones = "europe-west2-b,europe-west4-b,asia-northeast1-b,us-west1-b"
	r.Add(registry.TestSpec{
		Name:              fmt.Sprintf("import/tpcc/warehouses=%d/geo", geoWarehouses),
		Owner:             registry.OwnerSQLQueries,
		Cluster:           r.MakeClusterSpec(8, spec.CPU(16), spec.Geo(), spec.Zones(geoZones)),
		Timeout:           5 * time.Hour,
		EncryptionSupport: registry.EncryptionMetamorphic,
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
		// (07/27/21) The timeout was increased again to 10 hours. The test runs in
		// ~7 hours which causes it to occasionally exceed the previous timeout of 8
		// hours.
		{8, 10 * time.Hour},
	} {
		item := item
		r.Add(registry.TestSpec{
			Name:              fmt.Sprintf(`import/tpch/nodes=%d`, item.nodes),
			Owner:             registry.OwnerSQLQueries,
			Cluster:           r.MakeClusterSpec(item.nodes),
			Timeout:           item.timeout,
			EncryptionSupport: registry.EncryptionMetamorphic,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				tick, perfBuf := initBulkJobPerfArtifacts(t.Name(), item.timeout)

				c.Put(ctx, t.Cockroach(), "./cockroach")
				c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
				conn := c.Conn(ctx, t.L(), 1)
				if _, err := conn.Exec(`CREATE DATABASE csv;`); err != nil {
					t.Fatal(err)
				}
				if _, err := conn.Exec(`USE csv;`); err != nil {
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
				dul := roachtestutil.NewDiskUsageLogger(t, c)
				m.Go(dul.Runner)
				hc := roachtestutil.NewHealthChecker(t, c, c.All())
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

					createStmt, err := readCreateTableFromFixture(
						"gs://cockroach-fixtures/tpch-csv/schema/lineitem.sql?AUTH=implicit", conn)
					if err != nil {
						return err
					}

					// Create table to import into.
					if _, err := conn.ExecContext(ctx, createStmt); err != nil {
						return err
					}

					// Tick once before starting the import, and once after to capture the
					// total elapsed time. This is used by roachperf to compute and display
					// the average MB/sec per node.
					tick()
					_, err = conn.Exec(`
						IMPORT INTO csv.lineitem
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
					dest := filepath.Join(t.PerfArtifactsDir(), "stats.json")
					if err := c.RunE(ctx, c.Node(1), "mkdir -p "+filepath.Dir(dest)); err != nil {
						log.Errorf(ctx, "failed to create perf dir: %+v", err)
					}
					if err := c.PutString(ctx, perfBuf.String(), dest, 0755, c.Node(1)); err != nil {
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
	roachNodes := c.All()

	t.Status("starting csv servers")

	u := newVersionUpgradeTest(c,
		uploadAndStartFromCheckpointFixture(roachNodes, predecessorVersion),
		waitForUpgradeStep(roachNodes),
		preventAutoUpgradeStep(1),

		// Upgrade some of the nodes.
		binaryUpgradeStep(c.Node(1), clusterupgrade.MainVersion),
		binaryUpgradeStep(c.Node(2), clusterupgrade.MainVersion),

		successfulImportStep(warehouses, 1 /* nodeID */),
	)
	u.run(ctx, t)
}

func registerImportMixedVersion(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:  "import/mixed-versions",
		Owner: registry.OwnerSQLQueries,
		// Mixed-version support was added in 21.1.
		Cluster: r.MakeClusterSpec(4),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.IsLocal() && runtime.GOARCH == "arm64" {
				t.Skip("Skip under ARM64. See https://github.com/cockroachdb/cockroach/issues/89268")
			}
			predV, err := version.PredecessorVersion(*t.BuildVersion())
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
		c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings())
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
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(4),
		Run:     runImportDecommissioned,
	})
}
