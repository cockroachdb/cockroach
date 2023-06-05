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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
)

func registerExportParquet(r registry.Registry) {
	// This test sets up a 3-node CRDB cluster on 8vCPU machines initialized with
	// the TPC-C database containing 250 warehouses. Then, it executes 30 `EXPORT
	// INTO PARQUET` statements concurrently, repeatedly for 10 minutes.
	r.Add(registry.TestSpec{
		Name:            "export/parquet/bench",
		Owner:           registry.OwnerCDC,
		Tags:            registry.Tags("manual"),
		Cluster:         r.MakeClusterSpec(4, spec.CPU(8)),
		RequiresLicense: false,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Spec().NodeCount < 4 {
				t.Fatalf("expected at least 4 nodes, found %d", c.Spec().NodeCount)
			}

			numWarehouses, numConcurrentExports, exportDuration, pauseDuration := 250, 30, 10*time.Minute, 2*time.Minute
			if c.IsLocal() {
				numWarehouses, numConcurrentExports, exportDuration, pauseDuration = 10, 2, 1*time.Minute, 10*time.Second
			}

			// Set up grafana.
			crdbNodes := c.Spec().NodeCount - 1
			workloadNode := crdbNodes + 1
			cfg := (&prometheus.Config{}).
				WithPrometheusNode(c.Node(workloadNode).InstallNodes()[0]).
				WithCluster(c.Range(1, crdbNodes).InstallNodes()).
				WithNodeExporter(c.Range(1, crdbNodes).InstallNodes()).
				WithGrafanaDashboardJSON(grafana.ChangefeedRoachtestGrafanaDashboardJSON)
			cfg.Grafana.Enabled = true
			if !t.SkipInit() {
				err := c.StartGrafana(ctx, t.L(), cfg)
				if err != nil {
					t.Errorf("error starting prometheus/grafana: %s", err)
				}
				nodeURLs, err := c.ExternalIP(ctx, t.L(), c.Node(workloadNode))
				if err != nil {
					t.Errorf("error getting grafana node external ip: %s", err)
				}
				t.Status(fmt.Sprintf("started grafana at http://%s:3000/d/928XNlN4k/basic?from=now-15m&to=now", nodeURLs[0]))
			} else {
				t.Status("skipping grafana installation")
			}

			t.Status(fmt.Sprintf("initializing tpcc database with %d warehouses", numWarehouses))
			tpccOpts := tpccOptions{
				Warehouses:                    numWarehouses,
				SkipPostRunCheck:              true,
				ExtraSetupArgs:                "--checks=false",
				DisableDefaultScheduledBackup: true,
			}
			setupTPCC(ctx, t, c, tpccOpts)
			t.Status("finished initializing tpcc database")

			// Add padding to let the cluster metrics settle after initializing tpcc.
			t.Status(fmt.Sprintf("waiting for %s", pauseDuration))
			time.Sleep(pauseDuration)

			t.Status(fmt.Sprintf("running exports for %s", exportDuration))
			// Signal workers to stop after the export duration.
			cancelWorkers := atomic.Int64{}
			_ = time.AfterFunc(exportDuration, func() {
				t.Status("terminating workers...")
				cancelWorkers.Store(1)
			})

			wg := sync.WaitGroup{}
			for i := 0; i < numConcurrentExports; i++ {
				wg.Add(1)
				go func(i int, target string) {
					t.Status(fmt.Sprintf("worker %d/%d starting export of target %s", i+1, numConcurrentExports, target))
					fileNum := 0
					db := c.Conn(ctx, t.L(), 1)
					for cancelWorkers.Load() == 0 {
						_, err := db.Exec(
							fmt.Sprintf("EXPORT INTO PARQUET 'nodelocal://1/outputfile%d' FROM SELECT * FROM %s", fileNum, target))
						fileNum += 1
						if err != nil {
							t.Fatalf(err.Error())
						}
					}
					t.Status(fmt.Sprintf("worker %d/%d terminated", i+1, numConcurrentExports))
					wg.Done()
				}(i, allTpccTargets[i%len(allTpccTargets)])
			}
			wg.Wait()

			// Uncomment when using --debug to inspect metrics, gather profiles, etc.
			// t.FailNow()
		},
	})

	// This test sets up a 3-node CRDB cluster on 8vCPU machines initialized with
	// the TPC-C database containing 100 warehouses. Then, it executes concurrent
	// exports until the entire database is exported.
	r.Add(registry.TestSpec{
		Name:            "export/parquet/tpcc-100",
		Owner:           registry.OwnerCDC,
		Tags:            registry.Tags("daily"),
		Cluster:         r.MakeClusterSpec(4, spec.CPU(8)),
		RequiresLicense: false,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Spec().NodeCount < 4 {
				t.Fatalf("expected at least 4 nodes, found %d", c.Spec().NodeCount)
			}

			numWarehouses, pauseDuration := 100, 2*time.Minute
			if c.IsLocal() {
				numWarehouses, pauseDuration = 10, 10*time.Second
			}

			t.Status(fmt.Sprintf("initializing tpcc database with %d warehouses", numWarehouses))
			tpccOpts := tpccOptions{
				Warehouses:                    numWarehouses,
				SkipPostRunCheck:              true,
				ExtraSetupArgs:                "--checks=false",
				DisableDefaultScheduledBackup: true,
			}
			setupTPCC(ctx, t, c, tpccOpts)
			t.Status("finished initializing tpcc database")

			// Add padding to let the cluster metrics settle after initializing tpcc.
			t.Status(fmt.Sprintf("waiting for %s", pauseDuration))
			time.Sleep(pauseDuration)

			numWorkers := len(allTpccTargets)
			wg := sync.WaitGroup{}
			for i := 0; i < numWorkers; i++ {
				wg.Add(1)
				go func(i int, target string) {
					t.Status(fmt.Sprintf("worker %d/%d starting export of target %s", i+1, numWorkers, target))
					db := c.Conn(ctx, t.L(), 1)
					_, err := db.Exec(
						fmt.Sprintf("EXPORT INTO PARQUET 'nodelocal://1/outputfile%d' FROM SELECT * FROM %s", i, target))
					if err != nil {
						t.Fatalf(err.Error())
					}
					t.Status(fmt.Sprintf("worker %d/%d terminated", i+1, numWorkers))
					wg.Done()
				}(i, allTpccTargets[i])
			}
			wg.Wait()
		},
	})
}
