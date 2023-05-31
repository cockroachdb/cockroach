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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/uber-go/atomic"
)

// This test sets up a 3-node CRDB cluster on 8vCPU machines initialized with
// the TPC-C database containing 250 warehouses. Then, it executes an `EXPORT
// INTO PARQUET` statement concurrently for each table for 10 minutes. The
// purpose of this test is to benchmark exporting with parquet.
func registerExportParquet(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:            "export/parquet/tpcc-250",
		Owner:           registry.OwnerCDC,
		Tags:            registry.Tags(`weekly`),
		Cluster:         r.MakeClusterSpec(4, spec.CPU(8)),
		RequiresLicense: false,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Spec().NodeCount < 4 {
				t.Fatalf("expected at least 4 nodes, found %d", c.Spec().NodeCount)
			}

			crdbNodes := c.Spec().NodeCount - 1
			workloadNode := crdbNodes + 1
			numWarehouses, numConcurrentExports, exportDuration := 250, 30, 10*time.Minute
			if c.IsLocal() {
				numWarehouses, numConcurrentExports, exportDuration = 10, 2, 1*time.Minute
			}

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

			// Some padding to let the cluster metrics settle after initializing TPCC.
			pauseDuration := 2 * time.Minute
			t.Status(fmt.Sprintf("waiting for %s", pauseDuration))
			time.Sleep(pauseDuration)

			t.Status(fmt.Sprintf("running exports for %s", exportDuration))

			// Signal to tell the works running the export workload to finish.
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
					for cancelWorkers.Load() == 0 {
						_, err := c.Conn(ctx, t.L(), 1).Exec(
							fmt.Sprintf("EXPORT INTO PARQUET 'nodelocal://1/outputfile' FROM SELECT * FROM %s", target))
						if err != nil {
							t.Fatalf(err.Error())
						}
					}
					t.Status(fmt.Sprintf("worker %d/%d terminated", i+1, numConcurrentExports))
					wg.Done()
				}(i, allTpccTargets[i%len(allTpccTargets)])
			}
			wg.Wait()
		},
	})
}
