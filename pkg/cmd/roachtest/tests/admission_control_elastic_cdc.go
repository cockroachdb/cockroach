// Copyright 2022 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
)

// This test sets up a 3-node CRDB cluster on 8vCPU machines running
// 1000-warehouse TPC-C, and kicks off a few changefeed backfills concurrently.
// We've observed latency spikes during backfills because of its CPU/scan-heavy
// nature -- it can elevate CPU scheduling latencies which in turn translates to
// an increase in foreground latency.
func registerElasticControlForCDC(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:            "admission-control/elastic-cdc",
		Owner:           registry.OwnerAdmissionControl,
		Tags:            []string{`weekly`},
		Cluster:         r.MakeClusterSpec(4, spec.CPU(8)),
		RequiresLicense: true,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Spec().NodeCount < 4 {
				t.Fatalf("expected at least 4 nodes, found %d", c.Spec().NodeCount)
			}

			crdbNodes := c.Spec().NodeCount - 1
			workloadNode := crdbNodes + 1
			numWarehouses, workloadDuration, estimatedSetupTime := 1000, 60*time.Minute, 10*time.Minute
			if c.IsLocal() {
				numWarehouses, workloadDuration, estimatedSetupTime = 1, time.Minute, 2*time.Minute
			}

			promCfg := &prometheus.Config{}
			promCfg.WithPrometheusNode(c.Node(workloadNode).InstallNodes()[0]).
				WithNodeExporter(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
				WithCluster(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
				WithGrafanaDashboard("http://go.crdb.dev/p/changefeed-admission-control-grafana").
				WithScrapeConfigs(
					prometheus.MakeWorkloadScrapeConfig("workload", "/",
						makeWorkloadScrapeNodes(
							c.Node(workloadNode).InstallNodes()[0],
							[]workloadInstance{{nodes: c.Node(workloadNode)}},
						),
					),
				)

			if t.SkipInit() {
				t.Status(fmt.Sprintf("running tpcc for %s (<%s)", workloadDuration, time.Minute))
			} else {
				t.Status(fmt.Sprintf("initializing + running tpcc for %s (<%s)", workloadDuration, 10*time.Minute))
			}

			padDuration, err := time.ParseDuration(ifLocal(c, "5s", "5m"))
			if err != nil {
				t.Fatal(err)
			}
			stopFeedsDuration, err := time.ParseDuration(ifLocal(c, "5s", "1m"))
			if err != nil {
				t.Fatal(err)
			}

			runTPCC(ctx, t, c, tpccOptions{
				Warehouses:                    numWarehouses,
				Duration:                      workloadDuration,
				SetupType:                     usingImport,
				EstimatedSetupTime:            estimatedSetupTime,
				SkipPostRunCheck:              true,
				ExtraSetupArgs:                "--checks=false",
				PrometheusConfig:              promCfg,
				DisableDefaultScheduledBackup: true,
				During: func(ctx context.Context) error {
					db := c.Conn(ctx, t.L(), crdbNodes)
					defer db.Close()

					t.Status(fmt.Sprintf("configuring cluster (<%s)", 30*time.Second))
					{
						setAdmissionControl(ctx, t, c, true)

						// Changefeeds depend on rangefeeds being enabled.
						if _, err := db.Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true"); err != nil {
							return err
						}
					}

					stopFeeds(db) // stop stray feeds (from repeated runs against the same cluster for ex.)
					defer stopFeeds(db)

					m := c.NewMonitor(ctx, c.Range(1, crdbNodes))
					m.Go(func(ctx context.Context) error {
						const iters, changefeeds = 5, 10
						for i := 0; i < iters; i++ {
							if i == 0 {
								t.Status(fmt.Sprintf("setting performance baseline (<%s)", padDuration))
							}
							time.Sleep(padDuration) // each iteration lasts long enough to observe effects in metrics

							t.Status(fmt.Sprintf("during: round %d: stopping extant changefeeds (<%s)", i, stopFeedsDuration))
							stopFeeds(db)
							time.Sleep(stopFeedsDuration) // buffer for cancellations to take effect/show up in metrics

							t.Status(fmt.Sprintf("during: round %d: creating %d changefeeds (<%s)", i, changefeeds, time.Minute))

							for j := 0; j < changefeeds; j++ {
								var createChangefeedStmt string
								if i%2 == 0 {
									createChangefeedStmt = fmt.Sprintf(`
									CREATE CHANGEFEED FOR tpcc.order_line, tpcc.stock, tpcc.customer
									INTO 'null://' WITH cursor = '-%ds'
								`, int64(float64(i+1)*padDuration.Seconds())) // scanning as far back as possible (~ when the workload started)
								} else {
									createChangefeedStmt = "CREATE CHANGEFEED FOR tpcc.order_line, tpcc.stock, tpcc.customer " +
										"INTO 'null://' WITH initial_scan = 'only'"
								}

								if _, err := db.ExecContext(ctx, createChangefeedStmt); err != nil {
									return err
								}
							}
						}
						return nil
					})

					t.Status(fmt.Sprintf("waiting for workload to finish (<%s)", workloadDuration))
					m.Wait()

					return nil
				},
			})
		},
	})
}
