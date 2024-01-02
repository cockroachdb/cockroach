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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func registerElasticControlForRowLevelTTL(r registry.Registry) {
	const nodes = 7
	var clusterSpec = spec.CPU(4)
	r.Add(makeElasticControlRowLevelTTL(r.MakeClusterSpec(nodes, clusterSpec), false /* expiredRows */))
	r.Add(makeElasticControlRowLevelTTL(r.MakeClusterSpec(nodes, clusterSpec), true /* expiredRows */))
}

func makeElasticControlRowLevelTTL(spec spec.ClusterSpec, expiredRows bool) registry.TestSpec {
	return registry.TestSpec{
		Name:             fmt.Sprintf("admission-control/row-level-ttl/expired-rows=%t", expiredRows),
		Owner:            registry.OwnerAdmissionControl,
		Benchmark:        true,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Weekly),
		Cluster:          spec,
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			crdbNodes := c.Spec().NodeCount - 1
			workloadNode := crdbNodes + 1

			numWarehouses, activeWarehouses, workloadDuration, estimatedSetupTime := 1500, 100, 20*time.Minute, 20*time.Minute
			if c.IsLocal() {
				numWarehouses, activeWarehouses, workloadDuration, estimatedSetupTime = 1, 1, 3*time.Minute, 2*time.Minute
			}

			promCfg := &prometheus.Config{}
			promCfg.WithPrometheusNode(c.Node(workloadNode).InstallNodes()[0]).
				WithNodeExporter(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
				WithCluster(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
				WithGrafanaDashboard("https://go.crdb.dev/p/index-admission-control-grafana").
				WithScrapeConfigs(
					prometheus.MakeWorkloadScrapeConfig("workload", "/",
						makeWorkloadScrapeNodes(
							c.Node(workloadNode).InstallNodes()[0],
							[]workloadInstance{{nodes: c.Node(workloadNode)}},
						),
					),
				)

			if t.SkipInit() {
				t.Status(fmt.Sprintf("running tpcc for %s (<%s)", workloadDuration, estimatedSetupTime))
			} else {
				t.Status(fmt.Sprintf("initializing + running tpcc for %s (<%s)", workloadDuration, estimatedSetupTime))
			}

			runTPCC(ctx, t, c, tpccOptions{
				Warehouses:         numWarehouses,
				Duration:           workloadDuration,
				SetupType:          usingImport,
				EstimatedSetupTime: estimatedSetupTime,
				// The expired-rows test will delete rows from the order_line table, so
				// the post run checks are expected to fail.
				SkipPostRunCheck: expiredRows,
				PrometheusConfig: promCfg,
				// We limit the number of workers because the default results in a lot
				// of connections which can lead to OOM issues (see #40566).
				ExtraRunArgs:                  fmt.Sprintf("--wait=false --tolerate-errors --max-rate=100 --active-warehouses=%d --workers=%d", activeWarehouses, numWarehouses),
				DisableDefaultScheduledBackup: true,
				During: func(ctx context.Context) error {
					cronOffset := 10
					if c.IsLocal() {
						cronOffset = 1
					}
					nowMinute := timeutil.Now().Minute()
					scheduledMinute := (nowMinute + cronOffset) % 60 // schedule the TTL cron job to kick off a few minutes after test start

					var expirationExpr string
					if expiredRows {
						expirationExpr = `'((ol_delivery_d::TIMESTAMP) + INTERVAL ''1 days'') AT TIME ZONE ''UTC'''`
					} else {
						// The TPCC fixtures have dates from 2006 for the ol_delivery_d column.
						expirationExpr = `'((ol_delivery_d::TIMESTAMP) + INTERVAL ''1000 years'') AT TIME ZONE ''UTC'''`
					}

					// NB: To verify that AC is working as expected, ensure
					// admission_scheduler_latency_listener_p99_nanos is around
					// 1ms, that sys_cpu_combined_percent_normalized doesn't hit
					// 100% (it stays around 60% at the time of writing) and
					// that admission_elastic_cpu_utilization >= 5%, showing
					// that we're acquiring elastic CPU tokens.

					ttlStatement := fmt.Sprintf(`
					ALTER TABLE tpcc.public.order_line SET (
					    ttl_expiration_expression=%s,
					    ttl_job_cron='%d * * * *'
					);`, expirationExpr, scheduledMinute)
					return runAndLogStmts(ctx, t, c, "enable-ttl", []string{ttlStatement})
				},
			})
		},
	}
}
