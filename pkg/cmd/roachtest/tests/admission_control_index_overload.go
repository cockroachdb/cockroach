// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/stretchr/testify/assert"
)

// This test sets up a 3-node CRDB cluster on 8vCPU machines, loads it up with a
// large TPC-C dataset, and sets up a foreground load of kv50/1b. It then
// attempts to create a useless secondary index on the table while the workload
// is running to measure the impact. The index will not be used by any of the
// queries, but the intent is to measure the impact of the index creation.
//
// TODO(irfansharif): Nuke this test once admission-control/index-backfill
// stabilizes.
func registerIndexOverload(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "admission-control/index-overload",
		Owner:            registry.OwnerAdmissionControl,
		Benchmark:        true,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Weekly),
		Cluster:          r.MakeClusterSpec(4, spec.CPU(8), spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			c.Start(
				ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule),
				install.MakeClusterSettings(), c.CRDBNodes(),
			)

			{
				promCfg := &prometheus.Config{}
				promCfg.WithPrometheusNode(c.WorkloadNode().InstallNodes()[0])
				promCfg.WithNodeExporter(c.All().InstallNodes())
				promCfg.WithCluster(c.CRDBNodes().InstallNodes())
				promCfg.WithGrafanaDashboardJSON(grafana.SnapshotAdmissionControlGrafanaJSON)
				promCfg.ScrapeConfigs = append(promCfg.ScrapeConfigs, prometheus.MakeWorkloadScrapeConfig("workload",
					"/", makeWorkloadScrapeNodes(c.WorkloadNode().InstallNodes()[0], []workloadInstance{
						{nodes: c.WorkloadNode()},
					})))
				_, cleanupFunc := setupPrometheusForRoachtest(ctx, t, c, promCfg, []workloadInstance{{nodes: c.WorkloadNode()}})
				defer cleanupFunc()
			}

			duration, err := time.ParseDuration(roachtestutil.IfLocal(c, "20s", "10m"))
			assert.NoError(t, err)
			testDuration := 3 * duration

			db := c.Conn(ctx, t.L(), len(c.CRDBNodes()))
			defer db.Close()

			if !t.SkipInit() {
				t.Status("initializing kv dataset ", time.Minute)
				splits := roachtestutil.IfLocal(c, " --splits=3", " --splits=100")
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), "./cockroach workload init kv "+splits+" {pgurl:1}")

				// We need a big enough size so index creation will take enough time.
				t.Status("initializing tpcc dataset ", duration)
				warehouses := roachtestutil.IfLocal(c, " --warehouses=1", " --warehouses=2000")
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), "./cockroach workload fixtures import tpcc --checks=false"+warehouses+" {pgurl:1}")

				// Setting this low allows us to hit overload. In a larger cluster with
				// more nodes and larger tables, it will hit the unmodified 1000 limit.
				// TODO(baptist): Ideally lower the default setting to 10. Once that is
				// done, then this block can be removed.
				if _, err := db.ExecContext(ctx,
					"SET CLUSTER SETTING admission.l0_file_count_overload_threshold=10",
				); err != nil {
					t.Fatalf("failed to alter cluster setting: %v", err)
				}
			}

			t.Status("starting kv workload thread to run for ", testDuration)
			m := c.NewMonitor(ctx, c.CRDBNodes())
			m.Go(func(ctx context.Context) error {
				testDurationStr := " --duration=" + testDuration.String()
				concurrency := roachtestutil.IfLocal(c, "  --concurrency=8", " --concurrency=2048")
				c.Run(ctx, option.WithNodes(c.WorkloadNode()),
					"./cockroach workload run kv --read-percent=50 --max-rate=1000 --max-block-bytes=4096"+
						testDurationStr+concurrency+fmt.Sprintf(" {pgurl%s}", c.CRDBNodes()),
				)
				return nil
			})

			t.Status("recording baseline performance ", duration)
			time.Sleep(duration)

			// Choose an index creation that takes ~10-12 minutes.
			t.Status("starting index creation ", duration)
			if _, err := db.ExecContext(ctx,
				"CREATE INDEX test_index ON tpcc.stock(s_quantity)",
			); err != nil {
				t.Fatalf("failed to create index: %v", err)
			}

			t.Status("index creation complete - waiting for workload to finish ", duration)
			m.Wait()
		},
	})
}
