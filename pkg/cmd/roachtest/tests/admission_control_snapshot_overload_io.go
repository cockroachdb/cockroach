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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
)

func registerSnapshotOverloadIO(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "admission-control/snapshot-overload-io",
		Owner:            registry.OwnerAdmissionControl,
		Benchmark:        true,
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.ManualOnly,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(8)),
		Leases:           registry.MetamorphicLeases,
		Timeout:          12 * time.Hour,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Spec().NodeCount < 4 {
				t.Fatalf("expected at least 4 nodes, found %d", c.Spec().NodeCount)
			}

			crdbNodes := c.Spec().NodeCount - 1
			workloadNode := crdbNodes + 1
			for i := 1; i <= crdbNodes; i++ {
				startOpts := option.NewStartOpts(option.NoBackupSchedule)
				startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, fmt.Sprintf("--attrs=n%d", i))
				c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
			}

			db := c.Conn(ctx, t.L(), crdbNodes)
			defer db.Close()

			t.Status(fmt.Sprintf("configuring cluster settings (<%s)", 30*time.Second))
			{
				// Defensive, since admission control is enabled by default.
				setAdmissionControl(ctx, t, c, true)

				// Ensure ingest splits and excises are enabled. (Enabled by default in v24.1+)
				if _, err := db.ExecContext(
					ctx, "SET CLUSTER SETTING kv.snapshot_receiver.excise.enabled = 'false'"); err != nil {
					t.Fatalf("failed to set storage.ingest_split.enabled: %v", err)
				}
				if _, err := db.ExecContext(
					ctx, "SET CLUSTER SETTING storage.ingest_split.enabled = 'false'"); err != nil {
					t.Fatalf("failed to set storage.ingest_split.enabled: %v", err)
				}

				// Set high snapshot rates.
				if _, err := db.ExecContext(
					ctx, "SET CLUSTER SETTING kv.snapshot_rebalance.max_rate = '256MiB'"); err != nil {
					t.Fatalf("failed to set kv.snapshot_rebalance.max_rate: %v", err)
				}
			}

			t.Status(fmt.Sprintf("setting up prometheus/grafana (<%s)", 2*time.Minute))
			{
				promCfg := &prometheus.Config{}
				promCfg.WithPrometheusNode(c.Node(workloadNode).InstallNodes()[0])
				promCfg.WithNodeExporter(c.Range(1, c.Spec().NodeCount-1).InstallNodes())
				promCfg.WithCluster(c.Range(1, c.Spec().NodeCount-1).InstallNodes())
				promCfg.ScrapeConfigs = append(promCfg.ScrapeConfigs, prometheus.MakeWorkloadScrapeConfig("workload",
					"/", makeWorkloadScrapeNodes(c.Node(workloadNode).InstallNodes()[0], []workloadInstance{
						{nodes: c.Node(workloadNode)},
					})))
				promCfg.WithGrafanaDashboardJSON(grafana.SnapshotAdmissionControlGrafanaJSON)
				_, cleanupFunc := setupPrometheusForRoachtest(ctx, t, c, promCfg, nil)
				defer cleanupFunc()
			}

			// Initialize the kv database,
			t.Status(fmt.Sprintf("initializing kv dataset (<%s)", time.Minute))
			c.Run(ctx, option.WithNodes(c.Node(workloadNode)),
				"./cockroach workload init kv --drop --splits=1000 --insert-count=500000000 "+
					"--max-block-bytes=4096 --min-block-bytes=4096 {pgurl:1}")

			t.Status(fmt.Sprintf("starting kv workload thread (<%s)", time.Minute))
			m := c.NewMonitor(ctx, c.Range(1, crdbNodes))
			m.Go(func(ctx context.Context) error {
				c.Run(ctx, option.WithNodes(c.Node(crdbNodes+1)),
					fmt.Sprintf("./cockroach workload run kv --histograms=%s/stats.json --read-percent=0 --max-rate 500 --concurrency=1024 {pgurl:1-%d}",
						t.PerfArtifactsDir(), crdbNodes))
				return nil
			})

			// Wait for nodes to get populated.
			t.Status(fmt.Sprintf("waiting for kv workload to populate data for %s", time.Hour))
			time.Sleep(time.Hour)

			// Kill node 3.
			t.Status(fmt.Sprintf("killing node 3... (<%s)", time.Minute))
			c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(3))

			// Wait for raft log truncation.
			t.Status(fmt.Sprintf("waiting for raft log truncation (<%s)", time.Hour))

			// Start node 3.
			t.Status(fmt.Sprintf("killing node 3... (<%s)", time.Minute))
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(3))

			//t.Status(fmt.Sprintf("starting snapshot transfers for %s (<%s)", totalTransferDuration, time.Minute))
			//m.Go(func(ctx context.Context) error {
			//	for i := 0; i < iters; i++ {
			//		nextDestinationNode := 1 + ((i + 1) % crdbNodes) // if crdbNodes = 3, this cycles through 2, 3, 1, 2, 3, 1, ...
			//		t.Status(fmt.Sprintf("snapshot round %d/%d: inert data and active leases routing to n%d (<%s)",
			//			i+1, iters, nextDestinationNode, transferDuration))
			//
			//		if _, err := db.ExecContext(ctx, fmt.Sprintf(
			//			"ALTER DATABASE kv CONFIGURE ZONE USING num_replicas = %d, constraints = '{%s}', lease_preferences = '[[+n%d]]'",
			//			crdbNodes, constraint, nextDestinationNode),
			//		); err != nil {
			//			t.Fatalf("failed to configure zone for DATABASE kv: %v", err)
			//		}
			//		time.Sleep(leaseWaitDuration)
			//
			//		if _, err := db.ExecContext(ctx,
			//			fmt.Sprintf("ALTER DATABASE tpcc CONFIGURE ZONE USING num_replicas = 1, constraints = '[+n%d]';", nextDestinationNode),
			//		); err != nil {
			//			t.Fatalf("failed to configure zone for RANGE DEFAULT: %v", err)
			//		}
			//		time.Sleep(replicaWaitDuration)
			//	}
			//	return nil
			//})

			t.Status(fmt.Sprintf("waiting for workload/snapshot transfers to finish %s", time.Minute))
			m.Wait()
		},
	})
}
