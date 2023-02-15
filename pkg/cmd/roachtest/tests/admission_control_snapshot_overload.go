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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
)

// This test sets up a 3-node CRDB cluster on 8vCPU machines, loads
// it up with a large TPC-C dataset, and sets up a foreground load of kv95/1b.
// The TPC-C dataset has a replication of factor of 1 and is used as the large
// lump we bus around through snapshots -- snapshots that are send to the node
// containing leaseholders serving kv95 traffic. Snapshots follow where the
// leases travel, cycling through each node, evaluating performance isolation in
// the presence of snapshots.
//
// TODO(irfansharif): This primarily stresses CPU; write an equivalent for IO.
// TODO(irfansharif): This is a relatively long-running test (takes ~3.5hrs);
// make it shorter.
func registerSnapshotOverload(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "admission-control/snapshot-overload",
		Owner:   registry.OwnerAdmissionControl,
		Tags:    []string{`weekly`},
		Cluster: r.MakeClusterSpec(4, spec.CPU(8)),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Spec().NodeCount < 4 {
				t.Fatalf("expected at least 4 nodes, found %d", c.Spec().NodeCount)
			}

			c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
			crdbNodes := c.Spec().NodeCount - 1
			workloadNode := crdbNodes + 1
			for i := 1; i <= crdbNodes; i++ {
				startOpts := option.DefaultStartOptsNoBackups()
				startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, fmt.Sprintf("--attrs=n%d", i))
				c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
			}

			db := c.Conn(ctx, t.L(), crdbNodes)
			defer db.Close()

			// Set a replication factor of 1 and pin replicas to n1 by default.
			// For the active dataset (we use kv further below) we'll use a
			// different config.
			t.Status(fmt.Sprintf("configuring default zone and settings (<%s)", 30*time.Second))
			{
				if _, err := db.ExecContext(ctx,
					"ALTER RANGE DEFAULT CONFIGURE ZONE USING num_replicas = 1, constraints = '[+n1]';",
				); err != nil {
					t.Fatalf("failed to configure zone for RANGE DEFAULT: %v", err)
				}

				// Defensive, since admission control is enabled by default. This
				// test can fail if admission control is disabled.
				setAdmissionControl(ctx, t, c, true)

				// Set high snapshot rates.
				if _, err := db.ExecContext(
					ctx, "SET CLUSTER SETTING kv.snapshot_rebalance.max_rate = '256MiB'"); err != nil {
					t.Fatalf("failed to set kv.snapshot_rebalance.max_rate: %v", err)
				}
				if _, err := db.ExecContext(
					ctx, "SET CLUSTER SETTING kv.snapshot_recovery.max_rate = '256MiB'"); err != nil {
					t.Fatalf("failed to set kv.snapshot_recovery.max_rate: %v", err)
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
				promCfg.WithGrafanaDashboard("http://go.crdb.dev/p/snapshot-admission-control-grafana")
				_, cleanupFunc := setupPrometheusForRoachtest(ctx, t, c, promCfg, nil)
				defer cleanupFunc()
			}

			var constraints []string
			for i := 1; i <= crdbNodes; i++ {
				constraints = append(constraints, fmt.Sprintf("+n%d: 1", i))
			}
			constraint := strings.Join(constraints, ",")

			// Initialize the kv database with replicas on all nodes but
			// leaseholders on just n1.
			t.Status(fmt.Sprintf("initializing kv dataset (<%s)", time.Minute))
			if !t.SkipInit() {
				splits := ifLocal(c, " --splits=10", " --splits=100")
				c.Run(ctx, c.Node(workloadNode), "./cockroach workload init kv "+splits+" {pgurl:1}")

				if _, err := db.ExecContext(ctx, fmt.Sprintf(
					"ALTER DATABASE kv CONFIGURE ZONE USING num_replicas = %d, constraints = '{%s}', lease_preferences = '[[+n1]]'",
					crdbNodes, constraint),
				); err != nil {
					t.Fatalf("failed to configure zone for DATABASE kv: %v", err)
				}
			}

			t.Status(fmt.Sprintf("initializing tpcc dataset (<%s)", 20*time.Minute))
			if !t.SkipInit() {
				warehouses := ifLocal(c, " --warehouses=10", " --warehouses=2000")
				c.Run(ctx, c.Node(workloadNode), "./cockroach workload fixtures import tpcc --checks=false"+warehouses+" {pgurl:1}")
			}

			const iters = 4
			padDuration, err := time.ParseDuration(ifLocal(c, "20s", "5m"))
			if err != nil {
				t.Fatal(err)
			}
			leaseWaitDuration, err := time.ParseDuration(ifLocal(c, "20s", "5m"))
			if err != nil {
				t.Fatal(err)
			}
			replicaWaitDuration, err := time.ParseDuration(ifLocal(c, "40s", "25m"))
			if err != nil {
				t.Fatal(err)
			}

			transferDuration := leaseWaitDuration + replicaWaitDuration
			totalTransferDuration := transferDuration * iters
			totalWorkloadDuration := totalTransferDuration + (2 * padDuration)

			t.Status(fmt.Sprintf("starting kv workload thread to run for %s (<%s)", totalWorkloadDuration, time.Minute))
			m := c.NewMonitor(ctx, c.Range(1, crdbNodes))
			m.Go(func(ctx context.Context) error {
				duration := " --duration=" + totalWorkloadDuration.String()
				histograms := " --histograms=" + t.PerfArtifactsDir() + "/stats.json"
				concurrency := ifLocal(c, "  --concurrency=8", " --concurrency=256")
				maxRate := ifLocal(c, "  --max-rate=100", " --max-rate=12000")
				splits := ifLocal(c, "  --splits=10", " --splits=100")
				c.Run(ctx, c.Node(crdbNodes+1),
					"./cockroach workload run kv --max-block-bytes=1 --read-percent=95 "+
						histograms+duration+concurrency+maxRate+splits+fmt.Sprintf(" {pgurl:1-%d}", crdbNodes),
				)
				return nil
			})

			t.Status(fmt.Sprintf("setting performance baseline (<%s)", padDuration))
			time.Sleep(padDuration)

			t.Status(fmt.Sprintf("starting snapshot transfers for %s (<%s)", totalTransferDuration, time.Minute))
			m.Go(func(ctx context.Context) error {
				for i := 0; i < iters; i++ {
					nextDestinationNode := 1 + ((i + 1) % crdbNodes) // if crdbNodes = 3, this cycles through 2, 3, 1, 2, 3, 1, ...
					t.Status(fmt.Sprintf("snapshot round %d/%d: inert data and active leases routing to n%d (<%s)",
						i+1, iters, nextDestinationNode, transferDuration))

					if _, err := db.ExecContext(ctx, fmt.Sprintf(
						"ALTER DATABASE kv CONFIGURE ZONE USING num_replicas = %d, constraints = '{%s}', lease_preferences = '[[+n%d]]'",
						crdbNodes, constraint, nextDestinationNode),
					); err != nil {
						t.Fatalf("failed to configure zone for DATABASE kv: %v", err)
					}
					time.Sleep(leaseWaitDuration)

					if _, err := db.ExecContext(ctx,
						fmt.Sprintf("ALTER DATABASE tpcc CONFIGURE ZONE USING num_replicas = 1, constraints = '[+n%d]';", nextDestinationNode),
					); err != nil {
						t.Fatalf("failed to configure zone for RANGE DEFAULT: %v", err)
					}
					time.Sleep(replicaWaitDuration)
				}
				return nil
			})

			t.Status(fmt.Sprintf("waiting for workload/snapshot transfers to finish (<%s)", totalWorkloadDuration-padDuration))
			m.Wait()
		},
	})
}
