// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"
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
		Name:             "admission-control/snapshot-overload",
		Owner:            registry.OwnerAdmissionControl,
		Benchmark:        true,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Weekly),
		Cluster:          r.MakeClusterSpec(4, spec.CPU(8), spec.WorkloadNode()),
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Spec().NodeCount < 4 {
				t.Fatalf("expected at least 4 nodes, found %d", c.Spec().NodeCount)
			}

			for i := 1; i <= len(c.CRDBNodes()); i++ {
				startOpts := option.NewStartOpts(option.NoBackupSchedule)
				startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, fmt.Sprintf("--attrs=n%d", i))
				c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
			}

			db := c.Conn(ctx, t.L(), len(c.CRDBNodes()))
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
				roachtestutil.SetAdmissionControl(ctx, t, c, true)

				// Set high snapshot rates.
				if _, err := db.ExecContext(
					ctx, "SET CLUSTER SETTING kv.snapshot_rebalance.max_rate = '256MiB'"); err != nil {
					t.Fatalf("failed to set kv.snapshot_rebalance.max_rate: %v", err)
				}
			}

			t.Status(fmt.Sprintf("setting up prometheus/grafana (<%s)", 2*time.Minute))
			{
				promCfg := &prometheus.Config{}
				promCfg.WithPrometheusNode(c.WorkloadNode().InstallNodes()[0])
				promCfg.WithNodeExporter(c.CRDBNodes().InstallNodes())
				promCfg.WithCluster(c.CRDBNodes().InstallNodes())
				promCfg.ScrapeConfigs = append(promCfg.ScrapeConfigs, prometheus.MakeWorkloadScrapeConfig("workload",
					"/", makeWorkloadScrapeNodes(c.WorkloadNode().InstallNodes()[0], []workloadInstance{
						{nodes: c.WorkloadNode()},
					})))
				promCfg.WithGrafanaDashboardJSON(grafana.SnapshotAdmissionControlGrafanaJSON)
				_, cleanupFunc := setupPrometheusForRoachtest(ctx, t, c, promCfg, nil)
				defer cleanupFunc()
			}

			var constraints []string
			for i := 1; i <= len(c.CRDBNodes()); i++ {
				constraints = append(constraints, fmt.Sprintf("+n%d: 1", i))
			}
			constraint := strings.Join(constraints, ",")

			// Initialize the kv database with replicas on all nodes but
			// leaseholders on just n1.
			t.Status(fmt.Sprintf("initializing kv dataset (<%s)", time.Minute))
			if !t.SkipInit() {
				splits := roachtestutil.IfLocal(c, " --splits=10", " --splits=100")
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), "./cockroach workload init kv "+splits+" {pgurl:1}")

				if _, err := db.ExecContext(ctx, fmt.Sprintf(
					"ALTER DATABASE kv CONFIGURE ZONE USING num_replicas = %d, constraints = '{%s}', lease_preferences = '[[+n1]]'",
					len(c.CRDBNodes()), constraint),
				); err != nil {
					t.Fatalf("failed to configure zone for DATABASE kv: %v", err)
				}
			}

			t.Status(fmt.Sprintf("initializing tpcc dataset (<%s)", 20*time.Minute))
			if !t.SkipInit() {
				warehouses := roachtestutil.IfLocal(c, " --warehouses=10", " --warehouses=2000")
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), "./cockroach workload fixtures import tpcc --checks=false"+warehouses+" {pgurl:1}")
			}

			const iters = 4
			padDuration, err := time.ParseDuration(roachtestutil.IfLocal(c, "20s", "5m"))
			if err != nil {
				t.Fatal(err)
			}
			leaseWaitDuration, err := time.ParseDuration(roachtestutil.IfLocal(c, "20s", "5m"))
			if err != nil {
				t.Fatal(err)
			}
			replicaWaitDuration, err := time.ParseDuration(roachtestutil.IfLocal(c, "40s", "25m"))
			if err != nil {
				t.Fatal(err)
			}

			transferDuration := leaseWaitDuration + replicaWaitDuration
			totalTransferDuration := transferDuration * iters
			totalWorkloadDuration := totalTransferDuration + (2 * padDuration)

			t.Status(fmt.Sprintf("starting kv workload thread to run for %s (<%s)", totalWorkloadDuration, time.Minute))
			m := c.NewMonitor(ctx, c.CRDBNodes())
			m.Go(func(ctx context.Context) error {
				duration := " --duration=" + totalWorkloadDuration.String()
				concurrency := roachtestutil.IfLocal(c, "  --concurrency=8", " --concurrency=256")
				maxRate := roachtestutil.IfLocal(c, "  --max-rate=100", " --max-rate=12000")
				splits := roachtestutil.IfLocal(c, "  --splits=10", " --splits=100")

				labels := map[string]string{
					"concurrency": concurrency,
					"max-rate":    maxRate,
					"splits":      splits,
				}
				histograms := roachtestutil.GetWorkloadHistogramArgs(t, c, labels)
				c.Run(ctx, option.WithNodes(c.WorkloadNode()),
					"./cockroach workload run kv --max-block-bytes=1 --read-percent=95 "+
						histograms+duration+concurrency+maxRate+splits+fmt.Sprintf(" {pgurl%s}", c.CRDBNodes()),
				)
				return nil
			})

			t.Status(fmt.Sprintf("setting performance baseline (<%s)", padDuration))
			time.Sleep(padDuration)

			t.Status(fmt.Sprintf("starting snapshot transfers for %s (<%s)", totalTransferDuration, time.Minute))
			m.Go(func(ctx context.Context) error {
				for i := 0; i < iters; i++ {
					nextDestinationNode := 1 + ((i + 1) % len(c.CRDBNodes())) // if crdbNodes = 3, this cycles through 2, 3, 1, 2, 3, 1, ...
					t.Status(fmt.Sprintf("snapshot round %d/%d: inert data and active leases routing to n%d (<%s)",
						i+1, iters, nextDestinationNode, transferDuration))

					if _, err := db.ExecContext(ctx, fmt.Sprintf(
						"ALTER DATABASE kv CONFIGURE ZONE USING num_replicas = %d, constraints = '{%s}', lease_preferences = '[[+n%d]]'",
						len(c.CRDBNodes()), constraint, nextDestinationNode),
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
