// Copyright 2024 The Cockroach Authors.
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
	"path/filepath"
	"strconv"
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

// This test causes LSM overload induced by saturating disk bandwidth limits.
func registerDiskBandwidthOverload(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "admission-control/disk-bandwidth",
		Owner:            registry.OwnerAdmissionControl,
		Benchmark:        true,
		CompatibleClouds: registry.AllClouds,
		Suites:           registry.ManualOnly,
		Cluster:          r.MakeClusterSpec(4, spec.CPU(4), spec.VolumeSize(256)),
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

			// TODO(aaditya): This function shares a lot of logic with roachtestutil.DiskStaller. Consider merging the two.
			setBandwidthLimit := func(nodes option.NodeListOption, rw string, bw int, max bool) error {
				res, err := c.RunWithDetailsSingleNode(context.TODO(), t.L(), option.WithNodes(nodes[:1]), "lsblk | grep /mnt/data1 | awk '{print $2}'")
				if err != nil {
					t.Fatalf("error when determining block device: %s", err)
				}
				parts := strings.Split(strings.TrimSpace(res.Stdout), ":")
				if len(parts) != 2 {
					t.Fatalf("unexpected output from lsblk: %s", res.Stdout)
				}
				major, err := strconv.Atoi(parts[0])
				if err != nil {
					t.Fatalf("error when determining block device: %s", err)
				}
				minor, err := strconv.Atoi(parts[1])
				if err != nil {
					t.Fatalf("error when determining block device: %s", err)
				}

				cockroachIOController := filepath.Join("/sys/fs/cgroup/system.slice", roachtestutil.SystemInterfaceSystemdUnitName()+".service", "io.max")
				bytesPerSecondStr := "max"
				if !max {
					bytesPerSecondStr = fmt.Sprintf("%d", bw)
				}
				return c.RunE(ctx, option.WithNodes(nodes), "sudo", "/bin/bash", "-c", fmt.Sprintf(
					`'echo %d:%d %s=%s > %s'`,
					major,
					minor,
					rw,
					bytesPerSecondStr,
					cockroachIOController,
				))
			}

			setBandwidthLimit(c.Range(1, crdbNodes), "wbps", 1<<27 /* 128MiB */, false)
			setBandwidthLimit(c.Range(1, crdbNodes), "rbps", 1<<27 /* 128MiB */, false)

			db := c.Conn(ctx, t.L(), crdbNodes)
			defer db.Close()

			t.Status(fmt.Sprintf("configuring cluster settings (<%s)", 30*time.Second))
			{
				// Defensive, since admission control is enabled by default.
				setAdmissionControl(ctx, t, c, true)

				// Increase rebalance rate.
				if _, err := db.ExecContext(
					ctx, "SET CLUSTER SETTING kv.snapshot_rebalance.max_rate = '512MiB'"); err != nil {
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
			t.Status(fmt.Sprintf("initializing kv dataset (<%s)", time.Hour))
			c.Run(ctx, option.WithNodes(c.Node(workloadNode)),
				"./cockroach workload init kv --drop --insert-count=40000000 "+
					"--max-block-bytes=4096 --min-block-bytes=4096 {pgurl:1}")

			t.Status(fmt.Sprintf("starting kv workload thread (<%s)", time.Minute))
			m := c.NewMonitor(ctx, c.Range(1, crdbNodes))
			m.Go(func(ctx context.Context) error {
				c.Run(ctx, option.WithNodes(c.Node(workloadNode)),
					fmt.Sprintf("./cockroach workload run kv --tolerate-errors --splits=1000 --histograms=%s/stats.json --read-percent=50 --max-rate=600 --max-block-bytes=4096 --min-block-bytes=4096 --concurrency=256 {pgurl:1}",
						t.PerfArtifactsDir()))
				return nil
			})

			// Wait for data.
			t.Status(fmt.Sprintf("waiting for data build up (<%s)", time.Hour*1))
			time.Sleep(time.Hour * 1)

			// Kill node 3.
			t.Status(fmt.Sprintf("killing node 3... (<%s)", time.Minute))
			c.Stop(ctx, t.L(), option.DefaultStopOpts(), c.Node(3))

			// Wait for raft log truncation.
			t.Status(fmt.Sprintf("waiting for raft log truncation (<%s)", time.Hour*2))
			time.Sleep(time.Hour * 2)

			// Start node 3.
			t.Status(fmt.Sprintf("starting node 3... (<%s)", time.Minute))
			c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(), c.Node(3))

			// TODO(aaditya)
			// 	1. We need to assert on sublevel count and sql latency while the ingest is taking place

			t.Status(fmt.Sprintf("waiting for workload/snapshot transfers to finish %s", time.Minute))
			m.Wait()
		},
	})
}
