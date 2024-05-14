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
		Timeout:          time.Hour,
		CompatibleClouds: registry.OnlyGCE,
		Benchmark:        true,
		Suites:           registry.ManualOnly,
		Cluster:          r.MakeClusterSpec(3, spec.CPU(32)),
		RequiresLicense:  true,
		Leases:           registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.IsLocal() {
				t.Skip("not meant to run locally")
			}
			if c.Spec().NodeCount != 3 {
				t.Fatalf("expected 3 nodes, found %d", c.Spec().NodeCount)
			}
			crdbNodes := 1
			promNode := c.Spec().NodeCount
			regularNode, elasticNode := c.Spec().NodeCount, c.Spec().NodeCount-1

			for i := 1; i <= crdbNodes; i++ {
				startOpts := option.NewStartOpts(option.NoBackupSchedule)
				startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, fmt.Sprintf("--attrs=n%d", i))
				c.Start(ctx, t.L(), startOpts, install.MakeClusterSettings(), c.Node(i))
			}

			t.Status(fmt.Sprintf("setting up prometheus/grafana (<%s)", 2*time.Minute))
			{
				promCfg := &prometheus.Config{}
				promCfg.WithPrometheusNode(c.Node(promNode).InstallNodes()[0])
				promCfg.WithNodeExporter(c.Range(1, c.Spec().NodeCount-1).InstallNodes())
				promCfg.WithCluster(c.Range(1, c.Spec().NodeCount-1).InstallNodes())
				promCfg.ScrapeConfigs = append(promCfg.ScrapeConfigs, prometheus.MakeWorkloadScrapeConfig("workload",
					"/", makeWorkloadScrapeNodes(c.Node(promNode).InstallNodes()[0], []workloadInstance{
						{nodes: c.Node(promNode)},
					})))
				promCfg.WithGrafanaDashboardJSON(grafana.SnapshotAdmissionControlGrafanaJSON)
				_, cleanupFunc := setupPrometheusForRoachtest(ctx, t, c, promCfg, nil)
				defer cleanupFunc()
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

			if err := setBandwidthLimit(c.Range(1, crdbNodes), "wbps", 128<<20 /* 128MiB */, false); err != nil {
				t.Fatal(err)
			}
			if err := setBandwidthLimit(c.Range(1, crdbNodes), "rbps", 128<<20 /* 128MiB */, false); err != nil {
				t.Fatal(err)
			}

			db := c.Conn(ctx, t.L(), crdbNodes)
			defer db.Close()

			if _, err := db.ExecContext(
				ctx, "SET CLUSTER SETTING kvadmission.store.provisioned_bandwidth = '75MiB'"); err != nil {
				t.Fatalf("failed to set kvadmission.store.provisioned_bandwidth: %v", err)
			}

			duration := 30 * time.Minute
			t.Status(fmt.Sprintf("starting kv workload thread (<%s)", time.Minute))
			m := c.NewMonitor(ctx, c.Range(1, crdbNodes))
			m.Go(func(ctx context.Context) error {
				dur := " --duration=" + duration.String()
				url := fmt.Sprintf(" {pgurl:1-%d}", crdbNodes)
				cmd := "./cockroach workload run kv --init --histograms=perf/stats.json --concurrency=2 " +
					"--splits=1000 --read-percent=50 --min-block-bytes=4096 --max-block-bytes=4096 " +
					"--txn-qos='regular' --tolerate-errors" + dur + url
				c.Run(ctx, option.WithNodes(c.Node(regularNode)), cmd)
				return nil
			})

			m.Go(func(ctx context.Context) error {
				time.Sleep(5 * time.Minute)
				dur := " --duration=" + duration.String()
				url := fmt.Sprintf(" {pgurl:1-%d}", crdbNodes)
				cmd := "./cockroach workload run kv --init --histograms=perf/stats.json --concurrency=1024 " +
					"--splits=1000 --read-percent=0 --min-block-bytes=4096 --max-block-bytes=4096 " +
					"--txn-qos='background' --tolerate-errors" + dur + url
				c.Run(ctx, option.WithNodes(c.Node(elasticNode)), cmd)
				return nil
			})

			m.Wait()
		},
	})
}
