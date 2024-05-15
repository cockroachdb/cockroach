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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/clusterstats"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/grafana"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// This test sets up 2 workloads – kv0 consisting of "normal" priority writes
// and kv0 consisting of "background" priority writes. The goal is to show that
// even with a demanding "background" workload that is able to push the used
// bandwidth much higher than the provisioned one, the AC bandwidth limiter
// paces the traffic at the set bandwidth limit.
func registerDiskBandwidthOverload(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "admission-control/disk-bandwidth-limiter",
		Owner:            registry.OwnerAdmissionControl,
		Timeout:          time.Hour,
		CompatibleClouds: registry.OnlyGCE,
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

			promClient, err := clusterstats.SetupCollectorPromClient(ctx, c, t.L(), promCfg)
			require.NoError(t, err)
			statCollector := clusterstats.NewStatsCollector(ctx, promClient)

			// TODO(aaditya): This function shares some of the logic with roachtestutil.DiskStaller. Consider merging the two.
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

			// TODO(aaditya): Extend this test to also limit reads once we have a
			// mechanism to pace read traffic in AC.

			db := c.Conn(ctx, t.L(), crdbNodes)
			defer db.Close()

			const provisionedBandwidth = 75
			if _, err := db.ExecContext(
				// We intentionally set this to much lower than the provisioned value
				// above to clearly show that the bandwidth limiter works.
				ctx, fmt.Sprintf("SET CLUSTER SETTING kvadmission.store.provisioned_bandwidth = '%dMiB'", provisionedBandwidth)); err != nil {
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

			m.Go(func(ctx context.Context) error {
				const subLevelMetric = "storage_l0_sublevels"
				const writeBytesMetric = "sys_host_disk_write_bytes"
				getMetricVal := func(metricName string) (float64, error) {
					point, err := statCollector.CollectPoint(ctx, t.L(), timeutil.Now(), metricName)
					if err != nil {
						t.L().Errorf("could not query prom %s", err.Error())
						return 0, err
					}
					const labelName = "store"
					val := point[labelName]
					if len(val) != 1 {
						err = errors.Errorf(
							"unexpected number %d of points for metric %s", len(val), metricName)
						t.L().Errorf("%s", err.Error())
						return 0, err
					}
					for storeID, v := range val {
						t.L().Printf("%s(store=%s): %f", metricName, storeID, v.Value)
						return v.Value, nil
					}
					// Unreachable.
					panic("unreachable")
				}

				const subLevelThreshold = 20
				// Allow a 5% threshold.
				const bandwidthThreshold = provisionedBandwidth * 1.05
				const collectionIntervalSeconds = 10.0
				// We want to use the mean of the last 2m of data to avoid short-lived
				// spikes causing failures.
				const sampleCount = int(2 / (collectionIntervalSeconds / 60))
				// Loop for ~20 minutes.
				const numIterations = int(20 / (collectionIntervalSeconds / 60))
				numErrors := 0
				numSuccesses := 0
				var l0SublevelCount []float64
				var diskWriteBytes []float64
				for i := 0; i < numIterations; i++ {
					time.Sleep(collectionIntervalSeconds * time.Second)

					// L0 Sublevels
					val, err := getMetricVal(subLevelMetric)
					if err != nil {
						numErrors++
						continue
					}
					l0SublevelCount = append(l0SublevelCount, val)
					if len(l0SublevelCount) >= sampleCount {
						latestSampleMeanL0Sublevels := getMeanOverLastN(sampleCount, l0SublevelCount)
						if latestSampleMeanL0Sublevels > subLevelThreshold {
							t.Fatalf("sub-level mean %f over last %d iterations exceeded threshold", latestSampleMeanL0Sublevels, sampleCount)
						}
					}

					// Disk Bandwidth
					val, err = getMetricVal(writeBytesMetric)
					if err != nil {
						numErrors++
						continue
					}
					diskWriteBytes = append(diskWriteBytes, val)
					if len(diskWriteBytes) >= sampleCount {
						diff := diskWriteBytes[len(diskWriteBytes)-1] - diskWriteBytes[len(diskWriteBytes)-1-sampleCount]
						rate := diff / (float64(sampleCount * collectionIntervalSeconds))
						if rate > bandwidthThreshold {
							t.Fatalf("disk write bandwidth rate %f over last %d seconds exceeded threshold of %f", rate, sampleCount*collectionIntervalSeconds, bandwidthThreshold)
						}
					}
					numSuccesses++
				}
				t.Status(fmt.Sprintf("done waiting errors: %d successes: %d", numErrors, numSuccesses))
				if numErrors > numSuccesses {
					t.Fatalf("too many errors retrieving metrics")
				}
				return nil
			})

			m.Wait()
		},
	})
}
