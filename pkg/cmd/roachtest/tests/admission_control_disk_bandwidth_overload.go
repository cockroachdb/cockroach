// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
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
		Timeout:          3 * time.Hour,
		Benchmark:        true,
		CompatibleClouds: registry.AllExceptAzure,
		// TODO(aaditya): change to weekly once the test stabilizes.
		Suites:  registry.Suites(registry.Nightly),
		Cluster: r.MakeClusterSpec(2, spec.CPU(8), spec.WorkloadNode(), spec.ReuseNone()),
		Leases:  registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Spec().NodeCount != 2 {
				t.Fatalf("expected 2 nodes, found %d", c.Spec().NodeCount)
			}

			promCfg := &prometheus.Config{}
			promCfg.WithPrometheusNode(c.WorkloadNode().InstallNodes()[0]).
				WithNodeExporter(c.CRDBNodes().InstallNodes()).
				WithCluster(c.CRDBNodes().InstallNodes()).
				WithGrafanaDashboardJSON(grafana.SnapshotAdmissionControlGrafanaJSON)
			err := c.StartGrafana(ctx, t.L(), promCfg)
			require.NoError(t, err)

			startOpts := option.NewStartOpts(option.NoBackupSchedule)
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
				"--vmodule=io_load_listener=2")
			roachtestutil.SetDefaultAdminUIPort(c, &startOpts.RoachprodOpts)
			settings := install.MakeClusterSettings()
			c.Start(ctx, t.L(), startOpts, settings, c.CRDBNodes())

			promClient, err := clusterstats.SetupCollectorPromClient(ctx, c, t.L(), promCfg)
			require.NoError(t, err)
			statCollector := clusterstats.NewStatsCollector(ctx, promClient)

			roachtestutil.SetAdmissionControl(ctx, t, c, true)

			const provisionedBandwidth = 128 << 20 // 128 MiB
			t.Status(fmt.Sprintf("limiting disk bandwidth to %d bytes/s", provisionedBandwidth))
			staller := roachtestutil.MakeCgroupDiskStaller(t, c,
				false /* readsToo */, false /* logsToo */)
			staller.Setup(ctx)
			staller.Slow(ctx, c.CRDBNodes(), provisionedBandwidth)

			// TODO(aaditya): Extend this test to also limit reads once we have a
			// mechanism to pace read traffic in AC.

			// Initialize the kv databases.
			t.Status(fmt.Sprintf("initializing kv dataset (<%s)", 2*time.Minute))
			url := fmt.Sprintf(" {pgurl%s}", c.CRDBNodes())
			const foregroundDB = " --db='regular_kv'"
			const backgroundDB = " --db='background_kv'"

			c.Run(ctx, option.WithNodes(c.WorkloadNode()),
				"./cockroach workload init kv --drop --insert-count=400 "+
					"--max-block-bytes=512 --min-block-bytes=512"+foregroundDB+url)

			c.Run(ctx, option.WithNodes(c.WorkloadNode()),
				"./cockroach workload init kv --drop --insert-count=400 "+
					"--max-block-bytes=4096 --min-block-bytes=4096"+backgroundDB+url)

			// Run foreground kv workload, QoS="regular".
			duration := 90 * time.Minute
			m := c.NewMonitor(ctx, c.CRDBNodes())
			m.Go(func(ctx context.Context) error {
				t.Status(fmt.Sprintf("starting foreground kv workload thread (<%s)", time.Minute))
				dur := " --duration=" + duration.String()
				labels := map[string]string{
					"concurrency":  "2",
					"splits":       "1000",
					"read-percent": "50",
				}
				url := fmt.Sprintf(" {pgurl%s}", c.CRDBNodes())
				cmd := fmt.Sprintf("./cockroach workload run kv %s --concurrency=2 "+
					"--splits=1000 --read-percent=50 --min-block-bytes=512 --max-block-bytes=512 "+
					"--txn-qos='regular' --tolerate-errors %s %s %s",
					roachtestutil.GetWorkloadHistogramArgs(t, c, labels), foregroundDB, dur, url)
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
				return nil
			})

			// Run background kv workload, QoS="background".
			m.Go(func(ctx context.Context) error {
				t.Status(fmt.Sprintf("starting background kv workload thread (<%s)", time.Minute))
				dur := " --duration=" + duration.String()
				url := fmt.Sprintf(" {pgurl%s}", c.CRDBNodes())
				labels := map[string]string{
					"concurrency":  "1024",
					"read-percent": "0",
				}
				cmd := fmt.Sprintf("./cockroach workload run kv %s --concurrency=1024 "+
					"--read-percent=0 --min-block-bytes=4096 --max-block-bytes=4096 "+
					"--txn-qos='background' --tolerate-errors %s %s %s", roachtestutil.GetWorkloadHistogramArgs(t, c, labels), backgroundDB, dur, url)
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
				return nil
			})

			t.Status(fmt.Sprintf("waiting for workload to start and ramp up (<%s)", 30*time.Minute))
			time.Sleep(60 * time.Minute)

			db := c.Conn(ctx, t.L(), len(c.CRDBNodes()))
			defer db.Close()

			const bandwidthLimit = 75
			if _, err := db.ExecContext(
				// We intentionally set this to much lower than the provisioned value
				// above to clearly show that the bandwidth limiter works.
				ctx, fmt.Sprintf("SET CLUSTER SETTING kvadmission.store.provisioned_bandwidth = '%dMiB'", bandwidthLimit)); err != nil {
				t.Fatalf("failed to set kvadmission.store.provisioned_bandwidth: %v", err)
			}

			t.Status(fmt.Sprintf("setting bandwidth limit, and waiting for it to take effect. (<%s)", 2*time.Minute))
			time.Sleep(5 * time.Minute)

			m.Go(func(ctx context.Context) error {
				t.Status(fmt.Sprintf("starting monitoring thread (<%s)", time.Minute))
				writeBWMetric := divQuery("rate(sys_host_disk_write_bytes[1m])", 1<<20 /* 1MiB */)
				readBWMetric := divQuery("rate(sys_host_disk_read_bytes[1m])", 1<<20 /* 1MiB */)
				getMetricVal := func(query string, label string) (float64, error) {
					point, err := statCollector.CollectPoint(ctx, t.L(), timeutil.Now(), query)
					if err != nil {
						t.L().Errorf("could not query prom %s", err.Error())
						return 0, err
					}
					val := point[label]
					if len(val) != 1 {
						err = errors.Errorf(
							"unexpected number %d of points for metric %s", len(val), query)
						t.L().Errorf("%s", err.Error())
						return 0, err
					}
					for storeID, v := range val {
						t.L().Printf("%s(store=%s): %f", query, storeID, v.Value)
						return v.Value, nil
					}
					// Unreachable.
					panic("unreachable")
				}

				// Allow a 5% room for error.
				const bandwidthThreshold = bandwidthLimit * 1.05
				const collectionIntervalSeconds = 10.0
				// Loop for ~20 minutes.
				const numIterations = int(20 / (collectionIntervalSeconds / 60))
				numErrors := 0
				numSuccesses := 0
				for i := 0; i < numIterations; i++ {
					time.Sleep(collectionIntervalSeconds * time.Second)
					writeVal, err := getMetricVal(writeBWMetric, "node")
					if err != nil {
						numErrors++
						continue
					}
					readVal, err := getMetricVal(readBWMetric, "node")
					if err != nil {
						numErrors++
						continue
					}
					totalBW := writeVal + readVal
					// TODO(aaditya): We should be asserting on total bandwidth once reads
					// are being paced.
					if writeVal > bandwidthThreshold {
						t.Fatalf("write bandwidth %f exceeded threshold of %f, read bandwidth: %f, total bandwidth: %f", writeVal, bandwidthThreshold, readVal, totalBW)
					}
					numSuccesses++
				}
				t.Status(fmt.Sprintf("done monitoring, errors: %d successes: %d", numErrors, numSuccesses))
				if numErrors > numSuccesses {
					t.Fatalf("too many errors retrieving metrics")
				}
				return nil
			})

			m.Wait()
		},
	})
}
