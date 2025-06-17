// Copyright 2025 The Cockroach Authors.
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

// This test sets up 2 workloads – kv50 consisting of "normal" priority reads
// and writes and kv0 consisting of "background" priority writes. The goal is to
// show that even with a demanding "background" workload that is able to push
// the used IOPS much higher than the provisioned one, the AC IOPS control paces
// the traffic at the set IOPS limit.
func registerDiskIOPSOverload(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "admission-control/disk-iops-overload",
		Owner:            registry.OwnerAdmissionControl,
		Timeout:          3 * time.Hour,
		Benchmark:        true,
		CompatibleClouds: registry.OnlyGCE,
		// TODO(aaditya): change to weekly once the test stabilizes.
		Suites: registry.ManualOnly,
		Cluster: r.MakeClusterSpec(
			2, /* nodeCount*/
			spec.CPU(16),
			spec.WorkloadNode(),
			spec.ReuseNone(),
			spec.VolumeSize(100), // 100GB volume should provision 3K IOPS based on GCP docs.
			spec.DisableLocalSSD(),
		),
		Leases: registry.MetamorphicLeases,
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

			// Initialize the kv databases.
			t.Status(fmt.Sprintf("initializing kv dataset (<%s)", 2*time.Minute))
			url := fmt.Sprintf(" {pgurl%s}", c.CRDBNodes())
			const foregroundDB = " --db='regular_kv'"
			const backgroundDB = " --db='background_kv'"

			c.Run(ctx, option.WithNodes(c.WorkloadNode()),
				"./cockroach workload init kv --drop --insert-count=400 "+
					"--max-block-bytes=64 --min-block-bytes=64"+foregroundDB+url)

			c.Run(ctx, option.WithNodes(c.WorkloadNode()),
				"./cockroach workload init kv --drop --insert-count=400 "+
					"--max-block-bytes=64 --min-block-bytes=64"+backgroundDB+url)

			// Run foreground kv workload, QoS="regular".
			duration := 90 * time.Minute
			m := c.NewDeprecatedMonitor(ctx, c.CRDBNodes())
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
					"--splits=1000 --read-percent=50 --min-block-bytes=64 --max-block-bytes=64 "+
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
					"concurrency":  "500",
					"read-percent": "0",
				}
				cmd := fmt.Sprintf("./cockroach workload run kv %s --concurrency=500 "+
					"--read-percent=0 --min-block-bytes=64 --max-block-bytes=64 "+
					"--txn-qos='background' --tolerate-errors %s %s %s", roachtestutil.GetWorkloadHistogramArgs(t, c, labels), backgroundDB, dur, url)
				c.Run(ctx, option.WithNodes(c.WorkloadNode()), cmd)
				return nil
			})

			t.Status(fmt.Sprintf("waiting for workload to start and ramp up (%ss)", 60*time.Minute))
			time.Sleep(60 * time.Minute)

			db := c.Conn(ctx, t.L(), len(c.CRDBNodes()))
			defer db.Close()

			const IOPSLimit = 1000 // We want to validate the effectiveness of AC by setting this much less than the hardware limits.
			if _, err := db.ExecContext(
				// We intentionally set this to much lower than the provisioned value
				// above to clearly show that the bandwidth limiter works.
				ctx, fmt.Sprintf("SET CLUSTER SETTING kvadmission.store.provisioned_iops = '%d'", IOPSLimit)); err != nil {
				t.Fatalf("failed to set kvadmission.store.provisioned_iops: %v", err)
			}

			t.Status(fmt.Sprintf("setting iops limit, and waiting for it to take effect. (%s)", 5*time.Minute))
			time.Sleep(5 * time.Minute)

			m.Go(func(ctx context.Context) error {
				t.Status(fmt.Sprintf("starting monitoring thread (<%s)", time.Minute))
				writeIOPSMetric := "rate(sys_host_disk_write_count[1m])"
				readIOPSMetric := "rate(sys_host_disk_read_count[1m])"
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
				const IOPSThreshold = IOPSLimit * 1.05
				const collectionIntervalSeconds = 10.0
				const sampleCountForIOPS = 12
				// Loop for ~20 minutes.
				const numIterations = int(20 / (collectionIntervalSeconds / 60))
				var writeIOPSValues []float64
				numErrors := 0
				numSuccesses := 0
				for i := 0; i < numIterations; i++ {
					time.Sleep(collectionIntervalSeconds * time.Second)
					writeVal, err := getMetricVal(writeIOPSMetric, "node")
					if err != nil {
						numErrors++
						continue
					}
					readVal, err := getMetricVal(readIOPSMetric, "node")
					if err != nil {
						numErrors++
						continue
					}
					writeIOPSValues = append(writeIOPSValues, writeVal)
					totalIOPS := writeVal + readVal
					// We want to use the mean of the last 2m of data to avoid short-lived
					// spikes causing failures.
					if len(writeIOPSValues) >= sampleCountForIOPS {
						// TODO(aaditya): We should be asserting on total IOPS once reads
						// are being paced.
						latestSampleMeanForIOPS := roachtestutil.GetMeanOverLastN(sampleCountForIOPS, writeIOPSValues)
						if latestSampleMeanForIOPS > IOPSThreshold {
							t.Fatalf("mean write IOPS over the last 2m %f (last iter: %f) exceeded threshold of %f, read IOPS: %f, total IOPS: %f", latestSampleMeanForIOPS, writeVal, IOPSThreshold, readVal, totalIOPS)
						}
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
