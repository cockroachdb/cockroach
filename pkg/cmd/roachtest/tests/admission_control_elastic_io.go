// Copyright 2023 The Cockroach Authors.
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

// This test sets up a 1 node CRDB cluster on an 8vCPU machine, runs low
// priority kv0 that will overload the compaction throughput (out of L0) of
// the store. With admission control subjecting this low priority load to
// elastic IO tokens, the overload is limited.
func registerElasticIO(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "admission-control/elastic-io",
		Owner:            registry.OwnerAdmissionControl,
		Timeout:          time.Hour,
		Benchmark:        true,
		CompatibleClouds: registry.AllExceptAWS,
		// TODO(sumeer): Reduce to weekly after working well.
		Suites: registry.Suites(registry.Nightly),
		// Tags:      registry.Tags(`weekly`),
		// Second node is solely for Prometheus.
		Cluster:         r.MakeClusterSpec(2, spec.CPU(8)),
		RequiresLicense: true,
		Leases:          registry.MetamorphicLeases,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.IsLocal() {
				t.Skip("IO overload test is not meant to run locally")
			}
			if c.Spec().NodeCount != 2 {
				t.Fatalf("expected 2 nodes, found %d", c.Spec().NodeCount)
			}
			crdbNodes := c.Spec().NodeCount - 1
			workAndPromNode := crdbNodes + 1

			promCfg := &prometheus.Config{}
			promCfg.WithPrometheusNode(c.Node(workAndPromNode).InstallNodes()[0]).
				WithNodeExporter(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
				WithCluster(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
				WithGrafanaDashboardJSON(grafana.ChangefeedAdmissionControlGrafana)
			err := c.StartGrafana(ctx, t.L(), promCfg)
			require.NoError(t, err)
			c.Put(ctx, t.DeprecatedWorkload(), "./workload", c.Node(workAndPromNode))
			startOpts := option.NewStartOpts(option.NoBackupSchedule)
			roachtestutil.SetDefaultAdminUIPort(c, &startOpts.RoachprodOpts)
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
				"--vmodule=io_load_listener=2")
			settings := install.MakeClusterSettings()
			c.Start(ctx, t.L(), startOpts, settings, c.Range(1, crdbNodes))
			promClient, err := clusterstats.SetupCollectorPromClient(ctx, c, t.L(), promCfg)
			require.NoError(t, err)
			statCollector := clusterstats.NewStatsCollector(ctx, promClient)
			setAdmissionControl(ctx, t, c, true)
			duration := 30 * time.Minute
			t.Status("running workload")
			m := c.NewMonitor(ctx, c.Range(1, crdbNodes))
			m.Go(func(ctx context.Context) error {
				dur := " --duration=" + duration.String()
				url := fmt.Sprintf(" {pgurl:1-%d}", crdbNodes)
				cmd := "./workload run kv --init --histograms=perf/stats.json --concurrency=512 " +
					"--splits=1000 --read-percent=0 --min-block-bytes=65536 --max-block-bytes=65536 " +
					"--background-qos=true --tolerate-errors --secure" + dur + url
				c.Run(ctx, c.Node(workAndPromNode), cmd)
				return nil
			})
			m.Go(func(ctx context.Context) error {
				const subLevelMetric = "storage_l0_sublevels"
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
				now := timeutil.Now()
				endTime := now.Add(duration)
				// We typically see fluctuations from 1 to 5 sub-levels because the
				// elastic IO token logic gives 1.25*compaction-bandwidth tokens at 1
				// sub-level and 0.75*compaction-bandwidth at 5 sub-levels, with 5
				// sub-levels being very rare. We leave some breathing room and pick a
				// threshold of greater than 7 to fail the test. If elastic tokens are
				// not working, the threshold of 7 will be easily breached, since
				// regular tokens allow sub-levels to exceed 10.
				const subLevelThreshold = 7
				const sampleCountForL0Sublevel = 12
				var l0SublevelCount []float64
				// Sleep initially for stability to be achieved, before measuring.
				time.Sleep(5 * time.Minute)
				for {
					time.Sleep(10 * time.Second)
					val, err := getMetricVal(subLevelMetric)
					if err != nil {
						continue
					}
					l0SublevelCount = append(l0SublevelCount, val)
					// We want to use the mean of the last 2m of data to avoid short-lived
					// spikes causing failures.
					if len(l0SublevelCount) >= sampleCountForL0Sublevel {
						latestSampleMeanL0Sublevels := getMeanOverLastN(sampleCountForL0Sublevel, l0SublevelCount)
						if latestSampleMeanL0Sublevels > subLevelThreshold {
							t.Fatalf("sub-level mean %f over last %d iterations exceeded threshold", latestSampleMeanL0Sublevels, sampleCountForL0Sublevel)
						}
					}
					if timeutil.Now().After(endTime) {
						return nil
					}
				}
			})
			m.Wait()
		},
	})
}
