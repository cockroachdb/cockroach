// Copyright 2023 The Cockroach Authors.
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
	gosql "database/sql"
	"fmt"
	"math"
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

// This test sets up a 1 node CRDB cluster on an 8vCPU machine, runs a txn
// that creates a huge number of intents, and then commits to resolve those
// intents. When intent resolution is not subject to admission control, the
// LSM gets overloaded and has > 50 sub-levels.
func registerIntentResolutionOverload(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:      "admission-control/intent-resolution",
		Owner:     registry.OwnerAdmissionControl,
		Timeout:   time.Hour,
		Benchmark: true,
		// TODO(sumeer): Reduce to weekly after working well.
		// Tags:      registry.Tags(`weekly`),
		// Second node is solely for Prometheus.
		Cluster:          r.MakeClusterSpec(2, spec.CPU(8)),
		RequiresLicense:  true,
		Leases:           registry.MetamorphicLeases,
		CompatibleClouds: registry.AllExceptAWS,
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Spec().NodeCount != 2 {
				t.Fatalf("expected 2 nodes, found %d", c.Spec().NodeCount)
			}
			crdbNodes := c.Spec().NodeCount - 1
			promNode := crdbNodes + 1

			promCfg := &prometheus.Config{}
			promCfg.WithPrometheusNode(c.Node(promNode).InstallNodes()[0]).
				WithNodeExporter(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
				WithCluster(c.Range(1, c.Spec().NodeCount-1).InstallNodes()).
				WithGrafanaDashboardJSON(grafana.ChangefeedAdmissionControlGrafana)
			err := c.StartGrafana(ctx, t.L(), promCfg)
			require.NoError(t, err)
			promClient, err := clusterstats.SetupCollectorPromClient(ctx, c, t.L(), promCfg)
			require.NoError(t, err)
			statCollector := clusterstats.NewStatsCollector(ctx, promClient)

			startOpts := option.DefaultStartOptsNoBackups()
			startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs,
				"--vmodule=io_load_listener=2")
			roachtestutil.SetDefaultSQLPort(c, &startOpts.RoachprodOpts)
			roachtestutil.SetDefaultAdminUIPort(c, &startOpts.RoachprodOpts)
			settings := install.MakeClusterSettings()
			c.Start(ctx, t.L(), startOpts, settings, c.Range(1, crdbNodes))
			setAdmissionControl(ctx, t, c, true)
			t.Status("running txn")
			m := c.NewMonitor(ctx, c.Range(1, crdbNodes))
			m.Go(func(ctx context.Context) error {
				db := c.Conn(ctx, t.L(), crdbNodes)
				defer db.Close()
				_, err := db.Exec(`CREATE TABLE test_table(id integer PRIMARY KEY, t TEXT)`)
				if err != nil {
					return err
				}
				tx, err := db.BeginTx(ctx, &gosql.TxOptions{})
				if err != nil {
					return err
				}
				query := `INSERT INTO test_table(id, t) SELECT i, sha512(random()::text) FROM ` +
					`generate_series(0, 75000000) AS t(i);`
				_, err = tx.ExecContext(ctx, query)
				if err != nil {
					return err
				}
				t.Status("intents created, committing txn")
				err = tx.Commit()
				if err != nil {
					return err
				}
				t.Status("waiting for async intent resolution to complete")
				const subLevelMetric = "storage_l0_sublevels"
				const intentCountMetric = "intentcount"
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
				// Loop for up to 20 minutes. Intents take ~10min to resolve, and
				// we're padding by another 10min.
				const subLevelThreshold = 20
				const sampleCountForL0Sublevel = 12
				numErrors := 0
				numSuccesses := 0
				latestIntentCount := math.MaxInt
				var l0SublevelCount []float64
				for i := 0; i < 120; i++ {
					time.Sleep(10 * time.Second)
					val, err := getMetricVal(subLevelMetric)
					if err != nil {
						numErrors++
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
					val, err = getMetricVal(intentCountMetric)
					if err != nil {
						numErrors++
						continue
					}
					numSuccesses++
					latestIntentCount = int(val)
					if latestIntentCount < 10 {
						break
					}
				}
				t.Status(fmt.Sprintf("done waiting errors: %d successes: %d, intent-count: %d",
					numErrors, numSuccesses, latestIntentCount))
				if latestIntentCount > 20 {
					t.Fatalf("too many intents left")
				}
				if numErrors > numSuccesses {
					t.Fatalf("too many errors retrieving metrics")
				}
				return nil
			})
			m.Wait()
		},
	})
}

// Returns the mean over the last n samples. If n > len(items), returns the mean
// over the entire items slice.
func getMeanOverLastN(n int, items []float64) float64 {
	count := n
	if len(items) < n {
		count = len(items)
	}
	sum := float64(0)
	i := 0
	for i < count {
		sum += items[len(items)-1-i]
		i++
	}
	return sum / float64(count)
}
