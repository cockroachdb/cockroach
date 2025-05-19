// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
)

type tpchBenchSpec struct {
	Nodes       int
	CPUs        int
	ScaleFactor int
	benchType   string
	url         string
	// numQueries must match the number of queries in the file specified in url.
	numQueries      int
	numRunsPerQuery int
	// maxLatency is the expected maximum time that a query will take to execute
	// needed to correctly initialize histograms.
	maxLatency time.Duration
}

// runTPCHBench runs sets of queries against CockroachDB clusters in different
// configurations.
//
// In order to run a benchmark, a TPC-H dataset must first be loaded. To reuse
// this data across runs, it is recommended to use a combination of
// `--cluster=<cluster>` and `--wipe=false` flags to limit the loading phase to
// the first run.
//
// This benchmark runs with a single load generator node running a single
// worker.
func runTPCHBench(ctx context.Context, t test.Test, c cluster.Cluster, b tpchBenchSpec) {
	filename := b.benchType
	t.Status(fmt.Sprintf("downloading %s query file from %s", filename, b.url))
	if err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), fmt.Sprintf("curl %s > %s", b.url, filename)); err != nil {
		t.Fatal(err)
	}

	t.Status("starting nodes")
	c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), install.MakeClusterSettings(), c.CRDBNodes())

	m := c.NewMonitor(ctx, c.CRDBNodes())
	m.Go(func(ctx context.Context) error {
		conn := c.Conn(ctx, t.L(), 1)
		defer conn.Close()

		t.Status("setting up dataset")
		err := loadTPCHDataset(
			ctx, t, c, conn, b.ScaleFactor, m, c.CRDBNodes(), true, /* disableMergeQueue */
		)
		if err != nil {
			return err
		}

		t.L().Printf("running %s benchmark on tpch scale-factor=%d", filename, b.ScaleFactor)

		// maxOps flag will allow us to exit the workload once all the queries were
		// run b.numRunsPerQuery number of times.
		maxOps := b.numRunsPerQuery * b.numQueries

		labels := map[string]string{
			"max_ops":     fmt.Sprintf("%d", maxOps),
			"num_queries": fmt.Sprintf("%d", b.numQueries),
		}

		// Run with only one worker to get best-case single-query performance.
		cmd := fmt.Sprintf(
			"./workload run querybench --db=tpch --concurrency=1 --query-file=%s "+
				"--num-runs=%d --max-ops=%d {pgurl%s} %s --histograms-max-latency=%s",
			filename,
			b.numRunsPerQuery,
			maxOps,
			c.CRDBNodes(),
			roachtestutil.GetWorkloadHistogramArgs(t, c, labels),
			b.maxLatency.String(),
		)
		if err := c.RunE(ctx, option.WithNodes(c.WorkloadNode()), cmd); err != nil {
			t.Fatal(err)
		}
		return nil
	})
	m.Wait()
}

func registerTPCHBenchSpec(r registry.Registry, b tpchBenchSpec) {
	nameParts := []string{
		"tpchbench",
		b.benchType,
		fmt.Sprintf("nodes=%d", b.Nodes),
		fmt.Sprintf("cpu=%d", b.CPUs),
		fmt.Sprintf("sf=%d", b.ScaleFactor),
	}

	// Add a load generator node.
	numNodes := b.Nodes + 1

	r.Add(registry.TestSpec{
		Name:      strings.Join(nameParts, "/"),
		Owner:     registry.OwnerSQLQueries,
		Benchmark: true,
		Cluster:   r.MakeClusterSpec(numNodes, spec.WorkloadNode()),
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds:           registry.Clouds(spec.GCE, spec.Local),
		Suites:                     registry.Suites(registry.Nightly),
		RequiresDeprecatedWorkload: true, // uses querybench
		PostProcessPerfMetrics: func(test string, histograms *roachtestutil.HistogramMetric) (roachtestutil.AggregatedPerfMetrics, error) {

			// To calculate the total mean of the run, we store the sum of means and count of the means
			// We can't get the sum of the values since roachtestutil.HistogramSummaryMetric doesn't have
			// sum of the values.
			// This is an approximation
			totalMeanSum := 0.0
			totalMeanCount := 0.0
			for _, summary := range histograms.Summaries {
				for _, summaryMetric := range summary.Values {
					totalMeanSum += float64(summaryMetric.Mean)
				}
				totalMeanCount++
			}

			aggregatedMetrics := roachtestutil.AggregatedPerfMetrics{
				{
					Name:           test + "_mean_latency",
					Value:          roachtestutil.MetricPoint(totalMeanSum / totalMeanCount),
					Unit:           "ms",
					IsHigherBetter: false,
				},
			}

			return aggregatedMetrics, nil
		},
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCHBench(ctx, t, c, b)
		},
	})
}

func registerTPCHBench(r registry.Registry) {
	specs := []tpchBenchSpec{
		{
			Nodes:           3,
			CPUs:            4,
			ScaleFactor:     1,
			benchType:       `sql20`,
			url:             `https://raw.githubusercontent.com/cockroachdb/cockroach/master/pkg/workload/querybench/2.1-sql-20`,
			numQueries:      14,
			numRunsPerQuery: 3,
			maxLatency:      100 * time.Second,
		},
		{
			Nodes:           3,
			CPUs:            4,
			ScaleFactor:     1,
			benchType:       `tpch`,
			url:             `https://raw.githubusercontent.com/cockroachdb/cockroach/master/pkg/workload/querybench/tpch-queries`,
			numQueries:      22,
			numRunsPerQuery: 3,
			maxLatency:      500 * time.Second,
		},
	}

	for _, b := range specs {
		registerTPCHBenchSpec(r, b)
	}
}
