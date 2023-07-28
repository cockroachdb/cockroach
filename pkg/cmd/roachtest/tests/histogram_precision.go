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
	"bufio"
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"gonum.org/v1/gonum/floats"
	"gonum.org/v1/gonum/stat"
	"gonum.org/v1/gonum/stat/distuv"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
)

// This test sets up a 5-node CRDB cluster on 8vCPU machines running
// 1000-warehouse TPC-C, and records prometheus metrics for that cluster.
func registerHistogramBuckets(r registry.Registry) {
	r.Add(makeHistogramBuckets(r.MakeClusterSpec(5, spec.CPU(8)), 1000,
		time.Minute*5))
}

type simpleHist struct {
	sum     float64
	count   int
	buckets map[string]int
}

func makeHistogramBuckets(
	spec spec.ClusterSpec, warehouses int, length time.Duration,
) registry.TestSpec {
	return registry.TestSpec{
		Name:      "histogram-buckets",
		Owner:     registry.OwnerObsInf,
		Benchmark: false,
		Tags:      registry.Tags(`weekly`),
		Cluster:   spec,
		Leases:    registry.MetamorphicLeases,
		Timeout:   length * 3,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			if c.Spec().NodeCount < 4 {
				t.Fatalf("expected at least 4 nodes, found %d", c.Spec().NodeCount)
			}

			numWarehouses, workloadDuration, estimatedSetupTime := warehouses,
				length, 10*time.Minute
			if c.IsLocal() {
				numWarehouses, workloadDuration, estimatedSetupTime = 1, time.Minute,
					2*time.Minute
			}
			workloadNode := c.Spec().NodeCount
			crdbNodes := c.Range(1, c.Spec().NodeCount-1)

			promCfg := &prometheus.Config{}
			promCfg.WithPrometheusNode(c.Node(workloadNode).InstallNodes()[0]).
				WithNodeExporter(crdbNodes.InstallNodes()).
				WithCluster(crdbNodes.InstallNodes()).
				WithScrapeConfigs(
					prometheus.MakeWorkloadScrapeConfig("workload", "/",
						makeWorkloadScrapeNodes(
							c.Node(workloadNode).InstallNodes()[0],
							[]workloadInstance{{nodes: c.Node(workloadNode)}},
						),
					),
				)
			promCfg.Grafana.Enabled = false

			// runTPCC starts a prometheus instance and dumps data to local artifacts
			// directory on shutdown
			runTPCC(ctx, t, c, tpccOptions{
				Warehouses:                    numWarehouses,
				Duration:                      workloadDuration,
				SetupType:                     usingImport,
				EstimatedSetupTime:            estimatedSetupTime,
				SkipPostRunCheck:              true,
				DisableDefaultScheduledBackup: true,
				Start: func(ctx context.Context, t test.Test,
					c cluster.Cluster) {
					c.Put(ctx, t.Cockroach(), "./cockroach",
						c.All())
					settings := install.MakeClusterSettings()
					if c.IsLocal() {
						settings.Env = append(settings.Env, "COCKROACH_SCAN_INTERVAL=200ms")
						settings.Env = append(settings.Env, "COCKROACH_SCAN_MAX_IDLE_TIME=5ms")
					}
					settings.Env = append(settings.Env,
						"COCKROACH_HISTOGRAM_PRECISION_TESTING=true")
					startOpts := option.DefaultStartOpts()
					c.Start(ctx, t.L(), startOpts, settings, crdbNodes)
				},
				/*
					During: func(ctx context.Context) error {
						return runAndLogStmts(ctx, t, c, "triggermetrics", []string{
							`SELECT *;`, // trigger histogram metrics?
						})
					},
				*/
			})
			// get values for all histogram metrics
			histogramValues, err := getHistogramData(ctx, t, c)
			if err != nil {
				t.Fatal(err)
			}
			// flatten histogram for sample values
			for metric, hist := range histogramValues {
				sample, err := flattenHistogram(hist)
				if err != nil {
					t.Fatal(err)
				}
				fit, err := fitDist(sample, hist.count, hist.sum)
				if err != nil {
					t.Fatal(err)
				}
				t.L().Printf("%s fits a %s distribution", metric, fit)
			}
		},
	}
}

// getHistogramData returns a map of metric->histogram values for each node in
// the cluster.
func getHistogramData(ctx context.Context, t test.Test,
	c cluster.Cluster) (map[string]simpleHist, error) {
	histograms := make(map[string]simpleHist)
	getData := func(ctx context.Context, node int) func() error {
		return func() error {
			adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), c.Node(node))
			if err != nil {
				return err
			}
			url := "http://" + adminUIAddrs[0] + "/_status/vars"
			resp, err := httputil.Get(ctx, url)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				return errors.Errorf("invalid non-200 status code %v from node %d",
					resp.StatusCode, node)
			}
			scanner := bufio.NewScanner(resp.Body)
			for scanner.Scan() {
				m, ok := parsePrometheusMetric(scanner.Text())
				if ok {
					if strings.Contains(m.metric, "_bucket") {
						metricName := strings.Split(m.metric, "_bucket")[0]
						leVal := strings.Split(m.labelValues, "le=")[1]
						val, err := strconv.Atoi(m.value)
						if err != nil {
							return err
						}
						histograms[metricName].buckets[leVal] = val
					} else if strings.Contains(m.metric, "_sum") {
						metricName := strings.Split(m.metric, "_sum")[0]
						val, err := strconv.ParseFloat(m.value, 64)
						if err != nil {
							return err
						}
						if hist, ok := histograms[metricName]; ok {
							hist.sum = val
							histograms[metricName] = hist
						}
					}
				}
			}
			return nil
		}
	}
	g, gCtx := errgroup.WithContext(ctx)
	for i := 1; i <= c.Spec().NodeCount; i++ {
		g.Go(getData(gCtx, i))
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return histograms, nil
}

func flattenHistogram(hist simpleHist) ([]float64, error) {
	sample := make([]float64, hist.count)
	for le, count := range hist.buckets {
		var val float64
		for i := 0; i < count; i++ {
			if le != "inf+" {
				var err error
				val, err = strconv.ParseFloat(le, 64)
				if err != nil {
					return nil, err
				}
			} else {
				val = sample[len(sample)-1]
			}
			sample = append(sample, val)
		}
	}
	return sample, nil
}

// Want to run a goodness-of-fit test of these buckets against some standard
// probability distributions. Additionally, we want to make sure that we aren't
// cutting off a significant number of observations that surpass the uppermost
// threshold of buckets. Outliers are expected, and should be part of the fit.
func fitDist(sample []float64, count int, sum float64) (metric.Distribution, error) {

	var fit metric.Distribution

	var (
		min       = sample[0]
		max       = sample[len(sample)-1]
		mean      = sum / float64(count)
		stdev     = stat.StdDev(sample, nil)
		expGrowth = math.Pow(max/min, 1.0/float64(count-1))
		uni       = distuv.Uniform{Min: min, Max: max}
		norm      = distuv.Normal{Mu: mean, Sigma: stdev}
		lognorm   = distuv.LogNormal{Mu: mean, Sigma: stdev}
		expo      = distuv.Exponential{Rate: expGrowth}
		inv       = 1.0 / float64(count)
		exp       = make([]float64, count)
		xedf      = make([]float64, count)
		diff      = make([]float64, count)
		// K-S critical value at the 95% confidence level, n > 40
		crit = 1.36 / math.Sqrt(float64(count))
	)

	// normal
	for i, v := range sample {
		xedf[i] = inv * float64(i)
		exp[i] = norm.CDF(v)
		diff[i] = math.Abs(xedf[i] - exp[i])
	}
	dist := stat.KolmogorovSmirnov(sample, xedf, sample, exp)
	fmt.Printf("stat:     %d\n", count)
	fmt.Printf("diff:     %2.3f\n", floats.Max(diff))
	fmt.Printf("dist-KS:  %2.3f\n", dist)
	fmt.Printf("dist(3σ): %2.3f\n", crit)

	h0 := "reject normal distribution"
	if dist < crit {
		h0 = "do not reject normal distribution"
		fit = metric.Normal
	}
	fmt.Printf("H0:       %s\n", h0)

	// lognormal
	for i, v := range sample {
		exp[i] = lognorm.CDF(v)
		diff[i] = math.Abs(xedf[i] - exp[i])
	}
	dist = stat.KolmogorovSmirnov(sample, xedf, sample, exp)
	fmt.Printf("stat:     %d\n", count)
	fmt.Printf("diff:     %2.3f\n", floats.Max(diff))
	fmt.Printf("dist-KS:  %2.3f\n", dist)
	fmt.Printf("dist(3σ): %2.3f\n", crit)

	h0 = "reject lognormal distribution"
	if dist < crit {
		h0 = "do not reject lognormal distribution"
		fit = metric.LogNormal
	}
	fmt.Printf("H0:       %s\n", h0)

	// exponential
	for i, v := range sample {
		exp[i] = expo.CDF(v)
		diff[i] = math.Abs(xedf[i] - exp[i])
	}
	dist = stat.KolmogorovSmirnov(sample, xedf, sample, exp)
	fmt.Printf("stat:     %d\n", count)
	fmt.Printf("diff:     %2.3f\n", floats.Max(diff))
	fmt.Printf("dist-KS:  %2.3f\n", dist)
	fmt.Printf("dist(3σ): %2.3f\n", crit)

	h0 = "reject exponential distribution"
	if dist < crit {
		h0 = "do not reject exponential distribution"
		fit = metric.Exponential
	}
	fmt.Printf("H0:       %s\n", h0)

	// uniform
	for i, v := range sample {
		exp[i] = uni.CDF(v)
		diff[i] = math.Abs(xedf[i] - exp[i])
	}
	dist = stat.KolmogorovSmirnov(sample, xedf, sample, exp)
	fmt.Printf("stat:     %d\n", count)
	fmt.Printf("diff:     %2.3f\n", floats.Max(diff))
	fmt.Printf("dist-KS:  %2.3f\n", dist)
	fmt.Printf("dist(3σ): %2.3f\n", crit)

	h0 = "reject uniform distribution"
	if dist < crit {
		h0 = "do not reject uniform distribution"
		fit = metric.Uniform
	}
	fmt.Printf("H0:       %s\n", h0)

	return fit, nil
}
