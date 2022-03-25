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
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/ttycolor"
)

type tpccQoSBackgroundOLAPSpec struct {
	Nodes           int
	CPUs            int
	Warehouses      int
	Concurrency     int
	OLAPConcurrency int
	Duration        time.Duration
	Ramp            time.Duration
	numRuns         int
}

// run sets up TPCC then runs both TPCC plus TPCH OLAP queries concurrently,
// first with the OLAP queries using `background` transaction quality of
// service, then with OLAP queries using `regular` transaction quality of
// service.  The results are logged and compared. The test fails if using
// `background` QoS doesn't result in a tpmC score from TPCC which is at least a
// certain percentage higher than that when OLAP queries are run with `regular`
// QoS.
func (s tpccQoSBackgroundOLAPSpec) run(ctx context.Context, t test.Test, c cluster.Cluster) {

	histogramsPath := t.PerfArtifactsDir() + "/stats.json"
	histogramsPathQoS := t.PerfArtifactsDir() + "/stats_qos.json"

	var throttledOLAPTpmC, throttledOLAPTPCCEfficiency []float64
	var noThrottleTpmC, noThrottleTPCCEfficiency []float64
	throttledOLAPTpmC = make([]float64, 0, s.numRuns)
	throttledOLAPTPCCEfficiency = make([]float64, 0, s.numRuns)
	noThrottleTpmC = make([]float64, 0, s.numRuns)
	noThrottleTPCCEfficiency = make([]float64, 0, s.numRuns)
	var tpmCQoS, tpmCNoQoS float64
	var efficiencyCQoS, efficiencyNoQoS float64
	//var baselineTpmC, baselineEfficiency float64

	for i := 0; i < s.numRuns; i++ {
		crdbNodes, workloadNode := s.setupDatabases(ctx, t, c)

		//s.runTPCC(ctx, t, c, crdbNodes, workloadNode, histogramsPath, false /* useBackgroundQoS */)
		//baselineTpmC, baselineEfficiency, _ =
		//	s.getTpmcAndEfficiency(ctx, t, c, workloadNode, histogramsPath, true /* withQoS */)

		startQoS := timeutil.Now()
		s.runTPCCAndOLAPQueries(ctx, t, c, crdbNodes, workloadNode, histogramsPathQoS, true /* useBackgroundQoS */)
		endQoS := timeutil.Now()
		// Get the TPCC perf and efficiency when other OLAP queries are run with
		// background QoS.
		tpmC, efficiency, throttledResult :=
			s.getTpmcAndEfficiency(ctx, t, c, workloadNode, histogramsPathQoS, true /* withQoS */)
		tpmCQoS += tpmC
		efficiencyCQoS += efficiency
		throttledOLAPTpmC = append(throttledOLAPTpmC, tpmC)
		throttledOLAPTPCCEfficiency = append(throttledOLAPTPCCEfficiency, efficiency)

		startNoQoS := timeutil.Now()
		s.runTPCCAndOLAPQueries(ctx, t, c, crdbNodes, workloadNode, histogramsPath, false /* useBackgroundQoS */)
		endNoQoS := timeutil.Now()

		// Get the TPCC perf and efficiency when other OLAP queries are run with
		// regular QoS.
		tpmC, efficiency, noThrottleResult :=
			s.getTpmcAndEfficiency(ctx, t, c, workloadNode, histogramsPath, false /* withQoS */)
		tpmCNoQoS += tpmC
		efficiencyNoQoS += efficiency
		noThrottleTpmC = append(noThrottleTpmC, tpmC)
		noThrottleTPCCEfficiency = append(noThrottleTPCCEfficiency, efficiency)

		printResults(throttledResult, noThrottleResult, t)

		t.L().Printf("\n")
		t.L().Printf("Admission control metrics without QoS\n")
		t.L().Printf("-------------------------------------\n")
		printAdmissionMetrics(ctx, c, t, c.Node(1), startNoQoS, endNoQoS)
		t.L().Printf("\n")
		t.L().Printf("Admission control metrics with QoS\n")
		t.L().Printf("----------------------------------\n")
		printAdmissionMetrics(ctx, c, t, c.Node(1), startQoS, endQoS)
		t.L().Printf("\n")
	}
	efficiencyNoQoS /= float64(s.numRuns)
	efficiencyCQoS /= float64(s.numRuns)
	tpmCNoQoS /= float64(s.numRuns)
	tpmCQoS /= float64(s.numRuns)

	// Test results vary. Allow at most a 5% regression or random variance in the
	// run using background QoS.
	const maxAllowedRegression = -5.0
	percentImprovement := 100.0 * (tpmCQoS - tpmCNoQoS) / tpmCNoQoS
	//t.L().Printf("tpmC_No_Tpch:         %.2f   Efficiency: %.4v\n",
	//	baselineTpmC, baselineEfficiency)
	t.L().Printf("tpmC_No_QoS:         %.2f   Efficiency: %.4v\n",
		tpmCNoQoS, efficiencyNoQoS)
	t.L().Printf("tpmC_Background_QoS: %.2f   Efficiency: %.4v\n",
		tpmCQoS, efficiencyCQoS)
	var scoreDelta string
	if percentImprovement < 0.0 {
		scoreDelta = fmt.Sprintf("%.1f%% lower", -percentImprovement)
	} else {
		scoreDelta = fmt.Sprintf("%.1f%% higher", percentImprovement)
	}
	message := fmt.Sprintf(
		`TPCC run in parallel with OLAP queries using background QoS
                                                   had a tpmC score %s than with regular QoS.`,
		scoreDelta)
	if percentImprovement < maxAllowedRegression {
		ttycolor.Stdout(ttycolor.Red)
		failMessage := fmt.Sprintf("FAIL: %s\n", message)
		t.L().Printf(failMessage)
		ttycolor.Stdout(ttycolor.Reset)
		t.Fatalf(failMessage)
	} else {
		ttycolor.Stdout(ttycolor.Green)
		t.L().Printf("SUCCESS: %s\n", message)
	}
	ttycolor.Stdout(ttycolor.Reset)
}

func printAdmissionMetrics(
	ctx context.Context,
	c cluster.Cluster,
	t test.Test,
	adminNode option.NodeListOption,
	start, end time.Time,
) {
	// Query needed information over the timespan of the query.
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), adminNode)
	if err != nil {
		t.Fatal(err)
	}
	var admissionMetrics = []tsQuery{
		// Work Queue Admission Counter
		{name: "admission.requested.kv", queryType: total},
		{name: "admission.admitted.kv", queryType: total},
		{name: "admission.errored", queryType: total},
		{name: "admission.requested.kv-stores", queryType: total},
		{name: "admission.admitted.kv-stores", queryType: total},
		{name: "admission.errored.kv-stores", queryType: total},
		{name: "admission.requested.sql-kv-response", queryType: total},
		{name: "admission.admitted.sql-kv-response", queryType: total},
		{name: "admission.errored.sql-kv-response", queryType: total},
		{name: "admission.requested.sql-sql-response", queryType: total},
		{name: "admission.admitted.sql-sql-response", queryType: total},
		{name: "admission.errored.sql-sql-response", queryType: total},
		{name: "admission.requested.sql-leaf-start", queryType: total},
		{name: "admission.admitted.sql-leaf-start", queryType: total},
		{name: "admission.errored.sql-leaf-start", queryType: total},
		{name: "admission.requested.sql-root-start", queryType: total},
		{name: "admission.admitted.sql-root-start", queryType: total},
		{name: "admission.errored.sql-root-start", queryType: total},
		// Work Queue Length
		{name: "admission.wait_queue_length.kv", queryType: total},
		{name: "admission.wait_queue_length.kv-stores", queryType: total},
		{name: "admission.wait_queue_length.sql-kv-response", queryType: total},
		{name: "admission.wait_queue_length.sql-sql-response", queryType: total},
		{name: "admission.wait_queue_length.sql-leaf-start", queryType: total},
		{name: "admission.wait_queue_length.sql-root-start", queryType: total},

		// Work Queue Admission Latency Sum
		{name: "admission.wait_sum.kv", queryType: total},
		{name: "admission.wait_sum.kv-stores", queryType: total},
		{name: "admission.wait_sum.sql-kv-response", queryType: total},
		{name: "admission.wait_sum.sql-sql-response", queryType: total},
		{name: "admission.wait_sum.sql-leaf-start", queryType: total},
		{name: "admission.wait_sum.sql-root-start", queryType: total},
		// Granter
		{name: "admission.granter.total_slots.kv", queryType: total},
		{name: "admission.granter.used_slots.kv", queryType: total},
		{name: "admission.granter.used_slots.sql-leaf-start", queryType: total},
		{name: "admission.granter.used_slots.sql-root-start", queryType: total},

		// IO Tokens Exhausted Duration Sum
		{name: "admission.granter.io_tokens_exhausted_duration.kv", queryType: total},
	}

	adminURL := adminUIAddrs[0]
	response := mustGetMetrics(t, adminURL, start, end, admissionMetrics)

	// Drop the first two minutes of datapoints as a "ramp-up" period.
	//perMinute := response.Results[0].Datapoints[2:]
	//cumulative := response.Results[1].Datapoints[2:]  // msirek-temp

	t.L().Printf("\n")
	for i, metric := range admissionMetrics {
		lastIdx := len(response.Results[i].Datapoints) - 1
		if lastIdx < 1 {
			continue
		}
		dataPoints := response.Results[i].Datapoints
		totalCount := dataPoints[lastIdx].Value - dataPoints[0].Value
		t.L().Printf("s: %f\n", metric.name, totalCount)
	}
	t.L().Printf("\n")
}

func (s tpccQoSBackgroundOLAPSpec) setupDatabases(
	ctx context.Context, t test.Test, c cluster.Cluster,
) (crdbNodes, workloadNode option.NodeListOption) {
	// Set up TPCC tables.
	crdbNodes, workloadNode = setupTPCC(
		ctx, t, c, tpccOptions{
			Warehouses: s.Warehouses, SetupType: usingImport, DontOverrideWarehouses: true,
		})
	m := c.NewMonitor(ctx, crdbNodes)
	// Set up TPCH tables.
	m.Go(func(ctx context.Context) error {
		t.Status("loading TPCH tables")
		cmd := fmt.Sprintf(
			"./workload init tpch {pgurl:1-%d} --data-loader=import",
			c.Spec().NodeCount-1,
		)
		c.Run(ctx, workloadNode, cmd)
		return nil
	})
	m.Wait()
	return crdbNodes, workloadNode
}

func (s tpccQoSBackgroundOLAPSpec) runTPCCAndOLAPQueries(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	crdbNodes, workloadNode option.NodeListOption,
	histogramsPath string,
	useBackgroundQoS bool,
) {
	m := c.NewMonitor(ctx, crdbNodes)
	// Kick off TPC-H with concurrency.
	m.Go(func(ctx context.Context) error {
		var backgroundQoSOpt string
		message := fmt.Sprintf("running TPCH with concurrency of %d", s.OLAPConcurrency)
		if useBackgroundQoS {
			message += " with background quality of service"
			backgroundQoSOpt = "--background-qos"
		}
		t.Status(message)
		cmd := fmt.Sprintf(
			"./cockroach workload run tpch {pgurl:1-%d} --tolerate-errors "+
				"--concurrency=%d --duration=%s %s",
			c.Spec().NodeCount-1, s.OLAPConcurrency, s.Duration+s.Ramp, backgroundQoSOpt,
		)
		c.Run(ctx, workloadNode, cmd)
		return nil
	})
	s.runTPCC(ctx, t, c, crdbNodes, workloadNode, histogramsPath, useBackgroundQoS)
	m.Wait()
}

func (s tpccQoSBackgroundOLAPSpec) runTPCC(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	crdbNodes, workloadNode option.NodeListOption,
	histogramsPath string,
	useBackgroundQoS bool,
) {
	// Kick off TPC-C
	m := c.NewMonitor(ctx, crdbNodes)
	m.Go(func(ctx context.Context) error {
		message := "running tpcc"
		t.WorkerStatus(message)
		cmd := fmt.Sprintf(
			"./cockroach workload run tpcc"+
				" --tolerate-errors"+
				" --max-rate=0"+
				" --wait=0"+
				" --warehouses=%d"+
				" --concurrency=%d"+
				" --histograms=%s "+
				" --ramp=%s "+
				" --duration=%s {pgurl:1-%d}",
			s.Warehouses, s.Concurrency, histogramsPath, s.Ramp, s.Duration, c.Spec().NodeCount-1)
		c.Run(ctx, workloadNode, cmd)
		return nil
	})
	m.Wait()
}

func (s tpccQoSBackgroundOLAPSpec) getTpmcAndEfficiency(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	workloadNode option.NodeListOption,
	histogramsPath string,
	withQoS bool,
) (tpmC float64, efficiency float64, result *tpcc.Result) {
	var fileName string
	if withQoS {
		fileName = "stats_qos.json"
	} else {
		fileName = "stats.json"
	}
	localHistPath := filepath.Join(t.ArtifactsDir(), fileName)
	// Copy the performance results from the workloadNode to the local system
	// where roachtest is being run.
	if err := c.Get(ctx, t.L(), histogramsPath, localHistPath, workloadNode); err != nil {
		t.Fatal(err)
	}

	snapshots, err := histogram.DecodeSnapshots(localHistPath)
	if err != nil {
		t.Fatal(err)
	}
	result = tpcc.NewResultWithSnapshots(s.Warehouses, 0, snapshots)
	tpmC = result.TpmC()
	efficiency = result.Efficiency()
	return tpmC, efficiency, result
}

func printResults(throttledResult *tpcc.Result, noThrottleResult *tpcc.Result, t test.Test) {
	t.L().Printf("\n")
	t.L().Printf("TPCC results with OLAP queries running simultaneously\n")
	t.L().Printf("-----------------------------------------------------\n")
	printOneResult(noThrottleResult, t)
	t.L().Printf("\n\n")
	t.L().Printf("TPCC results with OLAP queries running simultaneously with background QoS\n")
	t.L().Printf("-------------------------------------------------------------------------\n")
	printOneResult(throttledResult, t)
	t.L().Printf("\n\n")
}

func printOneResult(res *tpcc.Result, t test.Test) {
	t.L().Printf("Duration: %.5v, Warehouses: %v, Efficiency: %.4v, tpmC: %.2f\n",
		res.Elapsed, res.ActiveWarehouses, res.Efficiency(), res.TpmC())
	t.L().Printf("_elapsed___ops/sec(cum)__p50(ms)__p90(ms)__p95(ms)__p99(ms)_pMax(ms)\n")

	var queries []string
	for query := range res.Cumulative {
		queries = append(queries, query)
	}
	sort.Strings(queries)
	for _, query := range queries {
		hist := res.Cumulative[query]
		t.L().Printf("%7.1fs %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f %s\n",
			res.Elapsed.Seconds(),
			float64(hist.TotalCount())/res.Elapsed.Seconds(),
			time.Duration(hist.ValueAtQuantile(50)).Seconds()*1000,
			time.Duration(hist.ValueAtQuantile(90)).Seconds()*1000,
			time.Duration(hist.ValueAtQuantile(95)).Seconds()*1000,
			time.Duration(hist.ValueAtQuantile(99)).Seconds()*1000,
			time.Duration(hist.ValueAtQuantile(100)).Seconds()*1000,
			query,
		)
	}
}

func (s tpccQoSBackgroundOLAPSpec) getArtifactsPath() string {
	return fmt.Sprintf("qos/tpcc_background_olap/nodes=%d/cpu=%d/w=%d/c=%d",
		s.Nodes, s.CPUs, s.Warehouses, s.Concurrency)
}

func registerTPCCQoSBackgroundOLAPSpec(r registry.Registry, s tpccQoSBackgroundOLAPSpec) {
	name := s.getArtifactsPath()
	r.Add(registry.TestSpec{
		Name:    name,
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(s.Nodes+1, spec.CPU(s.CPUs)),
		Run:     s.run,
		Timeout: 1 * time.Hour,
	})
}

func registerTPCCQoSBackgroundOLAP(r registry.Registry) {
	specs := []tpccQoSBackgroundOLAPSpec{
		{
			CPUs:            4,
			Concurrency:     200,
			OLAPConcurrency: 64,
			Nodes:           3,
			Warehouses:      30,
			Duration:        15 * time.Minute,
			Ramp:            1 * time.Minute,
			numRuns:         1,
		},
	}
	for _, s := range specs {
		registerTPCCQoSBackgroundOLAPSpec(r, s)
	}
}
