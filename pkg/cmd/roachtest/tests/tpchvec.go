// Copyright 2019 The Cockroach Authors.
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
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/workload/tpch"
)

const tpchVecPerfSlownessThreshold = 1.5

var tpchTables = []string{
	"nation", "region", "part", "supplier",
	"partsupp", "customer", "orders", "lineitem",
}

// tpchVecTestRunConfig specifies the configuration of a tpchvec test run.
type tpchVecTestRunConfig struct {
	// numRunsPerQuery determines how many time a single query runs, set to 1
	// by default.
	numRunsPerQuery int
	// queriesToRun specifies which queries to run (in [1, tpch.NumQueries]
	// range).
	queriesToRun []int
	// clusterSetups specifies all cluster setup queries that need to be
	// executed before running any of the TPCH queries. First dimension
	// determines the number of different clusterSetups a tpchvec test is run
	// with, and every clusterSetups[i] specifies all queries for setup with
	// index i.
	// Note: these are expected to modify cluster-wide settings.
	clusterSetups [][]string
	// setupNames contains 1-to-1 mapping with clusterSetups to provide
	// user-friendly names for the setups.
	setupNames []string
}

// performClusterSetup executes all queries in clusterSetup on conn.
func performClusterSetup(t test.Test, conn *gosql.DB, clusterSetup []string) {
	for _, query := range clusterSetup {
		if _, err := conn.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
}

type tpchVecTestCase interface {
	// getRunConfig returns the configuration of tpchvec test run.
	getRunConfig() tpchVecTestRunConfig
	// preTestRunHook is called before any tpch query is run. Can be used to
	// perform any setup that cannot be expressed as a modification to
	// cluster-wide settings (those should go into tpchVecTestRunConfig).
	preTestRunHook(ctx context.Context, t test.Test, c cluster.Cluster, conn *gosql.DB, clusterSetup []string)
	// postQueryRunHook is called after each tpch query is run with the output and
	// the index of the setup it was run in.
	postQueryRunHook(t test.Test, output []byte, setupIdx int)
	// postTestRunHook is called after all tpch queries are run. Can be used to
	// perform teardown or general validation.
	postTestRunHook(ctx context.Context, t test.Test, c cluster.Cluster, conn *gosql.DB)
}

// tpchVecTestCaseBase is a default tpchVecTestCase implementation that can be
// embedded and extended.
type tpchVecTestCaseBase struct{}

func (b tpchVecTestCaseBase) getRunConfig() tpchVecTestRunConfig {
	runConfig := tpchVecTestRunConfig{
		numRunsPerQuery: 1,
		clusterSetups: [][]string{{
			"RESET CLUSTER SETTING sql.distsql.temp_storage.workmem",
			"SET CLUSTER SETTING sql.defaults.vectorize=on",
		}},
		setupNames: []string{"default"},
	}
	for queryNum := 1; queryNum <= tpch.NumQueries; queryNum++ {
		runConfig.queriesToRun = append(runConfig.queriesToRun, queryNum)
	}
	return runConfig
}

func (b tpchVecTestCaseBase) preTestRunHook(
	t test.Test, conn *gosql.DB, clusterSetup []string, createStats bool,
) {
	performClusterSetup(t, conn, clusterSetup)
	if createStats {
		createStatsFromTables(t, conn, tpchTables)
	}
}

func (b tpchVecTestCaseBase) postQueryRunHook(test.Test, []byte, int) {}

func (b tpchVecTestCaseBase) postTestRunHook(
	context.Context, test.Test, cluster.Cluster, *gosql.DB,
) {
}

type tpchVecPerfHelper struct {
	timeByQueryNum []map[int][]float64
}

func newTpchVecPerfHelper(numSetups int) *tpchVecPerfHelper {
	timeByQueryNum := make([]map[int][]float64, numSetups)
	for i := range timeByQueryNum {
		timeByQueryNum[i] = make(map[int][]float64)
	}
	return &tpchVecPerfHelper{
		timeByQueryNum: timeByQueryNum,
	}
}

func (h *tpchVecPerfHelper) parseQueryOutput(t test.Test, output []byte, setupIdx int) {
	runtimeRegex := regexp.MustCompile(`.*\[q([\d]+)\] returned \d+ rows after ([\d]+\.[\d]+) seconds.*`)
	scanner := bufio.NewScanner(bytes.NewReader(output))
	for scanner.Scan() {
		line := scanner.Bytes()
		match := runtimeRegex.FindSubmatch(line)
		if match != nil {
			queryNum, err := strconv.Atoi(string(match[1]))
			if err != nil {
				t.Fatalf("failed parsing %q as int with %s", match[1], err)
			}
			queryTime, err := strconv.ParseFloat(string(match[2]), 64)
			if err != nil {
				t.Fatalf("failed parsing %q as float with %s", match[2], err)
			}
			h.timeByQueryNum[setupIdx][queryNum] = append(h.timeByQueryNum[setupIdx][queryNum], queryTime)
		}
	}
}

const (
	tpchPerfTestVecOnConfigIdx  = 1
	tpchPerfTestVecOffConfigIdx = 0
)

type tpchVecPerfTest struct {
	tpchVecTestCaseBase
	*tpchVecPerfHelper

	disableStatsCreation bool
}

var _ tpchVecTestCase = &tpchVecPerfTest{}

func newTpchVecPerfTest(disableStatsCreation bool) *tpchVecPerfTest {
	return &tpchVecPerfTest{
		tpchVecPerfHelper:    newTpchVecPerfHelper(2 /* numSetups */),
		disableStatsCreation: disableStatsCreation,
	}
}

func (p tpchVecPerfTest) getRunConfig() tpchVecTestRunConfig {
	runConfig := p.tpchVecTestCaseBase.getRunConfig()
	if p.disableStatsCreation {
		// Query 9 takes too long without stats, so we'll skip it.
		runConfig.queriesToRun = append(runConfig.queriesToRun[:8], runConfig.queriesToRun[9:]...)
	}
	runConfig.numRunsPerQuery = 3
	// Make a copy of the default configuration setup and add different
	// vectorize setting updates. Note that it's ok that the default setup
	// sets vectorize cluster setting to 'on' - we will override it with
	// queries below.
	defaultSetup := runConfig.clusterSetups[0]
	runConfig.clusterSetups = append(runConfig.clusterSetups, make([]string, len(defaultSetup)))
	copy(runConfig.clusterSetups[1], defaultSetup)
	runConfig.clusterSetups[tpchPerfTestVecOffConfigIdx] = append(runConfig.clusterSetups[tpchPerfTestVecOffConfigIdx],
		"SET CLUSTER SETTING sql.defaults.vectorize=off")
	runConfig.clusterSetups[tpchPerfTestVecOnConfigIdx] = append(runConfig.clusterSetups[tpchPerfTestVecOnConfigIdx],
		"SET CLUSTER SETTING sql.defaults.vectorize=on")
	runConfig.setupNames = make([]string, 2)
	runConfig.setupNames[tpchPerfTestVecOffConfigIdx] = "off"
	runConfig.setupNames[tpchPerfTestVecOnConfigIdx] = "on"
	return runConfig
}

func (p tpchVecPerfTest) preTestRunHook(
	ctx context.Context, t test.Test, c cluster.Cluster, conn *gosql.DB, clusterSetup []string,
) {
	p.tpchVecTestCaseBase.preTestRunHook(t, conn, clusterSetup, !p.disableStatsCreation /* createStats */)
}

func (p *tpchVecPerfTest) postQueryRunHook(t test.Test, output []byte, setupIdx int) {
	p.parseQueryOutput(t, output, setupIdx)
}

func (p *tpchVecPerfTest) postTestRunHook(
	ctx context.Context, t test.Test, c cluster.Cluster, conn *gosql.DB,
) {
	runConfig := p.getRunConfig()
	t.Status("comparing the runtimes (only median values for each query are compared)")
	for _, queryNum := range runConfig.queriesToRun {
		findMedian := func(times []float64) float64 {
			sort.Float64s(times)
			return times[len(times)/2]
		}
		vecOnTimes := p.timeByQueryNum[tpchPerfTestVecOnConfigIdx][queryNum]
		vecOffTimes := p.timeByQueryNum[tpchPerfTestVecOffConfigIdx][queryNum]
		if len(vecOnTimes) != runConfig.numRunsPerQuery {
			t.Fatal(fmt.Sprintf("[q%d] unexpectedly wrong number of run times "+
				"recorded with vec ON config: %v", queryNum, vecOnTimes))
		}
		if len(vecOffTimes) != runConfig.numRunsPerQuery {
			t.Fatal(fmt.Sprintf("[q%d] unexpectedly wrong number of run times "+
				"recorded with vec OFF config: %v", queryNum, vecOffTimes))
		}
		vecOnTime := findMedian(vecOnTimes)
		vecOffTime := findMedian(vecOffTimes)
		if vecOffTime < vecOnTime {
			t.L().Printf(
				fmt.Sprintf("[q%d] vec OFF was faster by %.2f%%: "+
					"%.2fs ON vs %.2fs OFF --- WARNING\n"+
					"vec ON times: %v\t vec OFF times: %v",
					queryNum, 100*(vecOnTime-vecOffTime)/vecOffTime,
					vecOnTime, vecOffTime, vecOnTimes, vecOffTimes))
		} else {
			t.L().Printf(
				fmt.Sprintf("[q%d] vec ON was faster by %.2f%%: "+
					"%.2fs ON vs %.2fs OFF\n"+
					"vec ON times: %v\t vec OFF times: %v",
					queryNum, 100*(vecOffTime-vecOnTime)/vecOnTime,
					vecOnTime, vecOffTime, vecOnTimes, vecOffTimes))
		}
		if vecOnTime >= tpchVecPerfSlownessThreshold*vecOffTime {
			// For some reason, the vectorized engine executed the query a lot
			// slower than the row-by-row engine which is unexpected. In order
			// to understand where the slowness comes from, we will run EXPLAIN
			// ANALYZE (DEBUG) of the query with all `vectorize` options
			// tpchPerfTestNumRunsPerQuery times (hoping at least one will
			// "catch" the slowness).
			for setupIdx, setup := range runConfig.clusterSetups {
				performClusterSetup(t, conn, setup)
				// performClusterSetup has changed the cluster settings;
				// however, the session variables might contain the old values,
				// so we will open up new connections for each of the setups in
				// order to get the correct cluster setup on each.
				tempConn := c.Conn(ctx, 1)
				defer tempConn.Close()
				if _, err := tempConn.Exec("USE tpch;"); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < runConfig.numRunsPerQuery; i++ {
					t.Status(fmt.Sprintf("\nRunning EXPLAIN ANALYZE (DEBUG) for setup=%s\n", runConfig.setupNames[setupIdx]))
					rows, err := tempConn.Query(fmt.Sprintf(
						"EXPLAIN ANALYZE (DEBUG) %s;", tpch.QueriesByNumber[queryNum],
					))
					if err != nil {
						t.Fatal(err)
					}
					// The output of the command looks like:
					//   Statement diagnostics bundle generated. Download from the Admin UI (Advanced
					//   Debug -> Statement Diagnostics History), via the direct link below, or using
					//   the command line.
					//   Admin UI: http://Yahors-MacBook-Pro.local:8081
					//   Direct link: http://Yahors-MacBook-Pro.local:8081/_admin/v1/stmtbundle/574364979110641665
					//   Command line: cockroach statement-diag list / download
					// We are interested in the line that contains the url that
					// we will curl below.
					directLinkPrefix := "Direct link: "
					var line, url, debugOutput string
					for rows.Next() {
						if err = rows.Scan(&line); err != nil {
							t.Fatal(err)
						}
						debugOutput += line + "\n"
						if strings.HasPrefix(line, directLinkPrefix) {
							url = line[len(directLinkPrefix):]
							break
						}
					}
					if err = rows.Close(); err != nil {
						t.Fatal(err)
					}
					if url == "" {
						t.Fatal(fmt.Sprintf("unexpectedly didn't find a line "+
							"with %q prefix in EXPLAIN ANALYZE (DEBUG) output\n%s",
							directLinkPrefix, debugOutput))
					}
					// We will curl into the logs folder so that test runner
					// retrieves the bundle together with the log files.
					curlCmd := fmt.Sprintf(
						"curl %s > logs/bundle_%s_%d.zip", url, runConfig.setupNames[setupIdx], i,
					)
					if err = c.RunL(ctx, t.L(), c.Node(1), curlCmd); err != nil {
						t.Fatal(err)
					}
				}
			}
			t.Fatal(fmt.Sprintf(
				"[q%d] vec ON is slower by %.2f%% than vec OFF\n"+
					"vec ON times: %v\nvec OFF times: %v",
				queryNum, 100*(vecOnTime-vecOffTime)/vecOffTime, vecOnTimes, vecOffTimes))
		}
	}
}

type tpchVecBenchTest struct {
	tpchVecTestCaseBase
	*tpchVecPerfHelper

	numRunsPerQuery int
	queriesToRun    []int
	clusterSetups   [][]string
	setupNames      []string
}

var _ tpchVecTestCase = &tpchVecBenchTest{}

// queriesToRun can be omitted in which case all queries that are not skipped
// for the given version will be run.
func newTpchVecBenchTest(
	numRunsPerQuery int, queriesToRun []int, clusterSetups [][]string, setupNames []string,
) *tpchVecBenchTest {
	return &tpchVecBenchTest{
		tpchVecPerfHelper: newTpchVecPerfHelper(len(setupNames)),
		numRunsPerQuery:   numRunsPerQuery,
		queriesToRun:      queriesToRun,
		clusterSetups:     clusterSetups,
		setupNames:        setupNames,
	}
}

func (b tpchVecBenchTest) getRunConfig() tpchVecTestRunConfig {
	runConfig := b.tpchVecTestCaseBase.getRunConfig()
	runConfig.numRunsPerQuery = b.numRunsPerQuery
	if b.queriesToRun != nil {
		runConfig.queriesToRun = b.queriesToRun
	}
	defaultSetup := runConfig.clusterSetups[0]
	// We slice up defaultSetup to make sure that new slices are allocated in
	// appends below.
	defaultSetup = defaultSetup[:len(defaultSetup):len(defaultSetup)]
	runConfig.clusterSetups = make([][]string, len(b.clusterSetups))
	runConfig.setupNames = b.setupNames
	for setupIdx, configSetup := range b.clusterSetups {
		runConfig.clusterSetups[setupIdx] = append(defaultSetup, configSetup...)
	}
	return runConfig
}

func (b tpchVecBenchTest) preTestRunHook(
	_ context.Context, t test.Test, _ cluster.Cluster, conn *gosql.DB, clusterSetup []string,
) {
	b.tpchVecTestCaseBase.preTestRunHook(t, conn, clusterSetup, true /* createStats */)
}

func (b *tpchVecBenchTest) postQueryRunHook(t test.Test, output []byte, setupIdx int) {
	b.tpchVecPerfHelper.parseQueryOutput(t, output, setupIdx)
}

func (b *tpchVecBenchTest) postTestRunHook(
	ctx context.Context, t test.Test, c cluster.Cluster, conn *gosql.DB,
) {
	runConfig := b.getRunConfig()
	t.Status("comparing the runtimes (average of values (excluding best and worst) for each query are compared)")
	// A score for a single query is calculated as
	//   <query time on config> / <best query time among all configs>,
	// and then all query scores are summed. So the lower the total score, the
	// better the config is.
	scores := make([]float64, len(runConfig.setupNames))
	for _, queryNum := range runConfig.queriesToRun {
		// findAvgTime finds the average of times excluding best and worst as
		// possible outliers. It expects that len(times) >= 3.
		findAvgTime := func(times []float64) float64 {
			if len(times) < 3 {
				t.Fatal(fmt.Sprintf("unexpectedly query %d ran %d times on one of the setups", queryNum, len(times)))
			}
			sort.Float64s(times)
			sum, count := 0.0, 0
			for _, time := range times[1 : len(times)-1] {
				sum += time
				count++
			}
			return sum / float64(count)
		}
		bestTime := math.MaxFloat64
		var bestSetupIdx int
		for setupIdx := range runConfig.setupNames {
			setupTime := findAvgTime(b.timeByQueryNum[setupIdx][queryNum])
			if setupTime < bestTime {
				bestTime = setupTime
				bestSetupIdx = setupIdx
			}
		}
		t.L().Printf(fmt.Sprintf("[q%d] best setup is %s", queryNum, runConfig.setupNames[bestSetupIdx]))
		for setupIdx, setupName := range runConfig.setupNames {
			setupTime := findAvgTime(b.timeByQueryNum[setupIdx][queryNum])
			scores[setupIdx] += setupTime / bestTime
			t.L().Printf(fmt.Sprintf("[q%d] setup %s took %.2fs", queryNum, setupName, setupTime))
		}
	}
	t.Status("----- scores of the setups -----")
	bestScore := math.MaxFloat64
	var bestSetupIdx int
	for setupIdx, setupName := range runConfig.setupNames {
		score := scores[setupIdx]
		t.L().Printf(fmt.Sprintf("score of %s is %.2f", setupName, score))
		if bestScore > score {
			bestScore = score
			bestSetupIdx = setupIdx
		}
	}
	t.Status(fmt.Sprintf("----- best setup is %s -----", runConfig.setupNames[bestSetupIdx]))
}

type tpchVecDiskTest struct {
	tpchVecTestCaseBase
}

func (d tpchVecDiskTest) preTestRunHook(
	ctx context.Context, t test.Test, c cluster.Cluster, conn *gosql.DB, clusterSetup []string,
) {
	d.tpchVecTestCaseBase.preTestRunHook(t, conn, clusterSetup, true /* createStats */)
	// In order to stress the disk spilling of the vectorized engine, we will
	// set workmem limit to a random value in range [650KiB, 2000KiB).
	//
	// The lower bound of that range was determined by running all queries on a
	// single node cluster. If we lower that further, Q1 will take extremely
	// long time (which is the expected) because the hash aggregator spills to
	// disk and is using the fallback strategy of external sort + ordered
	// aggregator, with the sort processing all of the incoming data (on the
	// order of 800MiB) in partitions of roughly workmem/4 in size. Such
	// behavior is determined by the fact that we allocate some RAM for caches
	// of disk queues (limiting us to use at most 2 input partitions).
	rng, _ := randutil.NewPseudoRand()
	workmemInKiB := 650 + rng.Intn(1350)
	workmem := fmt.Sprintf("%dKiB", workmemInKiB)
	t.Status(fmt.Sprintf("setting workmem='%s'", workmem))
	if _, err := conn.Exec(fmt.Sprintf("SET CLUSTER SETTING sql.distsql.temp_storage.workmem='%s'", workmem)); err != nil {
		t.Fatal(err)
	}
}

func baseTestRun(
	ctx context.Context, t test.Test, c cluster.Cluster, conn *gosql.DB, tc tpchVecTestCase,
) {
	firstNode := c.Node(1)
	runConfig := tc.getRunConfig()
	for setupIdx, setup := range runConfig.clusterSetups {
		t.Status(fmt.Sprintf("running setup=%s", runConfig.setupNames[setupIdx]))
		tc.preTestRunHook(ctx, t, c, conn, setup)
		for _, queryNum := range runConfig.queriesToRun {
			// Note that we use --default-vectorize flag which tells tpch
			// workload to use the current cluster setting
			// sql.defaults.vectorize which must have been set correctly in
			// preTestRunHook.
			cmd := fmt.Sprintf("./workload run tpch --concurrency=1 --db=tpch "+
				"--default-vectorize --max-ops=%d --queries=%d {pgurl:1} --enable-checks=true",
				runConfig.numRunsPerQuery, queryNum)
			workloadOutput, err := c.RunWithBuffer(ctx, t.L(), firstNode, cmd)
			t.L().Printf("\n" + string(workloadOutput))
			if err != nil {
				// Note: if you see an error like "exit status 1", it is likely caused
				// by the erroneous output of the query.
				t.Fatal(err)
			}
			tc.postQueryRunHook(t, workloadOutput, setupIdx)
		}
	}
}

type tpchVecSmithcmpTest struct {
	tpchVecTestCaseBase
}

const tpchVecSmithcmp = "smithcmp"

func (s tpchVecSmithcmpTest) preTestRunHook(
	ctx context.Context, t test.Test, c cluster.Cluster, conn *gosql.DB, clusterSetup []string,
) {
	s.tpchVecTestCaseBase.preTestRunHook(t, conn, clusterSetup, true /* createStats */)
	const smithcmpSHA = "a3f41f5ba9273249c5ecfa6348ea8ee3ac4b77e3"
	node := c.Node(1)
	if c.IsLocal() && runtime.GOOS != "linux" {
		t.Fatalf("must run on linux os, found %s", runtime.GOOS)
	}
	// This binary has been manually compiled using
	// './build/builder.sh go build ./pkg/cmd/smithcmp' and uploaded to S3
	// bucket at cockroach/smithcmp. The binary shouldn't change much, so it is
	// acceptable.
	smithcmp, err := binfetcher.Download(ctx, binfetcher.Options{
		Component: tpchVecSmithcmp,
		Binary:    tpchVecSmithcmp,
		Version:   smithcmpSHA,
		GOOS:      "linux",
		GOARCH:    "amd64",
	})
	if err != nil {
		t.Fatal(err)
	}
	c.Put(ctx, smithcmp, "./"+tpchVecSmithcmp, node)
}

func smithcmpTestRun(
	ctx context.Context, t test.Test, c cluster.Cluster, conn *gosql.DB, tc tpchVecTestCase,
) {
	runConfig := tc.getRunConfig()
	tc.preTestRunHook(ctx, t, c, conn, runConfig.clusterSetups[0])
	const (
		configFile = `tpchvec_smithcmp.toml`
		configURL  = `https://raw.githubusercontent.com/cockroachdb/cockroach/master/pkg/cmd/roachtest/` + configFile
	)
	firstNode := c.Node(1)
	if err := c.RunE(ctx, firstNode, fmt.Sprintf("curl %s > %s", configURL, configFile)); err != nil {
		t.Fatal(err)
	}
	cmd := fmt.Sprintf("./%s %s", tpchVecSmithcmp, configFile)
	if err := c.RunE(ctx, firstNode, cmd); err != nil {
		t.Fatal(err)
	}
}

func runTPCHVec(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	testCase tpchVecTestCase,
	testRun func(ctx context.Context, t test.Test, c cluster.Cluster, conn *gosql.DB, tc tpchVecTestCase),
) {
	firstNode := c.Node(1)
	c.Put(ctx, t.Cockroach(), "./cockroach", c.All())
	c.Put(ctx, t.DeprecatedWorkload(), "./workload", firstNode)
	c.Start(ctx)

	conn := c.Conn(ctx, 1)
	disableAutoStats(t, conn)
	t.Status("restoring TPCH dataset for Scale Factor 1")
	if err := loadTPCHDataset(ctx, t, c, 1 /* sf */, c.NewMonitor(ctx), c.All()); err != nil {
		t.Fatal(err)
	}

	if _, err := conn.Exec("USE tpch;"); err != nil {
		t.Fatal(err)
	}
	scatterTables(t, conn, tpchTables)
	t.Status("waiting for full replication")
	WaitFor3XReplication(t, conn)

	testRun(ctx, t, c, conn, testCase)
	testCase.postTestRunHook(ctx, t, c, conn)
}

const tpchVecNodeCount = 3

func registerTPCHVec(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:    "tpchvec/perf",
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(tpchVecNodeCount),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCHVec(ctx, t, c, newTpchVecPerfTest(false /* disableStatsCreation */), baseTestRun)
		},
	})

	r.Add(registry.TestSpec{
		Name:    "tpchvec/disk",
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(tpchVecNodeCount),
		// 19.2 version doesn't have disk spilling nor memory monitoring, so
		// there is no point in running this config on that version.
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCHVec(ctx, t, c, tpchVecDiskTest{}, baseTestRun)
		},
	})

	r.Add(registry.TestSpec{
		Name:    "tpchvec/smithcmp",
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(tpchVecNodeCount),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCHVec(ctx, t, c, tpchVecSmithcmpTest{}, smithcmpTestRun)
		},
	})

	r.Add(registry.TestSpec{
		Name:    "tpchvec/perf_no_stats",
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(tpchVecNodeCount),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCHVec(ctx, t, c, newTpchVecPerfTest(true /* disableStatsCreation */), baseTestRun)
		},
	})

	r.Add(registry.TestSpec{
		Name:    "tpchvec/bench",
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(tpchVecNodeCount),
		Skip: "This config can be used to perform some benchmarking and is not " +
			"meant to be run on a nightly basis",
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			// In order to use this test for benchmarking, include the queries
			// that modify the cluster settings for all configs to benchmark
			// like in the example below. The example benchmarks three values
			// of coldata.BatchSize() variable against each other.
			// NOTE: the setting has been removed since the example was written,
			// but it still serves the purpose of showing how to use the config.
			var clusterSetups [][]string
			var setupNames []string
			for _, batchSize := range []int{512, 1024, 1536} {
				clusterSetups = append(clusterSetups, []string{
					fmt.Sprintf("SET CLUSTER SETTING sql.testing.vectorize.batch_size=%d", batchSize),
				})
				setupNames = append(setupNames, fmt.Sprintf("%d", batchSize))
			}
			benchTest := newTpchVecBenchTest(
				5,   /* numRunsPerQuery */
				nil, /* queriesToRun */
				clusterSetups,
				setupNames,
			)
			runTPCHVec(ctx, t, c, benchTest, baseTestRun)
		},
	})
}
