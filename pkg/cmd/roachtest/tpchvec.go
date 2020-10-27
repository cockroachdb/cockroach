// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bufio"
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/workload/tpch"
	"github.com/cockroachdb/errors"
)

type crdbVersion int

const (
	tpchVecVersion19_2 crdbVersion = iota
	tpchVecVersion20_1
	tpchVecVersion20_2
)

func toCRDBVersion(v string) (crdbVersion, error) {
	if strings.HasPrefix(v, "v19.2") {
		return tpchVecVersion19_2, nil
	} else if strings.HasPrefix(v, "v20.1") {
		return tpchVecVersion20_1, nil
	} else if strings.HasPrefix(v, "v20.2") {
		return tpchVecVersion20_2, nil
	} else {
		return 0, errors.Errorf("unrecognized version: %s", v)
	}
}

func vectorizeOptionToSetting(vectorize bool, version crdbVersion) string {
	if !vectorize {
		return "off"
	}
	switch version {
	case tpchVecVersion19_2:
		return "experimental_on"
	default:
		return "on"
	}
}

// queriesToSkipByVersion is a map keyed by version that contains query numbers
// to be skipped for the given version (as well as the reasons for why they are
// skipped).
var queriesToSkipByVersion = map[crdbVersion]map[int]string{
	tpchVecVersion19_2: {
		5:  "can cause OOM",
		7:  "can cause OOM",
		8:  "can cause OOM",
		9:  "can cause OOM",
		19: "can cause OOM",
	},
}

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
	// queriesToRun specifies the number of queries to run (in [1,
	// tpch.NumQueries] range).
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
func performClusterSetup(t *test, conn *gosql.DB, clusterSetup []string) {
	for _, query := range clusterSetup {
		if _, err := conn.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
}

type tpchVecTestCase interface {
	// getRunConfig returns the configuration of tpchvec test run.
	getRunConfig(version crdbVersion, queriesToSkip map[int]string) tpchVecTestRunConfig
	// preTestRunHook is called before any tpch query is run. Can be used to
	// perform any setup that cannot be expressed as a modification to
	// cluster-wide settings (those should go into tpchVecTestRunConfig).
	preTestRunHook(ctx context.Context, t *test, c *cluster, conn *gosql.DB, version crdbVersion, clusterSetup []string)
	// postQueryRunHook is called after each tpch query is run with the output and
	// the index of the setup it was run in.
	postQueryRunHook(t *test, output []byte, setupIdx int)
	// postTestRunHook is called after all tpch queries are run. Can be used to
	// perform teardown or general validation.
	postTestRunHook(ctx context.Context, t *test, c *cluster, conn *gosql.DB, version crdbVersion)
}

// tpchVecTestCaseBase is a default tpchVecTestCase implementation that can be
// embedded and extended.
type tpchVecTestCaseBase struct{}

func (b tpchVecTestCaseBase) getRunConfig(
	version crdbVersion, queriesToSkip map[int]string,
) tpchVecTestRunConfig {
	runConfig := tpchVecTestRunConfig{
		numRunsPerQuery: 1,
		clusterSetups: [][]string{{
			"RESET CLUSTER SETTING sql.distsql.temp_storage.workmem",
			fmt.Sprintf("SET CLUSTER SETTING sql.defaults.vectorize=%s",
				vectorizeOptionToSetting(true, version)),
		}},
		setupNames: []string{"default"},
	}
	if version != tpchVecVersion19_2 {
		runConfig.clusterSetups[0] = append(runConfig.clusterSetups[0],
			"RESET CLUSTER SETTING sql.testing.vectorize.batch_size",
		)
	}
	for queryNum := 1; queryNum <= tpch.NumQueries; queryNum++ {
		if _, shouldSkip := queriesToSkip[queryNum]; !shouldSkip {
			runConfig.queriesToRun = append(runConfig.queriesToRun, queryNum)
		}
	}
	return runConfig
}

func (b tpchVecTestCaseBase) preTestRunHook(
	_ context.Context,
	t *test,
	_ *cluster,
	conn *gosql.DB,
	version crdbVersion,
	clusterSetup []string,
) {
	performClusterSetup(t, conn, clusterSetup)
}

func (b tpchVecTestCaseBase) postQueryRunHook(*test, []byte, int) {}

func (b tpchVecTestCaseBase) postTestRunHook(
	context.Context, *test, *cluster, *gosql.DB, crdbVersion,
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

func (h *tpchVecPerfHelper) parseQueryOutput(t *test, output []byte, setupIdx int) {
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

func (p tpchVecPerfTest) getRunConfig(version crdbVersion, _ map[int]string) tpchVecTestRunConfig {
	var queriesToSkip map[int]string
	if p.disableStatsCreation {
		queriesToSkip = map[int]string{
			9: "takes too long without stats",
		}
	} else {
		queriesToSkip = queriesToSkipByVersion[version]
	}
	runConfig := p.tpchVecTestCaseBase.getRunConfig(version, queriesToSkip)
	runConfig.numRunsPerQuery = 3
	// Make a copy of the default configuration setup and add different
	// vectorize setting updates. Note that it's ok that the default setup
	// sets vectorize cluster setting to 'on' - we will override it with
	// queries below.
	defaultSetup := runConfig.clusterSetups[0]
	runConfig.clusterSetups = append(runConfig.clusterSetups, make([]string, len(defaultSetup)))
	copy(runConfig.clusterSetups[1], defaultSetup)
	runConfig.clusterSetups[tpchPerfTestVecOffConfigIdx] = append(runConfig.clusterSetups[tpchPerfTestVecOffConfigIdx],
		fmt.Sprintf("SET CLUSTER SETTING sql.defaults.vectorize=%s", vectorizeOptionToSetting(false, version)))
	runConfig.clusterSetups[tpchPerfTestVecOnConfigIdx] = append(runConfig.clusterSetups[tpchPerfTestVecOnConfigIdx],
		fmt.Sprintf("SET CLUSTER SETTING sql.defaults.vectorize=%s", vectorizeOptionToSetting(true, version)))
	runConfig.setupNames = make([]string, 2)
	runConfig.setupNames[tpchPerfTestVecOffConfigIdx] = "off"
	runConfig.setupNames[tpchPerfTestVecOnConfigIdx] = "on"
	return runConfig
}

func (p tpchVecPerfTest) preTestRunHook(
	ctx context.Context,
	t *test,
	c *cluster,
	conn *gosql.DB,
	version crdbVersion,
	clusterSetup []string,
) {
	p.tpchVecTestCaseBase.preTestRunHook(ctx, t, c, conn, version, clusterSetup)
	if !p.disableStatsCreation {
		createStatsFromTables(t, conn, tpchTables)
	}
}

func (p *tpchVecPerfTest) postQueryRunHook(t *test, output []byte, setupIdx int) {
	p.parseQueryOutput(t, output, setupIdx)
}

func (p *tpchVecPerfTest) postTestRunHook(
	ctx context.Context, t *test, c *cluster, conn *gosql.DB, version crdbVersion,
) {
	runConfig := p.getRunConfig(version, queriesToSkipByVersion[version])
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
			t.l.Printf(
				fmt.Sprintf("[q%d] vec OFF was faster by %.2f%%: "+
					"%.2fs ON vs %.2fs OFF --- WARNING\n"+
					"vec ON times: %v\t vec OFF times: %v",
					queryNum, 100*(vecOnTime-vecOffTime)/vecOffTime,
					vecOnTime, vecOffTime, vecOnTimes, vecOffTimes))
		} else {
			t.l.Printf(
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
					if err = c.RunL(ctx, t.l, c.Node(1), curlCmd); err != nil {
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

func (b tpchVecBenchTest) getRunConfig(version crdbVersion, _ map[int]string) tpchVecTestRunConfig {
	runConfig := b.tpchVecTestCaseBase.getRunConfig(version, queriesToSkipByVersion[version])
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

func (b *tpchVecBenchTest) postQueryRunHook(t *test, output []byte, setupIdx int) {
	b.tpchVecPerfHelper.parseQueryOutput(t, output, setupIdx)
}

func (b *tpchVecBenchTest) postTestRunHook(
	ctx context.Context, t *test, c *cluster, conn *gosql.DB, version crdbVersion,
) {
	runConfig := b.getRunConfig(version, queriesToSkipByVersion[version])
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
		t.l.Printf(fmt.Sprintf("[q%d] best setup is %s", queryNum, runConfig.setupNames[bestSetupIdx]))
		for setupIdx, setupName := range runConfig.setupNames {
			setupTime := findAvgTime(b.timeByQueryNum[setupIdx][queryNum])
			scores[setupIdx] += setupTime / bestTime
			t.l.Printf(fmt.Sprintf("[q%d] setup %s took %.2fs", queryNum, setupName, setupTime))
		}
	}
	t.Status("----- scores of the setups -----")
	bestScore := math.MaxFloat64
	var bestSetupIdx int
	for setupIdx, setupName := range runConfig.setupNames {
		score := scores[setupIdx]
		t.l.Printf(fmt.Sprintf("score of %s is %.2f", setupName, score))
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
	ctx context.Context,
	t *test,
	c *cluster,
	conn *gosql.DB,
	version crdbVersion,
	clusterSetup []string,
) {
	d.tpchVecTestCaseBase.preTestRunHook(ctx, t, c, conn, version, clusterSetup)
	createStatsFromTables(t, conn, tpchTables)
	// In order to stress the disk spilling of the vectorized
	// engine, we will set workmem limit to a random value in range
	// [16KiB, 256KiB).
	rng, _ := randutil.NewPseudoRand()
	workmemInKiB := 16 + rng.Intn(240)
	workmem := fmt.Sprintf("%dKiB", workmemInKiB)
	t.Status(fmt.Sprintf("setting workmem='%s'", workmem))
	if _, err := conn.Exec(fmt.Sprintf("SET CLUSTER SETTING sql.distsql.temp_storage.workmem='%s'", workmem)); err != nil {
		t.Fatal(err)
	}
}

// setSmallBatchSize sets a cluster setting to override the batch size to be in
// [1, 5) range.
func setSmallBatchSize(t *test, conn *gosql.DB, rng *rand.Rand) {
	batchSize := 1 + rng.Intn(4)
	t.Status(fmt.Sprintf("setting sql.testing.vectorize.batch_size to %d", batchSize))
	if _, err := conn.Exec(fmt.Sprintf("SET CLUSTER SETTING sql.testing.vectorize.batch_size=%d", batchSize)); err != nil {
		t.Fatal(err)
	}
}

type tpchVecSmallBatchSizeTest struct {
	tpchVecTestCaseBase
}

func (b tpchVecSmallBatchSizeTest) preTestRunHook(
	ctx context.Context,
	t *test,
	c *cluster,
	conn *gosql.DB,
	version crdbVersion,
	clusterSetup []string,
) {
	b.tpchVecTestCaseBase.preTestRunHook(ctx, t, c, conn, version, clusterSetup)
	createStatsFromTables(t, conn, tpchTables)
	rng, _ := randutil.NewPseudoRand()
	setSmallBatchSize(t, conn, rng)
}

func baseTestRun(
	ctx context.Context, t *test, c *cluster, conn *gosql.DB, version crdbVersion, tc tpchVecTestCase,
) {
	firstNode := c.Node(1)
	runConfig := tc.getRunConfig(version, queriesToSkipByVersion[version])
	for setupIdx, setup := range runConfig.clusterSetups {
		t.Status(fmt.Sprintf("running setup=%s", runConfig.setupNames[setupIdx]))
		tc.preTestRunHook(ctx, t, c, conn, version, setup)
		for _, queryNum := range runConfig.queriesToRun {
			// Note that we use --default-vectorize flag which tells tpch
			// workload to use the current cluster setting
			// sql.defaults.vectorize which must have been set correctly in
			// preTestRunHook.
			cmd := fmt.Sprintf("./workload run tpch --concurrency=1 --db=tpch "+
				"--default-vectorize --max-ops=%d --queries=%d {pgurl:1}",
				runConfig.numRunsPerQuery, queryNum)
			workloadOutput, err := c.RunWithBuffer(ctx, t.l, firstNode, cmd)
			t.l.Printf("\n" + string(workloadOutput))
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
	ctx context.Context,
	t *test,
	c *cluster,
	conn *gosql.DB,
	version crdbVersion,
	clusterSetup []string,
) {
	s.tpchVecTestCaseBase.preTestRunHook(ctx, t, c, conn, version, clusterSetup)
	createStatsFromTables(t, conn, tpchTables)
	const smithcmpSHA = "a3f41f5ba9273249c5ecfa6348ea8ee3ac4b77e3"
	node := c.Node(1)
	if local && runtime.GOOS != "linux" {
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
	// To increase test coverage, we will be randomizing the batch size in 50%
	// of the runs.
	rng, _ := randutil.NewPseudoRand()
	if rng.Float64() < 0.5 {
		setSmallBatchSize(t, conn, rng)
	}
}

func smithcmpTestRun(
	ctx context.Context, t *test, c *cluster, conn *gosql.DB, version crdbVersion, tc tpchVecTestCase,
) {
	runConfig := tc.getRunConfig(version, queriesToSkipByVersion[version])
	tc.preTestRunHook(ctx, t, c, conn, version, runConfig.clusterSetups[0])
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
	t *test,
	c *cluster,
	testCase tpchVecTestCase,
	testRun func(ctx context.Context, t *test, c *cluster, conn *gosql.DB, version crdbVersion, tc tpchVecTestCase),
) {
	firstNode := c.Node(1)
	c.Put(ctx, cockroach, "./cockroach", c.All())
	c.Put(ctx, workload, "./workload", firstNode)
	c.Start(ctx, t)

	conn := c.Conn(ctx, 1)
	disableAutoStats(t, conn)
	disableVectorizeRowCountThresholdHeuristic(t, conn)
	t.Status("restoring TPCH dataset for Scale Factor 1")
	if err := loadTPCHDataset(ctx, t, c, 1 /* sf */, newMonitor(ctx, c), c.All()); err != nil {
		t.Fatal(err)
	}

	if _, err := conn.Exec("USE tpch;"); err != nil {
		t.Fatal(err)
	}
	scatterTables(t, conn, tpchTables)
	t.Status("waiting for full replication")
	waitForFullReplication(t, conn)
	versionString, err := fetchCockroachVersion(ctx, c, c.Node(1)[0])
	if err != nil {
		t.Fatal(err)
	}
	version, err := toCRDBVersion(versionString)
	if err != nil {
		t.Fatal(err)
	}

	testRun(ctx, t, c, conn, version, testCase)
	testCase.postTestRunHook(ctx, t, c, conn, version)
}

const tpchVecNodeCount = 3

func registerTPCHVec(r *testRegistry) {
	r.Add(testSpec{
		Name:       "tpchvec/perf",
		Owner:      OwnerSQLExec,
		Cluster:    makeClusterSpec(tpchVecNodeCount),
		MinVersion: "v19.2.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCHVec(ctx, t, c, newTpchVecPerfTest(false /* disableStatsCreation */), baseTestRun)
		},
	})

	r.Add(testSpec{
		Name:    "tpchvec/disk",
		Owner:   OwnerSQLExec,
		Cluster: makeClusterSpec(tpchVecNodeCount),
		// 19.2 version doesn't have disk spilling nor memory monitoring, so
		// there is no point in running this config on that version.
		MinVersion: "v20.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCHVec(ctx, t, c, tpchVecDiskTest{}, baseTestRun)
		},
	})

	r.Add(testSpec{
		Name:    "tpchvec/smallbatchsize",
		Owner:   OwnerSQLExec,
		Cluster: makeClusterSpec(tpchVecNodeCount),
		// 19.2 version doesn't have the testing cluster setting to change the batch
		// size, so only run on versions >= 20.1.0.
		MinVersion: "v20.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCHVec(ctx, t, c, tpchVecSmallBatchSizeTest{}, baseTestRun)
		},
	})

	r.Add(testSpec{
		Name:       "tpchvec/smithcmp",
		Owner:      OwnerSQLExec,
		Cluster:    makeClusterSpec(tpchVecNodeCount),
		MinVersion: "v20.1.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCHVec(ctx, t, c, tpchVecSmithcmpTest{}, smithcmpTestRun)
		},
	})
}
