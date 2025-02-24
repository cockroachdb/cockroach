// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"bufio"
	"bytes"
	"context"
	gosql "database/sql"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/workload/tpch"
	"github.com/stretchr/testify/require"
)

var tpchTables = []string{
	"nation", "region", "part", "supplier",
	"partsupp", "customer", "orders", "lineitem",
}

// tpchVecTestRunConfig specifies the configuration of a tpchvec test run.
type tpchVecTestRunConfig struct {
	// numRunsPerQuery determines how many times a single query runs, set to 1
	// by default.
	numRunsPerQuery int
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
	// sharedProcessMT returns whether this test is running in shared-process
	// multi-tenant mode.
	sharedProcessMT() bool
	// getRunConfig returns the configuration of tpchvec test run.
	getRunConfig() tpchVecTestRunConfig
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

func (b tpchVecTestCaseBase) sharedProcessMT() bool {
	return false
}

func (b tpchVecTestCaseBase) getRunConfig() tpchVecTestRunConfig {
	return tpchVecTestRunConfig{
		numRunsPerQuery: 1,
		clusterSetups: [][]string{{
			"RESET CLUSTER SETTING sql.distsql.temp_storage.workmem",
			"SET CLUSTER SETTING sql.defaults.vectorize=on",
		}},
		setupNames: []string{"default"},
	}
}

func (b tpchVecTestCaseBase) postQueryRunHook(test.Test, []byte, int) {}

func (b tpchVecTestCaseBase) postTestRunHook(
	context.Context, test.Test, cluster.Cluster, *gosql.DB,
) {
}

type tpchVecPerfHelper struct {
	setupNames     []string
	timeByQueryNum []map[int][]float64
}

func newTpchVecPerfHelper(setupNames []string) *tpchVecPerfHelper {
	timeByQueryNum := make([]map[int][]float64, len(setupNames))
	for i := range timeByQueryNum {
		timeByQueryNum[i] = make(map[int][]float64)
	}
	return &tpchVecPerfHelper{
		setupNames:     setupNames,
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

func (h *tpchVecPerfHelper) getQueryTimes(
	t test.Test, numRunsPerQuery, queryNum int,
) (onTime, offTime float64) {
	findMedian := func(times []float64) float64 {
		sort.Float64s(times)
		return times[len(times)/2]
	}
	onTimes := h.timeByQueryNum[tpchPerfTestOnConfigIdx][queryNum]
	onName := h.setupNames[tpchPerfTestOnConfigIdx]
	offTimes := h.timeByQueryNum[tpchPerfTestOffConfigIdx][queryNum]
	offName := h.setupNames[tpchPerfTestOffConfigIdx]
	if len(onTimes) != numRunsPerQuery {
		t.Fatal(fmt.Sprintf("[q%d] unexpectedly wrong number of run times "+
			"recorded with %s config: %v", queryNum, onName, onTimes))
	}
	if len(offTimes) != numRunsPerQuery {
		t.Fatal(fmt.Sprintf("[q%d] unexpectedly wrong number of run times "+
			"recorded with %s config: %v", queryNum, offName, offTimes))
	}
	return findMedian(onTimes), findMedian(offTimes)
}

// compareSetups compares the runtimes of TPCH queries in different setups and
// logs that comparison. The expectation is that the second "ON" setup should be
// faster, and if that is not the case, then a warning message is included in
// the log.
func (h *tpchVecPerfHelper) compareSetups(
	t test.Test,
	numRunsPerQuery int,
	timesCallback func(queryNum int, onTime, offTime float64, onTimes, offTimes []float64),
) {
	t.Status("comparing the runtimes (only median values for each query are compared)")
	for queryNum := 1; queryNum <= tpch.NumQueries; queryNum++ {
		onTimes := h.timeByQueryNum[tpchPerfTestOnConfigIdx][queryNum]
		onName := h.setupNames[tpchPerfTestOnConfigIdx]
		offTimes := h.timeByQueryNum[tpchPerfTestOffConfigIdx][queryNum]
		offName := h.setupNames[tpchPerfTestOffConfigIdx]
		onTime, offTime := h.getQueryTimes(t, numRunsPerQuery, queryNum)
		if offTime < onTime {
			t.L().Printf(
				fmt.Sprintf("[q%d] %s was faster by %.2f%%: "+
					"%.2fs %s vs %.2fs %s --- WARNING\n"+
					"%s times: %v\t %s times: %v",
					queryNum, offName, 100*(onTime-offTime)/offTime, onTime, onName,
					offTime, offName, onName, onTimes, offName, offTimes))
		} else {
			t.L().Printf(
				fmt.Sprintf("[q%d] %s was faster by %.2f%%: "+
					"%.2fs %s vs %.2fs %s\n"+
					"%s times: %v\t %s times: %v",
					queryNum, onName, 100*(offTime-onTime)/onTime, onTime, onName,
					offTime, offName, onName, onTimes, offName, offTimes))
		}
		if timesCallback != nil {
			timesCallback(queryNum, onTime, offTime, onTimes, offTimes)
		}
	}
}

const (
	tpchPerfTestOnConfigIdx  = 1
	tpchPerfTestOffConfigIdx = 0
)

type tpchVecPerfTest struct {
	tpchVecTestCaseBase
	*tpchVecPerfHelper

	settingName       string
	slownessThreshold float64
	sharedProcess     bool
	logOnSlowness     bool
}

var _ tpchVecTestCase = &tpchVecPerfTest{}

func newTpchVecPerfTest(
	settingName string, slownessThreshold float64, sharedProcessMT bool,
) *tpchVecPerfTest {
	return &tpchVecPerfTest{
		tpchVecPerfHelper: newTpchVecPerfHelper([]string{"OFF", "ON"}),
		settingName:       settingName,
		slownessThreshold: slownessThreshold,
		sharedProcess:     sharedProcessMT,
	}
}

func (p tpchVecPerfTest) sharedProcessMT() bool {
	return p.sharedProcess
}

func (p tpchVecPerfTest) getRunConfig() tpchVecTestRunConfig {
	runConfig := p.tpchVecTestCaseBase.getRunConfig()
	runConfig.numRunsPerQuery = 3
	// Make a copy of the default configuration setup and add different setting
	// updates.
	//
	// When using sql.defaults.vectorize as the setting name, note that it's ok
	// that the default setup sets vectorize cluster setting to 'on' - we will
	// override it with queries below.
	defaultSetup := runConfig.clusterSetups[0]
	runConfig.clusterSetups = append(runConfig.clusterSetups, make([]string, len(defaultSetup)))
	copy(runConfig.clusterSetups[1], defaultSetup)
	runConfig.clusterSetups[tpchPerfTestOffConfigIdx] = append(runConfig.clusterSetups[tpchPerfTestOffConfigIdx],
		fmt.Sprintf("SET CLUSTER SETTING %s=off", p.settingName))
	runConfig.clusterSetups[tpchPerfTestOnConfigIdx] = append(runConfig.clusterSetups[tpchPerfTestOnConfigIdx],
		fmt.Sprintf("SET CLUSTER SETTING %s=on", p.settingName))
	runConfig.setupNames = make([]string, 2)
	runConfig.setupNames[tpchPerfTestOffConfigIdx] = fmt.Sprintf("%s=off", p.settingName)
	runConfig.setupNames[tpchPerfTestOnConfigIdx] = fmt.Sprintf("%s=on", p.settingName)
	return runConfig
}

func (p *tpchVecPerfTest) postQueryRunHook(t test.Test, output []byte, setupIdx int) {
	p.parseQueryOutput(t, output, setupIdx)
}

func (p *tpchVecPerfTest) postTestRunHook(
	ctx context.Context, t test.Test, c cluster.Cluster, conn *gosql.DB,
) {
	runConfig := p.getRunConfig()
	p.tpchVecPerfHelper.compareSetups(t, runConfig.numRunsPerQuery, func(queryNum int, onTime, offTime float64, onTimes, offTimes []float64) {
		if onTime >= p.slownessThreshold*offTime {
			// For some reason, the ON setup executed the query a lot slower
			// than the OFF setup which is unexpected.

			// Check whether we can reproduce this slowness to prevent false
			// positives.
			helper := newTpchVecPerfHelper(runConfig.setupNames)
			for setupIdx, setup := range runConfig.clusterSetups {
				performClusterSetup(t, conn, setup)
				result, err := c.RunWithDetailsSingleNode(
					ctx, t.L(), option.WithNodes(c.Node(1)),
					getTPCHVecWorkloadCmd(runConfig.numRunsPerQuery, queryNum, p.sharedProcess),
				)
				workloadOutput := result.Stdout + result.Stderr
				t.L().Printf(workloadOutput)
				if err != nil {
					// Note: if you see an error like "exit status 1", it is
					// likely caused by the erroneous output of the query.
					t.Fatal(err)
				}
				helper.parseQueryOutput(t, []byte(workloadOutput), setupIdx)
			}
			newOnTime, newOffTime := helper.getQueryTimes(t, runConfig.numRunsPerQuery, queryNum)
			if newOnTime < p.slownessThreshold*newOffTime {
				// This time the slowness threshold was satisfied, so we don't
				// fail the test.
				t.L().Printf(fmt.Sprintf(
					"[q%d] after re-running: %.2fs ON vs %.2fs OFF (proceeding)", queryNum, onTime, offTime,
				))
				return
			}
			t.L().Printf(fmt.Sprintf(
				"[q%d] after re-running: %.2fs ON vs %.2fs OFF (failing)", queryNum, onTime, offTime,
			))

			// In order to understand where the slowness comes from, we will run
			// EXPLAIN ANALYZE (DEBUG) of the query with all setup options
			// tpchPerfTestNumRunsPerQuery times (hoping at least one will
			// "catch" the slowness).
			for setupIdx, setup := range runConfig.clusterSetups {
				performClusterSetup(t, conn, setup)
				// performClusterSetup has changed the cluster settings;
				// however, the session variables might contain the old values,
				// so we will open up new connections for each of the setups in
				// order to get the correct cluster setup on each.
				var tenantName string
				if p.sharedProcessMT() {
					tenantName = appTenantName
				}
				tempConn, err := c.ConnE(ctx, t.L(), 1, option.VirtualClusterName(tenantName))
				if err != nil {
					t.Fatal(err)
				}
				//nolint:deferloop TODO(#137605)
				defer tempConn.Close()
				sqlConnCtx := clisqlclient.Context{}
				pgURL, err := c.ExternalPGUrl(ctx, t.L(), c.Node(1), roachprod.PGURLOptions{
					VirtualClusterName: tenantName,
				})
				if err != nil {
					t.Fatal(err)
				}
				connForBundle := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, pgURL[0])
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
					// The output of the command in both single-tenant and
					// multi-tenant configs contains a line like
					//
					//   SQL shell: \statement-diag download 951198764631457793
					//
					// We'll use that command to figure out the bundle ID and
					// then download the bundle into the artifacts.
					sqlShellPrefix := `SQL shell: \statement-diag download `
					var line, debugOutput string
					var bundleID int64
					for rows.Next() {
						if err = rows.Scan(&line); err != nil {
							t.Fatal(err)
						}
						debugOutput += line + "\n"
						if strings.HasPrefix(line, sqlShellPrefix) {
							id, err := strconv.Atoi(line[len(sqlShellPrefix):])
							if err != nil {
								t.Fatalf("couldn't parse bundle ID in %d\n%v", id, debugOutput)
							}
							bundleID = int64(id)
							break
						}
					}
					if err = rows.Close(); err != nil {
						t.Fatal(err)
					}
					if bundleID == 0 {
						t.Fatal(fmt.Sprintf("unexpectedly didn't find a line "+
							"with %q prefix in EXPLAIN ANALYZE (DEBUG) output\n%s",
							sqlShellPrefix, debugOutput))
					}
					dest := fmt.Sprintf("%s/bundle_%d_%d.zip", t.ArtifactsDir(), setupIdx, i)
					err = clisqlclient.StmtDiagDownloadBundle(ctx, connForBundle, bundleID, dest)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			msg := fmt.Sprintf(
				"[q%d] ON is slower by %.2f%% than OFF\n ON times: %v\nOFF times: %v",
				queryNum, 100*(onTime-offTime)/offTime, onTimes, offTimes,
			)
			if p.logOnSlowness {
				t.L().Printf(msg)
			} else {
				t.Fatal(msg)
			}
		}
	})
}

type tpchVecBenchTest struct {
	tpchVecTestCaseBase
	*tpchVecPerfHelper

	numRunsPerQuery int
	clusterSetups   [][]string
}

var _ tpchVecTestCase = &tpchVecBenchTest{}

func newTpchVecBenchTest(
	numRunsPerQuery int, clusterSetups [][]string, setupNames []string,
) *tpchVecBenchTest {
	return &tpchVecBenchTest{
		tpchVecPerfHelper: newTpchVecPerfHelper(setupNames),
		numRunsPerQuery:   numRunsPerQuery,
		clusterSetups:     clusterSetups,
	}
}

func (b tpchVecBenchTest) getRunConfig() tpchVecTestRunConfig {
	runConfig := b.tpchVecTestCaseBase.getRunConfig()
	runConfig.numRunsPerQuery = b.numRunsPerQuery
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
	for queryNum := 1; queryNum <= tpch.NumQueries; queryNum++ {
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

func (d tpchVecDiskTest) getRunConfig() tpchVecTestRunConfig {
	runConfig := d.tpchVecTestCaseBase.getRunConfig()

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
	rng, _ := randutil.NewTestRand()
	workmemInKiB := 650 + rng.Intn(1350)
	workmemQuery := fmt.Sprintf("SET CLUSTER SETTING sql.distsql.temp_storage.workmem='%dKiB'", workmemInKiB)
	for i := range runConfig.clusterSetups {
		runConfig.clusterSetups[i] = append(runConfig.clusterSetups[i], workmemQuery)
	}
	return runConfig
}

func getTPCHVecWorkloadCmd(numRunsPerQuery, queryNum int, sharedProcessMT bool) string {
	url := "{pgurl:1}"
	if sharedProcessMT {
		url = fmt.Sprintf("{pgurl:1:%s}", appTenantName)
	}
	// Note that we use --default-vectorize flag which tells tpch workload to
	// use the current cluster setting sql.defaults.vectorize which must have
	// been set correctly in preQueryRunHook.
	return fmt.Sprintf("./cockroach workload run tpch --concurrency=1 --db=tpch "+
		"--default-vectorize --max-ops=%d --queries=%d %s --enable-checks=true",
		numRunsPerQuery, queryNum, url)
}

func runTPCHVec(ctx context.Context, t test.Test, c cluster.Cluster, testCase tpchVecTestCase) {
	c.Start(ctx, t.L(), option.NewStartOpts(option.NoBackupSchedule), install.MakeClusterSettings())

	var conn *gosql.DB
	var disableMergeQueue bool
	if testCase.sharedProcessMT() {
		singleTenantConn := c.Conn(ctx, t.L(), 1)
		// Disable merge queue in the system tenant.
		if _, err := singleTenantConn.Exec("SET CLUSTER SETTING kv.range_merge.queue_enabled = false;"); err != nil {
			t.Fatal(err)
		}
		startOpts := option.StartSharedVirtualClusterOpts(appTenantName)
		c.StartServiceForVirtualCluster(ctx, t.L(), startOpts, install.MakeClusterSettings())
		conn = c.Conn(ctx, t.L(), c.All().RandNode()[0], option.VirtualClusterName(appTenantName))
	} else {
		conn = c.Conn(ctx, t.L(), 1)
		disableMergeQueue = true
	}

	t.Status("restoring TPCH dataset for Scale Factor 1")
	if err := loadTPCHDataset(
		ctx, t, c, conn, 1 /* sf */, c.NewMonitor(ctx), c.All(), disableMergeQueue,
	); err != nil {
		t.Fatal(err)
	}

	if _, err := conn.Exec("USE tpch;"); err != nil {
		t.Fatal(err)
	}
	scatterTables(t, conn, tpchTables)
	t.Status("waiting for full replication")
	err := roachtestutil.WaitFor3XReplication(ctx, t.L(), conn)
	require.NoError(t, err)

	runConfig := testCase.getRunConfig()
	for queryNum := 1; queryNum <= tpch.NumQueries; queryNum++ {
		for setupIdx, clusterSetup := range runConfig.clusterSetups {
			performClusterSetup(t, conn, clusterSetup)
			result, err := c.RunWithDetailsSingleNode(
				ctx, t.L(), option.WithNodes(c.Node(1)),
				getTPCHVecWorkloadCmd(runConfig.numRunsPerQuery, queryNum, testCase.sharedProcessMT()),
			)
			workloadOutput := result.Stdout + result.Stderr
			t.L().Printf(workloadOutput)
			if err != nil {
				// Note: if you see an error like "exit status 1", it is likely caused
				// by the erroneous output of the query.
				t.Fatal(err)
			}
			testCase.postQueryRunHook(t, []byte(workloadOutput), setupIdx)
		}
	}
	testCase.postTestRunHook(ctx, t, c, conn)
}

const tpchVecNodeCount = 3

func registerTPCHVec(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:      "tpchvec/perf",
		Owner:     registry.OwnerSQLQueries,
		Benchmark: true,
		Cluster:   r.MakeClusterSpec(tpchVecNodeCount),
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCHVec(ctx, t, c, newTpchVecPerfTest(
				"sql.defaults.vectorize", /* settingName */
				1.5,                      /* slownessThreshold */
				false,                    /* sharedProcessMT */
			))
		},
	})

	r.Add(registry.TestSpec{
		Name:    "tpchvec/disk",
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(tpchVecNodeCount),
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCHVec(ctx, t, c, tpchVecDiskTest{})
		},
	})

	r.Add(registry.TestSpec{
		Name:      "tpchvec/streamer",
		Owner:     registry.OwnerSQLQueries,
		Benchmark: true,
		Cluster:   r.MakeClusterSpec(tpchVecNodeCount),
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCHVec(ctx, t, c, newTpchVecPerfTest(
				"sql.distsql.use_streamer.enabled", /* settingName */
				1.5,                                /* slownessThreshold */
				false,                              /* sharedProcessMT */
			))
		},
	})

	r.Add(registry.TestSpec{
		Name:      "tpchvec/direct_scans",
		Owner:     registry.OwnerSQLQueries,
		Benchmark: true,
		Cluster:   r.MakeClusterSpec(tpchVecNodeCount),
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runTPCHVec(ctx, t, c, newTpchVecPerfTest(
				"sql.distsql.direct_columnar_scans.enabled", /* settingName */
				1.5,   /* slownessThreshold */
				false, /* sharedProcessMT */
			))
		},
	})

	r.Add(registry.TestSpec{
		Name:      "tpchvec/direct_scans/mt-shared-process",
		Owner:     registry.OwnerSQLQueries,
		Benchmark: true,
		Cluster:   r.MakeClusterSpec(tpchVecNodeCount),
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.Nightly),
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			p := newTpchVecPerfTest(
				"sql.distsql.direct_columnar_scans.enabled", /* settingName */
				1.5,  /* slownessThreshold */
				true, /* sharedProcessMT */
			)
			// Given that direct columnar scans are currently in an experimental
			// state, and we've seen a few failures where OFF config was
			// noticeably faster, for now we don't fail in such cases and simply
			// log.
			p.logOnSlowness = true
			runTPCHVec(ctx, t, c, p)
		},
	})

	r.Add(registry.TestSpec{
		Name:    "tpchvec/bench",
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(tpchVecNodeCount),
		// Uses gs://cockroach-fixtures-us-east1. See:
		// https://github.com/cockroachdb/cockroach/issues/105968
		CompatibleClouds: registry.Clouds(spec.GCE, spec.Local),
		Suites:           registry.Suites(registry.Nightly),
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
				5, /* numRunsPerQuery */
				clusterSetups,
				setupNames,
			)
			runTPCHVec(ctx, t, c, benchTest)
		},
	})
}
