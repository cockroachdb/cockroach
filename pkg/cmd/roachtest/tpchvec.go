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
	"math/rand"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/binfetcher"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
)

const (
	tpchVecNodeCount  = 3
	tpchVecNumQueries = 22
)

type crdbVersion int

const (
	tpchVecVersion19_2 crdbVersion = iota
	tpchVecVersion20_1
)

func toCRDBVersion(v string) (crdbVersion, error) {
	if strings.HasPrefix(v, "v19.2") {
		return tpchVecVersion19_2, nil
	} else if strings.HasPrefix(v, "v20.1") {
		return tpchVecVersion20_1, nil
	} else {
		return 0, errors.Errorf("unrecognized version: %s", v)
	}
}

var (
	vectorizeOnOptionByVersion = map[crdbVersion]string{
		tpchVecVersion19_2: "experimental_on",
		tpchVecVersion20_1: "on",
	}

	// queriesToSkipByVersion is a map keyed by version that contains query numbers
	// to be skipped for the given version (as well as the reasons for why they are skipped).
	queriesToSkipByVersion = map[crdbVersion]map[int]string{
		tpchVecVersion19_2: {
			5:  "can cause OOM",
			7:  "can cause OOM",
			8:  "can cause OOM",
			9:  "can cause OOM",
			19: "can cause OOM",
		},
	}

	// slownessThreshold describes the threshold at which we fail the test
	// if vec ON is slower that vec OFF, meaning that if
	// vec_on_time > vecOnSlowerFailFactor * vec_off_time, the test is failed.
	// This will help catch any regressions.
	// Note that for 19.2 version the threshold is higher in order to reduce
	// the noise.
	slownessThresholdByVersion = map[crdbVersion]float64{
		tpchVecVersion19_2: 1.5,
		tpchVecVersion20_1: 1.2,
	}
)

var tpchTables = []string{
	"nation", "region", "part", "supplier",
	"partsupp", "customer", "orders", "lineitem",
}

type tpchVecTestCase interface {
	// TODO(asubiotto): Getting the queries we want to run given a version should
	//  also be part of this.
	// vectorizeOptions are the vectorize options that each query will be run
	// with.
	vectorizeOptions() []bool
	// numRunsPerQuery is the number of times each tpch query should be run.
	numRunsPerQuery() int
	// preTestRunHook is called before any tpch query is run. Can be used to
	// perform setup.
	preTestRunHook(ctx context.Context, t *test, c *cluster, conn *gosql.DB, version crdbVersion)
	// postQueryRunHook is called after each tpch query is run with the output and
	// the vectorize mode it was run in.
	postQueryRunHook(t *test, output []byte, vectorized bool, batchSize string)
	// postTestRunHook is called after all tpch queries are run. Can be used to
	// perform teardown or general validation.
	postTestRunHook(t *test, conn *gosql.DB, version crdbVersion)
}

// tpchVecTestCaseBase is a default tpchVecTestCase implementation that can be
// embedded and extended.
type tpchVecTestCaseBase struct{}

func (b tpchVecTestCaseBase) vectorizeOptions() []bool {
	return []bool{true}
}

func (b tpchVecTestCaseBase) numRunsPerQuery() int {
	return 1
}

func (b tpchVecTestCaseBase) preTestRunHook(
	_ context.Context, t *test, _ *cluster, conn *gosql.DB, version crdbVersion,
) {
	if version != tpchVecVersion19_2 {
		t.Status("resetting sql.testing.vectorize.batch_size")
		if _, err := conn.Exec("RESET CLUSTER SETTING sql.testing.vectorize.batch_size"); err != nil {
			t.Fatal(err)
		}
	}
	t.Status("resetting workmem to default")
	if _, err := conn.Exec("RESET CLUSTER SETTING sql.distsql.temp_storage.workmem"); err != nil {
		t.Fatal(err)
	}
}

func (b tpchVecTestCaseBase) postQueryRunHook(
	t *test, output []byte, vectorized bool, batchSize string,
) {
}

func (b tpchVecTestCaseBase) postTestRunHook(_ *test, _ *gosql.DB, _ crdbVersion) {}

const (
	tpchPerfTestNumRunsPerQuery = 3
	tpchPerfTestVecOnConfigIdx  = 0
	tpchPerfTestVecOffConfigIdx = 1
)

type tpchVecPerfTest struct {
	tpchVecTestCaseBase
	timeByQueryNumAndBatchSize map[int]map[string][]float64
	timeByQueryNum             []map[int][]float64
}

var _ tpchVecTestCase = &tpchVecPerfTest{}

func newTpchVecPerfTest() *tpchVecPerfTest {
	return &tpchVecPerfTest{
		timeByQueryNum:             []map[int][]float64{make(map[int][]float64), make(map[int][]float64)},
		timeByQueryNumAndBatchSize: make(map[int]map[string][]float64),
	}
}

func (p tpchVecPerfTest) vectorizeOptions() []bool {
	// Since this is a performance test, each query should be run with both
	// vectorize modes.
	return []bool{true, false}
}

func (p tpchVecPerfTest) numRunsPerQuery() int {
	return tpchPerfTestNumRunsPerQuery
}

func (p *tpchVecPerfTest) postQueryRunHook(t *test, output []byte, _ bool, batchSize string) {

	/*
		configIdx := tpchPerfTestVecOffConfigIdx
		if vectorized {
			configIdx = tpchPerfTestVecOnConfigIdx
		}*/
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
			if _, ok := p.timeByQueryNumAndBatchSize[queryNum]; !ok {
				p.timeByQueryNumAndBatchSize[queryNum] = make(map[string][]float64)
			}
			p.timeByQueryNumAndBatchSize[queryNum][batchSize] = append(p.timeByQueryNumAndBatchSize[queryNum][batchSize], queryTime)

			//p.timeByQueryNum[configIdx][queryNum] = append(p.timeByQueryNum[configIdx][queryNum], queryTime)
		}
	}
}

// Should output queryNum,batchSize,time. It would be great if it were queryNum, batchSize1time, batchSize2time

func (p *tpchVecPerfTest) postTestRunHook(t *test, _ *gosql.DB, version crdbVersion) {
	f, err := os.Create("lookup_join_test_output")
	if err != nil {
		panic(p.timeByQueryNumAndBatchSize)
	}
	defer f.Close()

	for _, queryNum := range orderedQueryNums {
		if _, ok := p.timeByQueryNumAndBatchSize[queryNum]; !ok {
			t.l.Printf("unexpectedly could not find times for query %d", queryNum)
			continue
		}

		// Get median time for each batch size.
		medianTimes := make([]float64, len(orderedBatchSizes))
		for i, batchSize := range orderedBatchSizes {
			times, ok := p.timeByQueryNumAndBatchSize[queryNum][batchSize]
			if !ok {
				t.l.Printf("missing a time for query %d and batchSize %s", queryNum, batchSize)
				medianTimes[i] = 0
				continue
			}

			// Get median time.
			sort.Float64s(times)

			median := len(times) / 2
			if median == 0 {
				medianTimes[i] = 0
				continue
			}
			medianTimes[i] = times[median]
		}
		// We now have all times for a given query ordered by batch size, write a cs
		t.l.Printf("times for query %d are %v", queryNum, medianTimes)
		var b strings.Builder
		b.WriteString(fmt.Sprintf("%d", queryNum))
		for _, time := range medianTimes {
			b.WriteString(fmt.Sprintf(",%.2f", time))
		}
		b.WriteString("\n")
		if _, err := f.WriteString(b.String()); err != nil {
			t.Fatal(err)
		}
	}

	/*
		queriesToSkip := queriesToSkipByVersion[version]
		t.Status("comparing the runtimes (only median values for each query are compared)")
		for queryNum := 1; queryNum <= tpchVecNumQueries; queryNum++ {
			if _, skipped := queriesToSkip[queryNum]; skipped {
				continue
			}
			findMedian := func(times []float64) float64 {
				sort.Float64s(times)
				return times[len(times)/2]
			}
			vecOnTimes := p.timeByQueryNum[tpchPerfTestVecOnConfigIdx][queryNum]
			vecOffTimes := p.timeByQueryNum[tpchPerfTestVecOffConfigIdx][queryNum]
			if len(vecOnTimes) != tpchPerfTestNumRunsPerQuery {
				t.Fatal(fmt.Sprintf("[q%d] unexpectedly wrong number of run times "+
					"recorded with vec ON config: %v", queryNum, vecOnTimes))
			}
			if len(vecOffTimes) != tpchPerfTestNumRunsPerQuery {
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
			if vecOnTime >= slownessThresholdByVersion[version]*vecOffTime {
				t.Fatal(fmt.Sprintf(
					"[q%d] vec ON is slower by %.2f%% than vec OFF\n"+
						"vec ON times: %v\nvec OFF times: %v",
					queryNum, 100*(vecOnTime-vecOffTime)/vecOffTime, vecOnTimes, vecOffTimes))
			}
		}
	*/
}

type tpchVecDiskTest struct {
	tpchVecTestCaseBase
}

func (d tpchVecDiskTest) preTestRunHook(
	ctx context.Context, t *test, c *cluster, conn *gosql.DB, version crdbVersion,
) {
	d.tpchVecTestCaseBase.preTestRunHook(ctx, t, c, conn, version)
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
	ctx context.Context, t *test, c *cluster, conn *gosql.DB, version crdbVersion,
) {
	b.tpchVecTestCaseBase.preTestRunHook(ctx, t, c, conn, version)
	rng, _ := randutil.NewPseudoRand()
	setSmallBatchSize(t, conn, rng)
}

var orderedQueryNums = []int{1, 2, 4, 6, 7, 8, 9, 10, 11, 16, 20, 21}
var orderedBatchSizes = []string{"warmup", "0", "2MiB", "4MiB", "6MiB", "10MiB", "20MiB", "30MiB", "54MiB"}

func baseTestRun(
	ctx context.Context, t *test, c *cluster, version crdbVersion, tc tpchVecTestCase,
) {
	firstNode := c.Node(1)
	//queriesToSkip := queriesToSkipByVersion[version]
	conn := c.Conn(ctx, 1)
	for _, queryNum := range orderedQueryNums { //1; queryNum <= tpchVecNumQueries; queryNum++ {
		for _, batchSize := range orderedBatchSizes { //tc.vectorizeOptions() {
			/*if reason, skip := queriesToSkip[queryNum]; skip {
				t.Status(fmt.Sprintf("skipping q%d because of %q", queryNum, reason))
				continue
			}*/
			if batchSize != "warmup" {
				t.Status(fmt.Sprintf("running query %d with batchSize %s", queryNum, batchSize))
				if _, err := conn.Exec(fmt.Sprintf("SET CLUSTER SETTING ljbytes='%s'", batchSize)); err != nil {
					t.Fatal(err)
				}
			}
			cmd := fmt.Sprintf("./workload run tpch --concurrency=1 --db=tpch "+
				"--max-ops=%d --vectorize=on --queries=%d {pgurl:1-%d}",
				tc.numRunsPerQuery(), queryNum, tpchVecNodeCount)
			workloadOutput, err := c.RunWithBuffer(ctx, t.l, firstNode, cmd)
			t.l.Printf("\n" + string(workloadOutput))
			if err != nil {
				// Note: if you see an error like "exit status 1", it is likely caused
				// by the erroneous output of the query.
				t.Fatal(err)
			}
			tc.postQueryRunHook(t, workloadOutput, false, batchSize)
		}
	}
}

type tpchVecSmithcmpTest struct {
	tpchVecTestCaseBase
}

const tpchVecSmithcmp = "smithcmp"

func (s tpchVecSmithcmpTest) preTestRunHook(
	ctx context.Context, t *test, c *cluster, conn *gosql.DB, version crdbVersion,
) {
	s.tpchVecTestCaseBase.preTestRunHook(ctx, t, c, conn, version)
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

func smithcmpTestRun(ctx context.Context, t *test, c *cluster, _ crdbVersion, _ tpchVecTestCase) {
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
	testRun func(ctx context.Context, t *test, c *cluster, version crdbVersion, tc tpchVecTestCase),
) {
	firstNode := c.Node(1)
	c.Put(ctx, cockroach, "./cockroach", c.All())
	c.Put(ctx, workload, "./workload", firstNode)
	c.Start(ctx, t)

	conn := c.Conn(ctx, 1)
	disableAutoStats(t, conn)
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
	createStatsFromTables(t, conn, tpchTables)
	versionString, err := fetchCockroachVersion(ctx, c, c.Node(1)[0])
	if err != nil {
		t.Fatal(err)
	}
	version, err := toCRDBVersion(versionString)
	if err != nil {
		t.Fatal(err)
	}

	testCase.preTestRunHook(ctx, t, c, conn, version)
	testRun(ctx, t, c, version, testCase)
	testCase.postTestRunHook(t, conn, version)
}

func registerTPCHVec(r *testRegistry) {
	r.Add(testSpec{
		Name:       "tpchvec/perf",
		Owner:      OwnerSQLExec,
		Cluster:    makeClusterSpec(tpchVecNodeCount),
		MinVersion: "v19.2.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCHVec(ctx, t, c, newTpchVecPerfTest(), baseTestRun)
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
