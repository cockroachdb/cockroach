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
	"regexp"
	"sort"
	"strconv"
	"strings"

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
	//  also be part of this. This can also be where we return tpch queries with
	//  random placeholders.
	// vectorizeOptions are the vectorize options that each query will be run
	// with.
	vectorizeOptions() []bool
	// numRunsPerQuery is the number of times each tpch query should be run.
	numRunsPerQuery() int
	// preTestRunHook is called before any tpch query is run. Can be used to
	// perform setup.
	preTestRunHook(t *test, conn *gosql.DB, version crdbVersion)
	// postQueryRunHook is called after each tpch query is run with the output and
	// the vectorize mode it was run in.
	postQueryRunHook(t *test, output []byte, vectorized bool)
	// postTestRunHook is called after all tpch queries are run. Can be used to
	// perform teardown or general validation.
	postTestRunHook(t *test, conn *gosql.DB, version crdbVersion)
}

// tpchVecTestCaseBase is a default tpchVecTestCase implementation that can be
// embedded and extended.
type tpchVecTestCaseBase struct{}

func (r tpchVecTestCaseBase) vectorizeOptions() []bool {
	return []bool{true}
}

func (r tpchVecTestCaseBase) numRunsPerQuery() int {
	return 1
}

func (r tpchVecTestCaseBase) preTestRunHook(t *test, conn *gosql.DB, version crdbVersion) {
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

func (r tpchVecTestCaseBase) postQueryRunHook(_ *test, _ []byte, _ bool) {}

func (r tpchVecTestCaseBase) postTestRunHook(_ *test, _ *gosql.DB, _ crdbVersion) {}

const (
	tpchPerfTestNumRunsPerQuery = 3
	tpchPerfTestVecOnConfigIdx  = 0
	tpchPerfTestVecOffConfigIdx = 1
)

type tpchVecPerfTest struct {
	tpchVecTestCaseBase
	timeByQueryNum []map[int][]float64
}

var _ tpchVecTestCase = &tpchVecPerfTest{}

func newTpchVecPerfTest() *tpchVecPerfTest {
	return &tpchVecPerfTest{
		timeByQueryNum: []map[int][]float64{make(map[int][]float64), make(map[int][]float64)},
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

func (p *tpchVecPerfTest) postQueryRunHook(t *test, output []byte, vectorized bool) {
	configIdx := tpchPerfTestVecOffConfigIdx
	if vectorized {
		configIdx = tpchPerfTestVecOnConfigIdx
	}
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
			p.timeByQueryNum[configIdx][queryNum] = append(p.timeByQueryNum[configIdx][queryNum], queryTime)
		}
	}
}

func (p *tpchVecPerfTest) postTestRunHook(t *test, _ *gosql.DB, version crdbVersion) {
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
}

type tpchVecDiskTest struct {
	tpchVecTestCaseBase
}

func (d tpchVecDiskTest) preTestRunHook(t *test, conn *gosql.DB, version crdbVersion) {
	d.tpchVecTestCaseBase.preTestRunHook(t, conn, version)
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

type tpchVecSmallBatchSizeTest struct {
	tpchVecTestCaseBase
}

func (b tpchVecSmallBatchSizeTest) preTestRunHook(t *test, conn *gosql.DB, version crdbVersion) {
	b.tpchVecTestCaseBase.preTestRunHook(t, conn, version)
	rng, _ := randutil.NewPseudoRand()
	batchSize := 1 + rng.Intn(4)
	t.Status(fmt.Sprintf("setting sql.testing.vectorize.batch_size to %d", batchSize))
	if _, err := conn.Exec(fmt.Sprintf("SET CLUSTER SETTING sql.testing.vectorize.batch_size=%d", batchSize)); err != nil {
		t.Fatal(err)
	}
}

func runTPCHVec(ctx context.Context, t *test, c *cluster, testCase tpchVecTestCase) {
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
	queriesToSkip := queriesToSkipByVersion[version]

	testCase.preTestRunHook(t, conn, version)

	for queryNum := 1; queryNum <= tpchVecNumQueries; queryNum++ {
		for _, vectorize := range testCase.vectorizeOptions() {
			if reason, skip := queriesToSkip[queryNum]; skip {
				t.Status(fmt.Sprintf("skipping q%d because of %q", queryNum, reason))
				continue
			}
			vectorizeSetting := "off"
			if vectorize {
				vectorizeSetting = vectorizeOnOptionByVersion[version]
			}
			cmd := fmt.Sprintf("./workload run tpch --concurrency=1 --db=tpch "+
				"--max-ops=%d --queries=%d --vectorize=%s {pgurl:1-%d}",
				testCase.numRunsPerQuery(), queryNum, vectorizeSetting, tpchVecNodeCount)
			workloadOutput, err := c.RunWithBuffer(ctx, t.l, firstNode, cmd)
			t.l.Printf("\n" + string(workloadOutput))
			if err != nil {
				// Note: if you see an error like "exit status 1", it is likely caused
				// by the erroneous output of the query.
				t.Fatal(err)
			}
			testCase.postQueryRunHook(t, workloadOutput, vectorize)
		}
	}
	testCase.postTestRunHook(t, conn, version)
}

func registerTPCHVec(r *testRegistry) {
	r.Add(testSpec{
		Name:       "tpchvec/perf",
		Owner:      OwnerSQLExec,
		Cluster:    makeClusterSpec(tpchVecNodeCount),
		MinVersion: "v19.2.0",
		Run: func(ctx context.Context, t *test, c *cluster) {
			runTPCHVec(ctx, t, c, newTpchVecPerfTest())
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
			runTPCHVec(ctx, t, c, tpchVecDiskTest{})
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
			runTPCHVec(ctx, t, c, tpchVecSmallBatchSizeTest{})
		},
	})
}
