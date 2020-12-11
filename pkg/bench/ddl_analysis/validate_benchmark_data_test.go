// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bench_test

import (
	"bufio"
	"bytes"
	"flag"
	"io"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/stretchr/testify/require"
)

type benchmarkResult struct {
	name   string
	result int
}

type benchmarkExpectation struct {
	name string

	// min and max are the expected number KV round-trips that should be performed
	// in this benchmark.
	min, max int
}

const expectationsFilename = "benchmark_expectations"

var expectationsHeader = []string{"min", "max", "benchmark"}

var (
	rewriteFlag = flag.String("rewrite", "",
		"if non-empty, a regexp of benchmarks to rewrite")
	rewriteIterations = flag.Int("rewrite-iterations", 50,
		"if re-writing, the number of times to execute each benchmark to "+
			"determine the range of possible values")
)

// TestBenchmarkExpectation runs all of the benchmarks and
// one iteration and validates that the number of RPCs meets
// the expectation.
//
// It takes a long time and thus is skipped under stress, race
// and short.
func TestBenchmarkExpectation(t *testing.T) {
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderShort(t)

	expecations := readExpectationsFile(t)

	if *rewriteFlag != "" {
		rewriteBenchmarkExpecations(t)
		return
	}

	defer func() {
		if t.Failed() {
			t.Log("see the --rewrite flag to re-run the benchmarks and adjust the expectations")
		}
	}()

	results := runBenchmarks(t,
		"--test.run=^$",
		"--test.bench=.",
		"--test.benchtime=1x")

	for _, r := range results {
		exp, ok := expecations.find(r.name)
		if !ok {
			t.Logf("no expectation for benchmark %s, got %d", r.name, r.result)
			continue
		}
		if exp.min > r.result || exp.max < r.result {
			t.Errorf("expected %s to perform KV lookups in [%d, %d], got %d",
				r.name, exp.min, exp.max, r.result)
		}
	}
}

func runBenchmarks(t *testing.T, flags ...string) []benchmarkResult {
	cmd := exec.Command(os.Args[0], flags...)
	t.Log(cmd)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("failed to run cmd %q: %v\nstderr:\n%v", cmd, err, stderr)
	}
	return readBenchmarkResults(t, &stdout)
}

var (
	benchmarkResultRe = regexp.MustCompile(`^Benchmark(?P<testname>.*)-\d+\s.*\s(?P<roundtrips>\S+)\sroundtrips$`)
	testNameIdx       = benchmarkResultRe.SubexpIndex("testname")
	roundtripsIdx     = benchmarkResultRe.SubexpIndex("roundtrips")
)

func readBenchmarkResults(t *testing.T, benchmarkOutput io.Reader) []benchmarkResult {
	var ret []benchmarkResult
	sc := bufio.NewScanner(benchmarkOutput)
	for sc.Scan() {
		match := benchmarkResultRe.FindStringSubmatch(sc.Text())
		if match == nil {
			continue
		}
		testName := match[testNameIdx]
		got, err := strconv.ParseFloat(match[roundtripsIdx], 64)
		require.NoError(t, err)
		ret = append(ret, benchmarkResult{
			name:   testName,
			result: int(got),
		})
	}
	require.NoError(t, sc.Err())
	return ret
}

// rewriteBenchmarkExpectations re-runs the specified benchmarks and throws out
// the existing values in the results file. All other values are preserved.
func rewriteBenchmarkExpecations(t *testing.T) {
	rewritePattern, err := regexp.Compile(*rewriteFlag)
	require.NoError(t, err)
	expectations := readExpectationsFile(t)

	expectations = removeMatching(expectations, rewritePattern)
	results := runBenchmarks(t,
		"--test.run", "^$",
		"--test.benchtime", "1x",
		"--test.bench", *rewriteFlag,
		"--test.count", strconv.Itoa(*rewriteIterations))
	expectations = append(removeMatching(expectations, rewritePattern),
		resultsToExpectations(results)...)
	sort.Sort(expectations)

	// Verify there aren't any duplicates.
	for i := 1; i < len(expectations); i++ {
		if expectations[i-1].name == expectations[i].name {
			t.Fatalf("duplicate expecatations for name %s", expectations[i].name)
		}
	}

	writeExpectationsFile(t, expectations)
}

func removeMatching(
	expectations benchmarkExpectations, rewritePattern *regexp.Regexp,
) benchmarkExpectations {
	truncated := expectations[:0]
	for i := range expectations {
		if rewritePattern.MatchString(expectations[i].name) {
			continue
		}
		truncated = append(truncated, expectations[i])
	}
	return truncated
}

func resultsToExpectations(results []benchmarkResult) benchmarkExpectations {
	sort.Slice(results, func(i, j int) bool {
		return results[i].name < results[j].name
	})
	var res benchmarkExpectations
	var cur benchmarkExpectation
	for _, result := range results {
		if result.name != cur.name {
			if cur != (benchmarkExpectation{}) {
				res = append(res, cur)
				cur = benchmarkExpectation{}
			}
			cur = benchmarkExpectation{
				name: result.name,
				min:  result.result,
				max:  result.result,
			}
		}
		if result.result < cur.min {
			cur.min = result.result
		}
		if result.result > cur.max {
			cur.max = result.result
		}
	}
	if cur != (benchmarkExpectation{}) {
		res = append(res, cur)
	}
	return res
}

func writeExpectationsFile(t *testing.T, expectations benchmarkExpectations) {
	f, err := os.Create(testutils.TestDataPath("testdata", expectationsFilename))
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	w := csv.NewWriter(f)
	w.Comma = '\t'
	require.NoError(t, w.Write(expectationsHeader))
	for _, exp := range expectations {
		require.NoError(t, w.Write([]string{
			strconv.Itoa(exp.min),
			strconv.Itoa(exp.max),
			exp.name,
		}))
	}
	w.Flush()
	require.NoError(t, w.Error())
}

func readExpectationsFile(t *testing.T) benchmarkExpectations {
	f, err := os.Open(testutils.TestDataPath("testdata", expectationsFilename))
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	r := csv.NewReader(f)
	r.Comma = '\t'
	records, err := r.ReadAll()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(records), 1, "must have at least a header")
	require.Equal(t, expectationsHeader, records[0])
	records = records[1:] // strip header
	ret := make(benchmarkExpectations, len(records))
	for i, r := range records {
		min, err := strconv.Atoi(r[0])
		require.NoErrorf(t, err, "line %d", i+1)
		max, err := strconv.Atoi(r[1])
		require.NoErrorf(t, err, "line %d", i+1)
		ret[i] = benchmarkExpectation{
			name: r[2],
			min:  min,
			max:  max,
		}
	}
	sort.Sort(ret)
	return ret
}

func (b benchmarkExpectations) find(name string) (benchmarkExpectation, bool) {
	idx := sort.Search(len(b), func(i int) bool {
		return b[i].name >= name
	})
	if idx < len(b) && b[idx].name == name {
		return b[idx], true
	}
	return benchmarkExpectation{}, false
}

type benchmarkExpectations []benchmarkExpectation

var _ sort.Interface = (benchmarkExpectations)(nil)

func (b benchmarkExpectations) Len() int           { return len(b) }
func (b benchmarkExpectations) Less(i, j int) bool { return b[i].name < b[j].name }
func (b benchmarkExpectations) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
