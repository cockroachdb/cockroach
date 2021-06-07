// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rttanalysis

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"flag"
	"io"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
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

var expectationsHeader = []string{"exp", "benchmark"}

var (
	rewriteFlag = flag.String("rewrite", "",
		"if non-empty, a regexp of benchmarks to rewrite")
	rewriteIterations = flag.Int("rewrite-iterations", 50,
		"if re-writing, the number of times to execute each benchmark to "+
			"determine the range of possible values")
)

func getBenchmarks(t *testing.T) (benchmarks []string) {
	cmd := exec.Command(os.Args[0], "--test.list", "^Benchmark")
	var out bytes.Buffer
	cmd.Stdout = &out
	require.NoError(t, cmd.Run())
	sc := bufio.NewScanner(&out)
	for sc.Scan() {
		benchmarks = append(benchmarks, sc.Text())
	}
	require.NoError(t, sc.Err())
	return benchmarks
}
func runBenchmarks(t *testing.T, flags ...string) []benchmarkResult {
	cmd := exec.Command(os.Args[0], flags...)

	// Disable metamorphic testing in the subprocesses.
	env := os.Environ()
	env = append(env, util.DisableMetamorphicEnvVar+"=t")
	cmd.Env = env
	t.Log(cmd)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	var stdoutBuf bytes.Buffer
	var g errgroup.Group
	g.Go(func() error {
		defer stdout.Close()
		_, err := io.Copy(os.Stdout, io.TeeReader(stdout, &stdoutBuf))
		return err
	})
	require.NoErrorf(t, cmd.Start(), "failed to start command %v", cmd)
	require.NoError(t, g.Wait())
	require.NoErrorf(t, cmd.Wait(), "failed to wait for command %v", cmd)
	return readBenchmarkResults(t, &stdoutBuf)
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
func rewriteBenchmarkExpecations(t *testing.T, benchmarks []string) {

	// Split off the filter so as to avoid spinning off unnecessary subprocesses.
	slashIdx := strings.Index(*rewriteFlag, "/")
	var afterSlash string
	if slashIdx == -1 {
		slashIdx = len(*rewriteFlag)
	} else {
		afterSlash = (*rewriteFlag)[slashIdx+1:]
	}
	benchmarkFilter, err := regexp.Compile((*rewriteFlag)[:slashIdx])
	require.NoError(t, err)

	var g errgroup.Group
	resChan := make(chan []benchmarkResult)
	run := func(b string) {
		if !benchmarkFilter.MatchString(b) {
			return
		}
		g.Go(func() error {
			t.Run(b, func(t *testing.T) {
				flags := []string{
					"--test.run", "^$",
					"--test.benchtime", "1x",
					"--test.bench", b + "/" + afterSlash,
					"--rewrite", *rewriteFlag,
					"--test.count", strconv.Itoa(*rewriteIterations),
				}
				if testing.Verbose() {
					flags = append(flags, "--test.v")
				}
				resChan <- runBenchmarks(t, flags...)
			})
			return nil
		})
	}
	for _, b := range benchmarks {
		run(b)
	}
	go func() { _ = g.Wait(); close(resChan) }()
	var results []benchmarkResult
	for res := range resChan {
		results = append(results, res...)
	}

	rewritePattern, err := regexp.Compile(*rewriteFlag)
	require.NoError(t, err)
	expectations := readExpectationsFile(t)
	expectations = removeMatching(expectations, rewritePattern)
	expectations = append(removeMatching(expectations, rewritePattern),
		resultsToExpectations(results)...)
	sort.Sort(expectations)

	// Verify there aren't any duplicates.
	for i := 1; i < len(expectations); i++ {
		if expectations[i-1].name == expectations[i].name {
			t.Fatalf("duplicate expecatations for Name %s", expectations[i].name)
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
	f, err := os.Create(testutils.TestDataPath(t, expectationsFilename))
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	w := csv.NewWriter(f)
	w.Comma = ','
	require.NoError(t, w.Write(expectationsHeader))
	for _, exp := range expectations {
		require.NoError(t, w.Write([]string{exp.String(), exp.name}))
	}
	w.Flush()
	require.NoError(t, w.Error())
}

func readExpectationsFile(t testing.TB) benchmarkExpectations {
	f, err := os.Open(testutils.TestDataPath(t, expectationsFilename))
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	r := csv.NewReader(f)
	r.Comma = ','
	records, err := r.ReadAll()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(records), 1, "must have at least a header")
	require.Equal(t, expectationsHeader, records[0])
	records = records[1:] // strip header
	ret := make(benchmarkExpectations, len(records))

	parseExp := func(expStr string) (min, max int, err error) {
		split := strings.Split(expStr, "-")
		if len(split) > 2 {
			return 0, 0, errors.Errorf("expected <min>-<max>, got %q", expStr)
		}
		min, err = strconv.Atoi(split[0])
		if err != nil {
			return 0, 0, err
		}
		if len(split) == 1 {
			max = min
			return min, max, err
		}
		max, err = strconv.Atoi(split[1])
		return min, max, err
	}
	for i, r := range records {
		min, max, err := parseExp(r[0])
		require.NoErrorf(t, err, "line %d", i+1)
		ret[i] = benchmarkExpectation{min: min, max: max, name: r[1]}
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

func (e benchmarkExpectation) matches(roundTrips int) bool {
	return e.min <= roundTrips && roundTrips <= e.max
}

func (e benchmarkExpectation) String() string {
	expStr := strconv.Itoa(e.min)
	if e.min != e.max {
		expStr += "-"
		expStr += strconv.Itoa(e.max)
	}
	return expStr
}

type benchmarkExpectations []benchmarkExpectation

var _ sort.Interface = (benchmarkExpectations)(nil)

func (b benchmarkExpectations) Len() int           { return len(b) }
func (b benchmarkExpectations) Less(i, j int) bool { return b[i].name < b[j].name }
func (b benchmarkExpectations) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
