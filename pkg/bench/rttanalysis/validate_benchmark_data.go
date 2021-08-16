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

type BenchmarkResult struct {
	Name   string
	Result int
}

type BenchmarkExpectation struct {
	Name string

	// Min and Max are the expected number KV round-trips that should be performed
	// in this benchmark.
	Min, Max int
}

const expectationsFilename = "benchmark_expectations"

var expectationsHeader = []string{"exp", "benchmark"}

var (
	RewriteFlag = flag.String("rewrite", "",
		"if non-empty, a regexp of benchmarks to rewrite")
	RewriteIterations = flag.Int("rewrite-iterations", 50,
		"if re-writing, the number of times to execute each benchmark to "+
			"determine the range of possible values")
)

func GetBenchmarks(t *testing.T) (benchmarks []string) {
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

func RunBenchmarks(t *testing.T, flags ...string) []BenchmarkResult {
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

func readBenchmarkResults(t *testing.T, benchmarkOutput io.Reader) []BenchmarkResult {
	var ret []BenchmarkResult
	sc := bufio.NewScanner(benchmarkOutput)
	for sc.Scan() {
		match := benchmarkResultRe.FindStringSubmatch(sc.Text())
		if match == nil {
			continue
		}
		testName := match[testNameIdx]
		got, err := strconv.ParseFloat(match[roundtripsIdx], 64)
		require.NoError(t, err)
		ret = append(ret, BenchmarkResult{
			Name:   testName,
			Result: int(got),
		})
	}
	require.NoError(t, sc.Err())
	return ret
}

// RewriteBenchmarkExpectations re-runs the specified benchmarks and throws out
// the existing values in the results file. All other values are preserved.
func RewriteBenchmarkExpectations(t *testing.T, benchmarks []string) {

	// Split off the filter so as to avoid spinning off unnecessary subprocesses.
	slashIdx := strings.Index(*RewriteFlag, "/")
	var afterSlash string
	if slashIdx == -1 {
		slashIdx = len(*RewriteFlag)
	} else {
		afterSlash = (*RewriteFlag)[slashIdx+1:]
	}
	benchmarkFilter, err := regexp.Compile((*RewriteFlag)[:slashIdx])
	require.NoError(t, err)

	var g errgroup.Group
	resChan := make(chan []BenchmarkResult)
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
					"--rewrite", *RewriteFlag,
					"--test.count", strconv.Itoa(*RewriteIterations),
				}
				if testing.Verbose() {
					flags = append(flags, "--test.v")
				}
				resChan <- RunBenchmarks(t, flags...)
			})
			return nil
		})
	}
	for _, b := range benchmarks {
		run(b)
	}
	go func() { _ = g.Wait(); close(resChan) }()
	var results []BenchmarkResult
	for res := range resChan {
		results = append(results, res...)
	}

	rewritePattern, err := regexp.Compile(*RewriteFlag)
	require.NoError(t, err)
	expectations := ReadExpectationsFile(t)
	expectations = removeMatching(expectations, rewritePattern)
	expectations = append(removeMatching(expectations, rewritePattern),
		resultsToExpectations(results)...)
	sort.Sort(expectations)

	// Verify there aren't any duplicates.
	for i := 1; i < len(expectations); i++ {
		if expectations[i-1].Name == expectations[i].Name {
			t.Fatalf("duplicate expecatations for Name %s", expectations[i].Name)
		}
	}

	writeExpectationsFile(t, expectations)
}

func removeMatching(
	expectations BenchmarkExpectations, rewritePattern *regexp.Regexp,
) BenchmarkExpectations {
	truncated := expectations[:0]
	for i := range expectations {
		if rewritePattern.MatchString(expectations[i].Name) {
			continue
		}
		truncated = append(truncated, expectations[i])
	}
	return truncated
}

func resultsToExpectations(results []BenchmarkResult) BenchmarkExpectations {
	sort.Slice(results, func(i, j int) bool {
		return results[i].Name < results[j].Name
	})
	var res BenchmarkExpectations
	var cur BenchmarkExpectation
	for _, result := range results {
		if result.Name != cur.Name {
			if cur != (BenchmarkExpectation{}) {
				res = append(res, cur)
				cur = BenchmarkExpectation{}
			}
			cur = BenchmarkExpectation{
				Name: result.Name,
				Min:  result.Result,
				Max:  result.Result,
			}
		}
		if result.Result < cur.Min {
			cur.Min = result.Result
		}
		if result.Result > cur.Max {
			cur.Max = result.Result
		}
	}
	if cur != (BenchmarkExpectation{}) {
		res = append(res, cur)
	}
	return res
}

func writeExpectationsFile(t *testing.T, expectations BenchmarkExpectations) {
	f, err := os.Create(testutils.TestDataPath(t, expectationsFilename))
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()
	w := csv.NewWriter(f)
	w.Comma = ','
	require.NoError(t, w.Write(expectationsHeader))
	for _, exp := range expectations {
		require.NoError(t, w.Write([]string{exp.String(), exp.Name}))
	}
	w.Flush()
	require.NoError(t, w.Error())
}

// ReadExpectationsFile reads the benchmark performance expectations from file.
func ReadExpectationsFile(t testing.TB) BenchmarkExpectations {
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
	ret := make(BenchmarkExpectations, len(records))

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
		ret[i] = BenchmarkExpectation{Min: min, Max: max, Name: r[1]}
	}
	sort.Sort(ret)
	return ret
}

// MakeBenchmarkSubtest creates a subtest function for the given benchmark based
// on these expectations.
func (b BenchmarkExpectations) MakeBenchmarkSubtest(benchmark string) func(t *testing.T) {
	flags := []string{
		"--test.run=^$",
		"--test.bench=" + benchmark,
		"--test.benchtime=1x",
	}
	if testing.Verbose() {
		flags = append(flags, "--test.v")
	}

	return func(t *testing.T) {
		results := RunBenchmarks(t, flags...)

		for _, r := range results {
			exp, ok := b.Find(r.Name)
			if !ok {
				t.Logf("no expectation for benchmark %s, got %d", r.Name, r.Result)
				continue
			}
			if !exp.Matches(r.Result) {
				t.Errorf("fail: expected %s to perform KV lookups in [%d, %d], got %d",
					r.Name, exp.Min, exp.Max, r.Result)
			} else {
				t.Logf("success: expected %s to perform KV lookups in [%d, %d], got %d",
					r.Name, exp.Min, exp.Max, r.Result)
			}
		}
	}
}

func (b BenchmarkExpectations) Find(name string) (BenchmarkExpectation, bool) {
	idx := sort.Search(len(b), func(i int) bool {
		return b[i].Name >= name
	})
	if idx < len(b) && b[idx].Name == name {
		return b[idx], true
	}
	return BenchmarkExpectation{}, false
}

func (e BenchmarkExpectation) Matches(roundTrips int) bool {
	return e.Min <= roundTrips && roundTrips <= e.Max
}

func (e BenchmarkExpectation) String() string {
	expStr := strconv.Itoa(e.Min)
	if e.Min != e.Max {
		expStr += "-"
		expStr += strconv.Itoa(e.Max)
	}
	return expStr
}

type BenchmarkExpectations []BenchmarkExpectation

var _ sort.Interface = (BenchmarkExpectations)(nil)

func (b BenchmarkExpectations) Len() int           { return len(b) }
func (b BenchmarkExpectations) Less(i, j int) bool { return b[i].Name < b[j].Name }
func (b BenchmarkExpectations) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
