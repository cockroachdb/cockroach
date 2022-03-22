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
	"encoding/csv"
	"flag"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/system"
	"github.com/cockroachdb/errors"
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

var expectationsHeader = []string{"exp", "benchmark"}

var (
	rewriteFlag = flag.Bool("rewrite", false,
		"if non-empty, a regexp of benchmarks to rewrite")
	rewriteIterations = flag.Int("rewrite-iterations", 50,
		"if re-writing, the number of times to execute each benchmark to "+
			"determine the range of possible values")
)

// RunBenchmarkExpectationTests runs tests to validate or rewrite the contents
// of the benchmark expectations file.
func runBenchmarkExpectationTests(t *testing.T, r *Registry) {
	if util.IsMetamorphicBuild() {
		execTestSubprocess(t)
		return
	}

	// Only create the scope after we've checked if we need to exec the subprocess.
	scope := log.Scope(t)
	defer scope.Close(t)

	defer func() {
		if t.Failed() {
			t.Log("see the --rewrite flag to re-run the benchmarks and adjust the expectations")
		}
	}()

	var results resultSet
	var wg sync.WaitGroup
	concurrency := ((system.NumCPU() - 1) / r.numNodes) + 1 // arbitrary
	limiter := quotapool.NewIntPool("rttanalysis", uint64(concurrency))
	isRewrite := *rewriteFlag
	for b, cases := range r.r {
		wg.Add(1)
		go func(b string, cases []RoundTripBenchTestCase) {
			defer wg.Done()
			t.Run(b, func(t *testing.T) {
				runs := 1
				if isRewrite {
					runs = *rewriteIterations
				}
				runRoundTripBenchmarkTest(t, scope, &results, cases, r.cc, runs, limiter)
			})
		}(b, cases)
	}
	wg.Wait()

	if isRewrite {
		writeExpectationsFile(t,
			mergeExpectations(
				readExpectationsFile(t),
				resultsToExpectations(t, results.toSlice()),
			))
	} else {
		checkResults(t, &results, readExpectationsFile(t))
	}
}

func checkResults(t *testing.T, results *resultSet, expectations benchmarkExpectations) {
	results.iterate(func(r benchmarkResult) {
		exp, ok := expectations.find(r.name)
		if !ok {
			t.Logf("no expectation for benchmark %s, got %d", r.name, r.result)
			return
		}
		if !exp.matches(r.result) {
			t.Errorf("fail: expected %s to perform KV lookups in [%d, %d], got %d",
				r.name, exp.min, exp.max, r.result)
		} else {
			t.Logf("success: expected %s to perform KV lookups in [%d, %d], got %d",
				r.name, exp.min, exp.max, r.result)
		}
	})
}

func mergeExpectations(existing, new benchmarkExpectations) (merged benchmarkExpectations) {
	sort.Sort(existing)
	sort.Sort(new)
	pop := func(be *benchmarkExpectations) (ret benchmarkExpectation) {
		ret = (*be)[0]
		*be = (*be)[1:]
		return ret
	}
	for len(existing) > 0 && len(new) > 0 {
		switch {
		case existing[0].name < new[0].name:
			merged = append(merged, pop(&existing))
		case existing[0].name > new[0].name:
			merged = append(merged, pop(&new))
		default:
			pop(&existing) // discard the existing value if they are equal
			merged = append(merged, pop(&new))
		}
	}
	// Only one of existing or new will be non-empty.
	merged = append(append(merged, new...), existing...)
	return merged
}

// execTestSubprocess execs the testing binary with all the same flags in order
// to run it without metamorphic testing enabled. Metamorphic testing messes
// with the benchmark results. It's particularly important to do this as we
// always run with metamorphic testing enabled in CI.
func execTestSubprocess(t *testing.T) {
	var args []string
	flag.CommandLine.Visit(func(f *flag.Flag) {
		vs := f.Value.String()
		switch f.Name {
		case "test.run":
			// Only run the current outermost test in the subprocess.
			prefix := "^" + regexp.QuoteMeta(t.Name()) + "$"
			if idx := strings.Index(vs, "/"); idx >= 0 {
				vs = prefix + vs[idx:]
			} else {
				vs = prefix
			}
		case "test.bench":
			// Omit the benchmark flags, we'll add a flag below to disable
			// benchmarks. Consider the below command. We don't want to
			// run the benchmarks again in this subprocess. We only want
			// to run exactly this one test.
			//
			//   go test --run Expectations --bench .
			//
			return
		}
		args = append(args, "--"+f.Name+"="+vs)
	})
	args = append(args, "--test.bench=^$") // disable benchmarks
	args = append(args, flag.CommandLine.Args()...)
	cmd := exec.Command(os.Args[0], args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, util.DisableMetamorphicEnvVar+"=t")
	t.Log(cmd.Args)
	if err := cmd.Run(); err != nil {
		t.FailNow()
	}
}

type resultSet struct {
	mu struct {
		syncutil.Mutex
		results []benchmarkResult
	}
}

func (s *resultSet) add(result benchmarkResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mu.results = append(s.mu.results, result)
}

func (s *resultSet) iterate(f func(res benchmarkResult)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, res := range s.mu.results {
		f(res)
	}
}

func (s *resultSet) toSlice() (res []benchmarkResult) {
	s.iterate(func(result benchmarkResult) {
		res = append(res, result)
	})
	return res
}

func resultsToExpectations(t *testing.T, results []benchmarkResult) benchmarkExpectations {
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

	// Verify there aren't any duplicates.
	for i := 1; i < len(res); i++ {
		if res[i-1].name == res[i].name {
			t.Fatalf("duplicate expectations for Name %s", res[i].name)
		}
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
	// Either the value falls within the expected range, or
	return (e.min <= roundTrips && roundTrips <= e.max) ||
		// the expectation isn't a range, so give it a leeway of one.
		e.min == e.max && (roundTrips == e.min-1 || roundTrips == e.min+1)
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
