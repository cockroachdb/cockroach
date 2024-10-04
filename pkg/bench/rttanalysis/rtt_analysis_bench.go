// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
)

// RoundTripBenchTestCase is a struct that holds the Name of a benchmark test
// case for ddl analysis and the statements to run for the test.
// Reset must drop any remaining objects after the current database is dropped
// so Setup and Stmt can be run again.
type RoundTripBenchTestCase struct {
	Name string
	// Setup runs before Stmt. The round-trips are not counted. It can consist of
	// multiple semicolon-separated statements, and they'll all be executed in a
	// transaction.
	Setup string
	// SetupEx is like Setup, but allows the test to separate different statements
	// in different transactions. This is commonly used to lease descriptors on
	// new tables so that the test is not bothered by the lease acquisition. The
	// lease acquisition cannot be done in the same transaction as the one
	// creating the table.
	SetupEx   []string
	Stmt      string
	Reset     string
	SkipIssue int
}

func runRoundTripBenchmark(b testingB, tests []RoundTripBenchTestCase, cc ClusterConstructor) {
	for _, tc := range tests {
		b.Run(tc.Name, func(b testingB) {
			if tc.SkipIssue != 0 {
				skip.WithIssue(b, tc.SkipIssue)
			}
			executeRoundTripTest(b, tc, cc)
		})
	}
}

// RunRoundTripBenchmark sets up a db run the RoundTripBenchTestCase test cases
// and counts how many round trips the Stmt specified by the test case performs.
// It runs each leaf subtest numRuns times. It uses the limiter to limit
// concurrency.
func runRoundTripBenchmarkTest(
	t *testing.T,
	scope *log.TestLogScope,
	results *resultSet,
	tests []RoundTripBenchTestCase,
	cc ClusterConstructor,
	numRuns int,
	limit *quotapool.IntPool,
) {
	skip.UnderMetamorphic(t, "changes the RTTs")
	var wg sync.WaitGroup
	for _, tc := range tests {
		wg.Add(1)
		go func(tc RoundTripBenchTestCase) {
			defer wg.Done()
			t.Run(tc.Name, func(t *testing.T) {
				runRoundTripBenchmarkTestCase(t, scope, results, tc, cc, numRuns, limit)
			})
		}(tc)
	}
	wg.Wait()
}

func runRoundTripBenchmarkTestCase(
	t *testing.T,
	scope *log.TestLogScope,
	results *resultSet,
	tc RoundTripBenchTestCase,
	cc ClusterConstructor,
	numRuns int,
	limit *quotapool.IntPool,
) {
	if tc.SkipIssue != 0 {
		skip.WithIssue(t, tc.SkipIssue)
	}
	var wg sync.WaitGroup
	for i := 0; i < numRuns; i++ {
		alloc, err := limit.Acquire(context.Background(), 1)
		require.NoError(t, err)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer alloc.Release()
			executeRoundTripTest(tShim{
				T: t, results: results, scope: scope,
			}, tc, cc)
		}()
	}
	wg.Wait()
}

// executeRoundTripTest executes a RoundTripBenchCase on with the provided SQL runner
func executeRoundTripTest(b testingB, tc RoundTripBenchTestCase, cc ClusterConstructor) {
	getDir, cleanup := b.logScope()
	defer cleanup()

	cluster := cc(b)
	defer cluster.close()

	sql := sqlutils.MakeSQLRunner(cluster.conn())

	expData := readExpectationsFile(b)

	exp, haveExp := expData.find(strings.TrimPrefix(b.Name(), "Benchmark"))

	roundTrips := 0
	b.ResetTimer()
	b.StopTimer()
	var r tracingpb.Recording

	// The statement trace records individual statements, but we may want to
	// execute multiple SQL statements. Note that multi-statement traces won't
	// count round trips correctly if there are duplicate statements.
	statements, err := parser.Parse(tc.Stmt)
	if err != nil {
		require.NoError(b, err)
	}

	// Do an extra iteration and don't record it in order to deal with effects of
	// running it the first time.
	for i := 0; i < b.N()+1; i++ {
		sql.Exec(b, "CREATE DATABASE bench")
		// Make sure the database descriptor is leased, so that tests don't count
		// the leasing.
		sql.Exec(b, "USE bench")
		// Also force a lease on the "public" schema too.
		sql.Exec(b, "CREATE TABLE bench.public.__dummy__()")
		sql.Exec(b, "SELECT 1 FROM bench.public.__dummy__")
		sql.Exec(b, "DROP TABLE bench.public.__dummy__")

		sql.Exec(b, tc.Setup)
		for _, s := range tc.SetupEx {
			sql.Exec(b, s)
		}
		for _, statement := range statements {
			cluster.clearStatementTrace(statement.SQL)
		}

		b.StartTimer()
		sql.Exec(b, tc.Stmt)
		b.StopTimer()
		var ok bool

		total := 0
		for _, statement := range statements {
			r, ok = cluster.getStatementTrace(statement.SQL)
			if !ok {
				b.Fatalf(
					"could not find number of round trips for statement: %s",
					statement.SQL,
				)
			}

			// If there's a retry error then we're just going to throw away this
			// run.
			rt, hasRetry := countKvBatchRequestsInRecording(r)
			if hasRetry {
				i--
				ok = false
				break
			} else if i > 0 { // skip the initial iteration
				total += rt
			}
		}
		if ok {
			roundTrips += total
		}

		sql.Exec(b, "DROP DATABASE bench;")
		sql.Exec(b, tc.Reset)
	}

	res := float64(roundTrips) / float64(b.N())

	reportf := b.Errorf
	if b.isBenchmark() {
		reportf = b.Logf
	}
	if haveExp && !exp.matches(int(res)) && !*rewriteFlag {
		reportf(`%s: got %v, expected %v`, b.Name(), res, exp)
		dir := getDir()
		jaegerJSON, err := r.ToJaegerJSON(tc.Stmt, "", "n0")
		require.NoError(b, err)
		path := filepath.Join(dir, strings.Replace(b.Name(), "/", "_", -1)) + ".jaeger.json"
		require.NoError(b, os.WriteFile(path, []byte(jaegerJSON), 0666))
		reportf("wrote jaeger trace to %s", path)
	}
	b.ReportMetric(res, roundTripsMetric)
}

const roundTripsMetric = "roundtrips"

// count the number of KvBatchRequests inside a recording, this is done by
// counting each "txn coordinator send" operation.
func countKvBatchRequestsInRecording(r tracingpb.Recording) (sends int, hasRetry bool) {
	root := r[0]
	return countKvBatchRequestsInSpan(r, root)
}

func countKvBatchRequestsInSpan(r tracingpb.Recording, sp tracingpb.RecordedSpan) (int, bool) {
	count := 0
	// Count the number of OpTxnCoordSender operations while traversing the
	// tree of spans.
	if sp.Operation == kvcoord.OpTxnCoordSender {
		count++
	}
	if logsContainRetry(sp.Logs) {
		return 0, true
	}

	for _, osp := range r {
		if osp.ParentSpanID != sp.SpanID {
			continue
		}

		subCount, hasRetry := countKvBatchRequestsInSpan(r, osp)
		if hasRetry {
			return 0, true
		}
		count += subCount
	}

	return count, false
}

func logsContainRetry(logs []tracingpb.LogRecord) bool {
	for _, l := range logs {
		if strings.Contains(l.String(), "TransactionRetryWithProtoRefreshError") {
			return true
		}
	}
	return false
}
