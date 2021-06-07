// Copyright 2020 The Cockroach Authors.
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
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
)

// RoundTripBenchTestCase is a struct that holds the Name of a benchmark test
// case for ddl analysis and the statements to run for the test.
// Reset must drop any remaining objects after the current database is dropped
// so Setup and Stmt can be run again.
type RoundTripBenchTestCase struct {
	Name  string
	Setup string
	Stmt  string
	Reset string
}

// RunRoundTripBenchmark sets up a db run the RoundTripBenchTestCase test cases
// and counts how many round trips the Stmt specified by the test case performs.
func RunRoundTripBenchmark(b *testing.B, tests []RoundTripBenchTestCase) {
	skip.UnderMetamorphic(b, "changes the RTTs")

	for _, tc := range tests {
		b.Run(tc.Name, func(b *testing.B) {
			defer log.Scope(b).Close(b)
			var stmtToKvBatchRequests sync.Map

			beforePlan := func(trace tracing.Recording, stmt string) {
				if _, ok := stmtToKvBatchRequests.Load(stmt); ok {
					stmtToKvBatchRequests.Store(stmt, trace)
				}
			}

			params := base.TestServerArgs{
				UseDatabase: "bench",
				Knobs: base.TestingKnobs{
					SQLExecutor: &sql.ExecutorTestingKnobs{
						WithStatementTrace: beforePlan,
					},
				},
			}

			s, db, _ := serverutils.StartServer(
				b, params,
			)
			sql := sqlutils.MakeSQLRunner(db)

			defer s.Stopper().Stop(context.Background())

			ExecuteRoundTripTest(b, sql, &stmtToKvBatchRequests, tc)
		})
	}
}

// ExecuteRoundTripTest executes a RoundTripBenchCase on with the provided SQL runner
func ExecuteRoundTripTest(
	b *testing.B, sql *sqlutils.SQLRunner, stmtToKvBatchRequests *sync.Map, tc RoundTripBenchTestCase,
) {
	expData := readExpectationsFile(b)

	defer log.Scope(b).Close(b)

	exp, haveExp := expData.find(strings.TrimPrefix(b.Name(), "Benchmark"))

	roundTrips := 0
	b.ResetTimer()
	b.StopTimer()
	var r tracing.Recording
	for i := 0; i < b.N; i++ {
		sql.Exec(b, "CREATE DATABASE bench;")
		sql.Exec(b, tc.Setup)
		stmtToKvBatchRequests.Store(tc.Stmt, nil)

		b.StartTimer()
		sql.Exec(b, tc.Stmt)
		b.StopTimer()

		out, _ := stmtToKvBatchRequests.Load(tc.Stmt)
		var ok bool
		if r, ok = out.(tracing.Recording); !ok {
			b.Fatalf(
				"could not find number of round trips for statement: %s",
				tc.Stmt,
			)
		}

		// If there's a retry error then we're just going to throw away this
		// run.
		rt, hasRetry := countKvBatchRequestsInRecording(r)
		if hasRetry {
			i--
		} else {
			roundTrips += rt
		}

		sql.Exec(b, "DROP DATABASE bench;")
		sql.Exec(b, tc.Reset)
	}

	res := float64(roundTrips) / float64(b.N)
	if haveExp && !exp.matches(int(res)) && *rewriteFlag == "" {
		b.Fatalf(`got %v, expected %v. trace:
%v
(above trace from test %s. got %v, expected %v)
`, res, exp, r, b.Name(), res, exp)
	}
	b.ReportMetric(res, "roundtrips")
}

// count the number of KvBatchRequests inside a recording, this is done by
// counting each "txn coordinator send" operation.
func countKvBatchRequestsInRecording(r tracing.Recording) (sends int, hasRetry bool) {
	root := r[0]
	return countKvBatchRequestsInSpan(r, root)
}

func countKvBatchRequestsInSpan(r tracing.Recording, sp tracingpb.RecordedSpan) (int, bool) {
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
