// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/diagutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/opentracing/opentracing-go"
)

// TestDDLAnalysis runs the datadriven DDL analysis tests. The test allows us
// to approximate the number of round trips a DDL statement takes by counting
// kv operations.
//
//  - exec
//
//    Executes SQL statements against the database. Outputs no results on
//    success. In case of error, outputs the error message.
//
//  - count
//    Executes a SQL statement and counts the number of kv batch request
//    operations that are made for the statement.

func TestDDLAnalysis(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	datadriven.Walk(t, "testdata/ddl_analysis", func(t *testing.T, path string) {
		diagSrv := diagutils.NewServer()
		defer diagSrv.Close()
		var stmtToKvBatchRequests sync.Map

		beforePlan := func(sp opentracing.Span, stmt string) {
			if _, ok := stmtToKvBatchRequests.Load(stmt); ok {
				sp.Finish()
				trace := tracing.GetRecording(sp)
				stmtToKvBatchRequests.Store(stmt, countKvBatchRequestsInRecording(trace))
			}
		}

		params := base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					WithStatementTrace: beforePlan,
				},
			},
		}

		s, sqlConn, _ := serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)

		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "exec":
				_, err := sqlConn.Exec(td.Input)
				if err != nil {
					if errors.HasAssertionFailure(err) {
						td.Fatalf(t, "%+v", err)
					}
					return fmt.Sprintf("error: %v\n", err)
				}
				return ""

			case "count":
				stmtToKvBatchRequests.Store(td.Input, -1)
				_, err := sqlConn.Exec(td.Input)
				if err != nil {
					if errors.HasAssertionFailure(err) {
						td.Fatalf(t, "%+v", err)
					}
					return fmt.Sprintf("error: %v\n", err)
				}

				// TODO(richardjcai): Update datadriven to accept a callback to call
				// on error so we can return the actual trace.
				count, _ := stmtToKvBatchRequests.Load(td.Input)

				if count == -1 {
					t.Fatalf(
						"could not find number of round trips for statement: %s",
						td.Input,
					)
				}

				return fmt.Sprintf("%d", count)

			default:
				td.Fatalf(t, "unknown command %s", td.Cmd)
				return ""
			}
		})
	})
}

// count the number of KvBatchRequests inside a recording, this is done by
// counting each "txn coordinator send" operation.
func countKvBatchRequestsInRecording(r tracing.Recording) int {
	root := r[0]

	// Find the topmost "flow" span to start traversing from.
	for _, sp := range r {
		if sp.ParentSpanID == root.SpanID && sp.Operation == "flow" {
			return countKvBatchRequestsInSpan(r, sp)
		}
	}

	return countKvBatchRequestsInSpan(r, root)
}

func countKvBatchRequestsInSpan(r tracing.Recording, sp tracing.RecordedSpan) int {
	count := 0
	// Count the number of OpTxnCoordSender operations while traversing the
	// tree of spans.
	if sp.Operation == kvcoord.OpTxnCoordSender {
		count++
	}

	for _, osp := range r {
		if osp.ParentSpanID != sp.SpanID {
			continue
		}
		count += countKvBatchRequestsInSpan(r, osp)
	}

	return count
}
