// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"
	"database/sql/driver"
	"math"
	"net/url"
	"strings"
	"testing"

	"github.com/lib/pq"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func withExecutor(test func(e *Executor, s *Session, evalCtx *tree.EvalContext), t *testing.T) {
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	ac := log.AmbientContext{Tracer: tracing.NewTracer()}
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()

	evalCtx := tree.NewTestingEvalContext()
	defer evalCtx.Stop(context.Background())

	e := s.Executor().(*Executor)
	session := NewSession(
		ctx, SessionArgs{User: security.RootUser}, e,
		nil /* remote */, &MemoryMetrics{}, nil /* conn */)
	session.StartUnlimitedMonitor()
	defer session.Finish(e)

	test(e, session, evalCtx)
}

func TestBufferedWriterBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	withExecutor(func(e *Executor, s *Session, evalCtx *tree.EvalContext) {
		query := "SELECT 1; SELECT * FROM generate_series(1,100)"
		res, err := e.ExecuteStatementsBuffered(s, query, nil, 2)
		if err != nil {
			t.Fatal("expected no error got", err)
		}
		defer res.Close(s.Ctx())
		if res.Empty {
			t.Fatal("expected non-empty results")
		}

		result := res.ResultList[0]
		if result.Err != nil {
			t.Fatal("expected no error got", err)
		}
		if result.PGTag != "SELECT" {
			t.Fatal("expected SELECT, got ", result.PGTag)
		}
		if result.Type != tree.Rows {
			t.Fatal("expected result type tree.Rows, got", result.Type)
		}
		if result.RowsAffected != 0 {
			t.Fatal("expected 0 rows affected, got", result.RowsAffected)
		}
		if !result.Columns.TypesEqual(sqlbase.ResultColumns{sqlbase.ResultColumn{Typ: types.Int}}) {
			t.Fatal("expected 1 column with int type, got", result.Columns)
		}

		result = res.ResultList[1]
		if result.Err != nil {
			t.Fatal("expected no error got", err)
		}
		if result.PGTag != "SELECT" {
			t.Fatal("expected SELECT, got ", result.PGTag)
		}
		if result.Type != tree.Rows {
			t.Fatal("expected result type tree.Rows, got", result.Type)
		}
		if result.RowsAffected != 0 {
			t.Fatal("expected 0 rows affected, got", result.RowsAffected)
		}
		if !result.Columns.TypesEqual(sqlbase.ResultColumns{sqlbase.ResultColumn{Typ: types.Int}}) {
			t.Fatal("expected 1 column with decimal type, got", result.Columns)
		}
		for i := 1; i < result.Rows.Len(); i++ {
			if result.Rows.At(i)[0].Compare(evalCtx, result.Rows.At(i - 1)[0]) < 0 {
				t.Fatal("expected monotonically increasing")
			}
		}
	}, t)
}

func TestBufferedWriterError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	withExecutor(func(e *Executor, s *Session, evalCtx *tree.EvalContext) {
		query := "SELECT 1; SELECT 1/(100-x) FROM generate_series(1,100) AS t(x)"
		res, err := e.ExecuteStatementsBuffered(s, query, nil, 2)
		if err == nil {
			res.Close(s.Ctx())
			t.Fatal("expected error")
		}
	}, t)
}

func TestBufferedWriterIncrementAffected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	withExecutor(func(e *Executor, s *Session, evalCtx *tree.EvalContext) {
		res, err := e.ExecuteStatementsBuffered(s, "CREATE DATABASE test; CREATE TABLE test.public.t (i INT)", nil, 2)
		if err != nil {
			t.Fatal("expected no error got", err)
		}
		res.Close(s.Ctx())
		query := "INSERT INTO test.public.t VALUES (1), (2), (3)"
		res, err = e.ExecuteStatementsBuffered(s, query, nil, 1)
		if err != nil {
			t.Fatal("expected no error got", err)
		}
		defer res.Close(s.Ctx())
		if res.Empty {
			t.Fatal("expected non-empty results")
		}

		result := res.ResultList[0]
		if result.Err != nil {
			t.Fatal("expected no err, got", result.Err)
		}
		if result.PGTag != "INSERT" {
			t.Fatal("expected INSERT, got ", result.PGTag)
		}
		if result.Type != tree.RowsAffected {
			t.Fatal("expected result type tree.Rows, got", result.Type)
		}
		if result.RowsAffected != 3 {
			t.Fatal("expected 3 rows affected, got", result.RowsAffected)
		}
	}, t)
}

func TestBufferedWriterRetries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	withExecutor(func(e *Executor, s *Session, evalCtx *tree.EvalContext) {
		query := "SELECT 1; SELECT CRDB_INTERNAL.FORCE_RETRY('1s':::INTERVAL)"
		res, err := e.ExecuteStatementsBuffered(s, query, nil, 2)
		if err != nil {
			t.Fatal("expected no error got", err)
		}
		defer res.Close(s.Ctx())
		if res.Empty {
			t.Fatal("expected non-empty results")
		}
	}, t)
}

func TestBufferedWriterReset(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.TODO()

	memMon := mon.MakeMonitor(
		"test",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment: use default block size */
		math.MaxInt64, /* noteworthy */
	)
	memMon.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(math.MaxInt64))
	defer memMon.Stop(ctx)
	acc := memMon.MakeBoundAccount()

	writer := newBufferedWriter(acc)
	gw := writer.NewResultsGroup().(*bufferedWriter)
	sw := gw.NewStatementResult()
	sw.BeginResult((*tree.Select)(nil))
	sw.SetColumns(sqlbase.ResultColumns{{Name: "test", Typ: types.String}})
	if err := sw.AddRow(ctx, tree.Datums{tree.DNull}); err != nil {
		t.Fatal(err)
	}
	if err := sw.CloseResult(); err != nil {
		t.Fatal(err)
	}
	if numRes := len(gw.currentGroupResults); numRes != 1 {
		t.Fatalf("expected 1 result, got %d", numRes)
	}
	gw.Reset(ctx)
	if numRes := len(gw.currentGroupResults); numRes != 0 {
		t.Fatalf("expected no results after reset, got %d", numRes)
	}
}

// Test that, if a communication error is encountered during streaming of
// results, the statement will return a StreamingWireFailure error. That's
// important because pgwire recognizes that error.
func TestStreamingWireFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// This test uses libpq because it needs to control when a network connection
	// is closed. The Go sql package doesn't easily let you do that.

	// We're going to run a query and kill the network connection while it's
	// running. The query needs to have a large enough set of results such that
	// a) they overflow the results buffer and cause them to actually be sent over
	// the network as they are produced, giving the server an opportunity to find
	// out that the network connection dropped.
	// b) beyond the point above, it takes a little while for
	// the server to actually find out that the network has been closed after the
	// client has closed it. That's why this query is much larger than would be
	// needed just for a).
	//
	// TODO(andrei): It's unfortunate that the server finds out about the status
	// of network connection every now and then, when it wants to flush some
	// results - for one it forces this test in using this big hammer of
	// generating tons and tons of results. What we'd like is for the server to
	// figure out the status of the connection concurrently with the query
	// execution - then this test would be able to wait for the server to figure
	// out that the connection is toast and then unblock the query.
	const query = "SELECT * FROM generate_series(1, 200000)"

	var conn driver.Conn
	serverErrChan := make(chan error)

	var ts serverutils.TestServerInterface
	ts, _, _ = serverutils.StartServer(t,
		base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLExecutor: &ExecutorTestingKnobs{
					BeforeExecute: func(ctx context.Context, stmt string, _ /* isParallel */ bool) {
						if strings.Contains(stmt, "generate_series") {
							if err := conn.Close(); err != nil {
								t.Error(err)
							}
						}
					},
					// We use an AfterExecute filter to get access to the server-side
					// execution error. The client will be disconnected by the time this
					// runs.
					AfterExecute: func(ctx context.Context, stmt string, resultWriter StatementResult, err error) {
						if strings.Contains(stmt, "generate_series") {
							serverErrChan <- err
						}
					},
				},
			},
		},
	)
	ctx := context.TODO()
	defer ts.Stopper().Stop(ctx)

	pgURL, cleanup := sqlutils.PGUrl(
		t, ts.ServingAddr(), "StartServer", url.User(security.RootUser))
	defer cleanup()
	var err error
	conn, err = pq.Open(pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	ex := conn.(driver.ExecerContext)

	// The BeforeExecute filter will kill the connection (form the client side) in
	// the middle of the query. Therefor, the client expects a ErrBadConn, and the
	// server expects a WireFailureError.
	if _, err := ex.ExecContext(ctx, query, nil); err != driver.ErrBadConn {
		t.Fatalf("expected ErrBadConn, got: %v", err)
	}
	serverErr := <-serverErrChan
	if _, ok := serverErr.(WireFailureError); !ok {
		t.Fatalf("expected WireFailureError, got %v ", err)
	}
}
