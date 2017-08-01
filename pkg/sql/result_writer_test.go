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
//
// Author: Tristan Ohlson (tsohlson@gmail.com)

package sql_test

import (
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"testing"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"golang.org/x/net/context"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

func withExecutor(
	test func(e *sql.Executor, s *sql.Session, evalCtx *parser.EvalContext), t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*server.TestServer)

	ac := log.AmbientContext{Tracer: tracing.NewTracer()}
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()

	evalCtx := parser.NewTestingEvalContext()
	defer evalCtx.Stop(context.Background())

	e := ts.GetExecutor()
	session := sql.NewSession(
		ctx, sql.SessionArgs{User: security.RootUser}, e, nil, &sql.MemoryMetrics{})
	session.StartUnlimitedMonitor()
	defer session.Finish(e)

	test(e, session, evalCtx)
}

func TestBufferedWriterError(t *testing.T) {
	withExecutor(func(e *sql.Executor, s *sql.Session, evalCtx *parser.EvalContext) {
		query := "SELECT 1/(100-x) FROM generate_series(1,100) AS t(x)"
		res := e.ExecuteStatementsBuffered(s, query, nil)
		defer res.Close(s.Ctx())
		if res.Empty {
			t.Fatal("expected non-empty results")
		}
		if len(res.ResultList) != 1 {
			t.Fatal("expected 1 result, got:", len(res.ResultList))
		}

		result := res.ResultList[0]
		if result.Err == nil {
			t.Fatal("expected pq: division by zero")
		}
		if result.PGTag != "SELECT" {
			t.Fatal("expected SELECT, got ", result.PGTag)
		}
		if result.Type != parser.Rows {
			t.Fatal("expected result type parser.Rows, got", result.Type)
		}
		if result.RowsAffected != 0 {
			t.Fatal("expected 0 rows affected, got", result.RowsAffected)
		}
		if !result.Columns.TypesEqual(sqlbase.ResultColumns{sqlbase.ResultColumn{Typ: parser.TypeDecimal}}) {
			t.Fatal("expected 1 column with decimal type, got", result.Columns)
		}
		for i := 1; i < result.Rows.Len(); i++ {
			if result.Rows.At(i)[0].Compare(evalCtx, result.Rows.At(i - 1)[0]) < 0 {
				t.Fatal("expected monotonic increasing")
			}
		}
	}, t)
}

func TestBufferedWriterIncrementAffected(t *testing.T) {
	withExecutor(func(e *sql.Executor, s *sql.Session, evalCtx *parser.EvalContext) {
		e.ExecuteStatementsBuffered(s, "CREATE DATABASE test; CREATE TABLE test.t (i INT)", nil)
		query := "INSERT INTO test.t VALUES (1), (2), (3)"
		res := e.ExecuteStatementsBuffered(s, query, nil)
		defer res.Close(s.Ctx())
		if res.Empty {
			t.Fatal("expected non-empty results")
		}
		if len(res.ResultList) != 1 {
			t.Fatal("expected 1 result, got:", len(res.ResultList))
		}

		result := res.ResultList[0]
		if result.Err != nil {
			t.Fatal("expected no err, got", result.Err)
		}
		if result.PGTag != "INSERT" {
			t.Fatal("expected INSERT, got ", result.PGTag)
		}
		if result.Type != parser.RowsAffected {
			t.Fatal("expected result type parser.Rows, got", result.Type)
		}
		if result.RowsAffected != 3 {
			t.Fatal("expected 3 rows affected, got", result.RowsAffected)
		}
	}, t)
}

func TestBufferedWriterRetries(t *testing.T) {
	withExecutor(func(e *sql.Executor, s *sql.Session, evalCtx *parser.EvalContext) {
		query := "SELECT 1; SELECT CRDB_INTERNAL.FORCE_RETRY('1s':::INTERVAL)"
		res := e.ExecuteStatementsBuffered(s, query, nil)
		defer res.Close(s.Ctx())
		if res.Empty {
			t.Fatal("expected non-empty results")
		}
		if len(res.ResultList) != 2 {
			t.Fatal("expected 2 results, got:", len(res.ResultList))
		}

		selectResult := res.ResultList[0]
		if selectResult.Err != nil {
			t.Fatal("expected no err, got", selectResult.Err)
		}

		retryResult := res.ResultList[1]
		if retryResult.Err != nil {
			t.Fatal("expected no err, got", retryResult.Err)
		}
	}, t)
}
