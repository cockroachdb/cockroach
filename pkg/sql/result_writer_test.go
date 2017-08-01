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

package sql_test

import (
	"database/sql/driver"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/lib/pq"
	"golang.org/x/net/context"
	"net/url"
	"strings"
	"testing"
)

func withExecutor(
	test func(e *sql.Executor, s *sql.Session, evalCtx *parser.EvalContext), t *testing.T,
) {
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*server.TestServer)

	ac := log.AmbientContext{Tracer: tracing.NewTracer()}
	ctx, span := ac.AnnotateCtxWithSpan(context.Background(), "test")
	defer span.Finish()

	evalCtx := parser.NewTestingEvalContext()
	defer evalCtx.Stop(context.Background())

	e := ts.Executor().(*sql.Executor)
	session := sql.NewSession(
		ctx, sql.SessionArgs{User: security.RootUser}, e, nil, &sql.MemoryMetrics{})
	session.StartUnlimitedMonitor()
	defer session.Finish(e)

	test(e, session, evalCtx)
}

func TestBufferedWriterBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	withExecutor(func(e *sql.Executor, s *sql.Session, evalCtx *parser.EvalContext) {
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
		if result.Type != parser.Rows {
			t.Fatal("expected result type parser.Rows, got", result.Type)
		}
		if result.RowsAffected != 0 {
			t.Fatal("expected 0 rows affected, got", result.RowsAffected)
		}
		if !result.Columns.TypesEqual(sqlbase.ResultColumns{sqlbase.ResultColumn{Typ: parser.TypeInt}}) {
			t.Fatal("expected 1 column with int type, got", result.Columns)
		}

		result = res.ResultList[1]
		if result.Err != nil {
			t.Fatal("expected no error got", err)
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
		if !result.Columns.TypesEqual(sqlbase.ResultColumns{sqlbase.ResultColumn{Typ: parser.TypeInt}}) {
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
	withExecutor(func(e *sql.Executor, s *sql.Session, evalCtx *parser.EvalContext) {
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
	withExecutor(func(e *sql.Executor, s *sql.Session, evalCtx *parser.EvalContext) {
		res, err := e.ExecuteStatementsBuffered(s, "CREATE DATABASE test; CREATE TABLE test.t (i INT)", nil, 2)
		if err != nil {
			t.Fatal("expected no error got", err)
		}
		res.Close(s.Ctx())
		query := "INSERT INTO test.t VALUES (1), (2), (3)"
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
		if result.Type != parser.RowsAffected {
			t.Fatal("expected result type parser.Rows, got", result.Type)
		}
		if result.RowsAffected != 3 {
			t.Fatal("expected 3 rows affected, got", result.RowsAffected)
		}
	}, t)
}

func TestBufferedWriterRetries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	withExecutor(func(e *sql.Executor, s *sql.Session, evalCtx *parser.EvalContext) {
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

func TestStreamingWireFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// query's result set is large enough to cause a flush midway through execution.
	const query = "SELECT * FROM generate_series(1,20000)"

	var conn driver.Conn
	errChan := make(chan error, 1)

	tc := serverutils.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					SQLExecutor: &sql.ExecutorTestingKnobs{
						BeforeExecute: func(ctx context.Context, stmt string, isDistributed bool) {
							if strings.Contains(stmt, "generate_series") {
								if err := conn.Close(); err != nil {
									t.Fatal("unexpected error", err)
								}
							}
						},
						AfterExecute: func(ctx context.Context, stmt string, resultWriter sql.StatementResultWriter, err error) {
							if strings.Contains(stmt, "generate_series") {
								errChan <- err
								close(errChan)
							}
						},
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.TODO())

	pgURL, cleanupGoDB := sqlutils.PGUrl(
		t, tc.Server(0).ServingAddr(), "StartServer", url.User(security.RootUser))
	defer cleanupGoDB()

	conn, err := pq.Open(pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	prepare, err := conn.Prepare(query)
	if err != nil {
		t.Fatal(err)
	}
	_, err = prepare.Exec(nil)
	if err == nil {
		t.Fatal("expected error got none")
	}
	err = <-errChan
	_, ok := err.(sql.WireFailureError)
	if !ok {
		t.Fatal("expected wirefailure error got", err)
	}
}
