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
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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

	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	e := s.Executor().(*Executor)
	session := NewSession(
		ctx, SessionArgs{User: security.RootUser}, e,
		&MemoryMetrics{}, nil /* conn */)
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
		cluster.MakeTestingClusterSettings(),
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
