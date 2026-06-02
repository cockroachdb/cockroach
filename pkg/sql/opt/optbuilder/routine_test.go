// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// routineBuilderHarness bundles the state needed to drive a
// sqlRoutineBodyBuilder directly in a unit test, without going through
// buildRoutine.
type routineBuilderHarness struct {
	ctx     context.Context
	semaCtx tree.SemaContext
	evalCtx eval.Context
	catalog cat.Catalog
}

func newRoutineBuilderHarness() *routineBuilderHarness {
	ctx := context.Background()
	return &routineBuilderHarness{
		ctx:     ctx,
		semaCtx: tree.MakeSemaContext(nil /* resolver */),
		evalCtx: eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings()),
		catalog: testcat.New(),
	}
}

// freshFactory returns a norm.Factory backed by a new, empty memo. Build and
// BuildStmt each require their own factory; a fresh factory also makes
// column-ID allocation deterministic across calls.
func (h *routineBuilderHarness) freshFactory() *norm.Factory {
	var f norm.Factory
	f.Init(h.ctx, &h.evalCtx, h.catalog)
	return &f
}

func (h *routineBuilderHarness) parseBody(t *testing.T, body string) []tree.Statement {
	stmts, err := parser.Parse(body)
	require.NoError(t, err)
	asts := make([]tree.Statement, len(stmts))
	for i := range stmts {
		asts[i] = stmts[i].AST
	}
	return asts
}

func (h *routineBuilderHarness) newBodyBuilder(
	t *testing.T,
	body string,
	rTyp *types.T,
	isSetReturning bool,
	paramNames []tree.Name,
	paramTypes []*types.T,
) memo.RoutineBodyBuilder {
	return newSQLRoutineBodyBuilder(sqlRoutineBodyBuilder{
		stmtASTs:       h.parseBody(t, body),
		paramTypes:     paramTypes,
		paramNames:     paramNames,
		rTyp:           rTyp,
		isSetReturning: isSetReturning,
		routineType:    tree.UDFRoutine,
	})
}

// TestSQLRoutineBodyBuilderNumStmts verifies that NumStmts reflects the number
// of body statements, including the synthetic VALUES (NULL) statement appended
// for VOID-returning routines.
func TestSQLRoutineBodyBuilderNumStmts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h := newRoutineBuilderHarness()
	testCases := []struct {
		name             string
		body             string
		rTyp             *types.T
		expectedNumStmts int
	}{
		{name: "single statement", body: "SELECT 1", rTyp: types.Int, expectedNumStmts: 1},
		{name: "multiple statements", body: "SELECT 1; SELECT 2; SELECT 3", rTyp: types.Int, expectedNumStmts: 3},
		{name: "void appends synthetic statement", body: "SELECT 1", rTyp: types.Void, expectedNumStmts: 2},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rb := h.newBodyBuilder(
				t, tc.body, tc.rTyp, false /* isSetReturning */, nil /* paramNames */, nil, /* paramTypes */
			)
			require.Equal(t, tc.expectedNumStmts, rb.NumStmts())
		})
	}
}

// TestSQLRoutineBodyBuilderBuildStmtMatchesBuild verifies that building the body
// one statement at a time via BuildStmt produces the same number of statements,
// the same per-statement operators, and the same parameter columns as the
// all-at-once Build.
func TestSQLRoutineBodyBuilderBuildStmtMatchesBuild(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h := newRoutineBuilderHarness()
	rb := h.newBodyBuilder(
		t, "SELECT 1; SELECT 2; SELECT 3", types.Int, false /* isSetReturning */, nil, nil,
	)

	body, bodyProps, params, err := rb.Build(
		h.ctx, &h.semaCtx, &h.evalCtx, h.catalog, h.freshFactory(),
	)
	require.NoError(t, err)
	require.Len(t, body, rb.NumStmts())
	require.Len(t, bodyProps, rb.NumStmts())

	for i := 0; i < rb.NumStmts(); i++ {
		stmt, props, perStmtParams, err := rb.BuildStmt(
			h.ctx, &h.semaCtx, &h.evalCtx, h.catalog, h.freshFactory(), i,
		)
		require.NoErrorf(t, err, "stmt %d", i)
		require.NotNil(t, stmt)
		require.NotNil(t, props)
		require.Equalf(t, body[i].Op(), stmt.Op(), "operator mismatch at stmt %d", i)
		require.Equalf(t, params, perStmtParams, "params mismatch at stmt %d", i)
	}
}

// TestSQLRoutineBodyBuilderStableParamCols verifies that BuildStmt returns the
// same parameter column IDs regardless of which statement index is built. The
// interleaved build/execute path relies on this so that argument-to-parameter
// mapping is consistent across statements.
func TestSQLRoutineBodyBuilderStableParamCols(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h := newRoutineBuilderHarness()
	rb := h.newBodyBuilder(
		t, "VALUES (1); SELECT 2; SELECT 3 UNION SELECT 4", types.Int, false, /* isSetReturning */
		[]tree.Name{"a", "b"}, []*types.T{types.Int, types.String},
	)

	_, _, params0, err := rb.BuildStmt(h.ctx, &h.semaCtx, &h.evalCtx, h.catalog, h.freshFactory(), 0)
	require.NoError(t, err)
	_, _, params1, err := rb.BuildStmt(h.ctx, &h.semaCtx, &h.evalCtx, h.catalog, h.freshFactory(), 1)
	require.NoError(t, err)
	_, _, params2, err := rb.BuildStmt(h.ctx, &h.semaCtx, &h.evalCtx, h.catalog, h.freshFactory(), 2)
	require.NoError(t, err)
	require.Len(t, params0, 2)
	require.Equal(t, params0, params1)
	require.Equal(t, params0, params2)
}

// TestSQLRoutineBodyBuilderLastStmtFinalization verifies that only the final
// body statement is finalized as the routine's output. For a non-set-returning
// routine, finalization applies an implicit LIMIT 1, which caps the statement's
// cardinality.
func TestSQLRoutineBodyBuilderLastStmtFinalization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	h := newRoutineBuilderHarness()
	rb := h.newBodyBuilder(
		t, "VALUES (1), (2), (3); VALUES (4), (5), (6)", types.Int, false /* isSetReturning */, nil, nil,
	)

	last, _, _, err := rb.BuildStmt(h.ctx, &h.semaCtx, &h.evalCtx, h.catalog, h.freshFactory(), 1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), last.Relational().Cardinality.Max)

	first, _, _, err := rb.BuildStmt(h.ctx, &h.semaCtx, &h.evalCtx, h.catalog, h.freshFactory(), 0)
	require.NoError(t, err)
	require.Equal(t, uint32(3), first.Relational().Cardinality.Max)
}
