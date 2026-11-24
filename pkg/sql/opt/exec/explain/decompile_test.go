// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package explain

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/stretchr/testify/require"
)

// TestDecompileCoversAllOps verifies that every execOperator is handled, by one
// of node.wrapperOp, node.isTransparent, or an explicit node.decompile case.
// A new op added to factory.opt that nobody remembered to wire up here
// would otherwise hit decompile's default branch, which returns nil ops;
// emit/emitInProduction turn that into an assertion failure in production,
// discarding the pinned plan.
//
// When a case runs to completion, the returned ops slice is also checked:
// nil means the op is unhandled (a wiring bug); a non-nil slice must be
// non-empty, since an empty slice would silently drop the operator constraint.
func TestDecompileCoversAllOps(t *testing.T) {
	for op := execOperator(1); op < numOperators; op++ {
		t.Run(fmt.Sprintf("op%d", op), func(t *testing.T) {
			n := &Node{op: op}
			// These short-circuits run before decompile in emit/emitInProduction,
			// so the op is covered without reaching a decompile case.
			if _, ok := n.wrapperOp(); ok {
				return
			}
			if n.isTransparent() {
				return
			}
			var ops []opt.Operator
			panicked := false
			func() {
				defer func() {
					// A handled case with a nil n.args panics on a type assertion
					// when it tries to read its arguments, which still counts as
					// covered.
					if recover() != nil {
						panicked = true
					}
				}()
				ops, _ = n.decompile()
			}()
			if panicked {
				return
			}
			if ops == nil {
				t.Fatalf("execOperator %d has no decompile case; add one "+
					"(use opt.UnknownOp if no constraint applies)", op)
			}
			if len(ops) == 0 {
				t.Fatalf("case for op %d returned empty ops slice; "+
					"include at least one op", op)
			}
		})
	}
}

func TestJoinTypeToOp(t *testing.T) {
	tests := []struct {
		joinType   descpb.JoinType
		expectedOp opt.Operator
	}{
		{joinType: descpb.InnerJoin, expectedOp: opt.InnerJoinOp},
		{joinType: descpb.LeftOuterJoin, expectedOp: opt.LeftJoinOp},
		{joinType: descpb.RightOuterJoin, expectedOp: opt.RightJoinOp},
		{joinType: descpb.FullOuterJoin, expectedOp: opt.FullJoinOp},
		{joinType: descpb.LeftSemiJoin, expectedOp: opt.SemiJoinOp},
		{joinType: descpb.LeftAntiJoin, expectedOp: opt.AntiJoinOp},
		// RIGHT_SEMI and RIGHT_ANTI are produced by the execbuilder when it
		// swaps hash join inputs based on row count estimates. They map to the
		// same optimizer operators as their LEFT counterparts.
		{joinType: descpb.RightSemiJoin, expectedOp: opt.SemiJoinOp},
		{joinType: descpb.RightAntiJoin, expectedOp: opt.AntiJoinOp},
		// Set-op join types (used by the lower-level execution layers for
		// INTERSECT/EXCEPT) never appear in plan-gist hash joins, so they fall
		// through to UnknownOp.
		{joinType: descpb.IntersectAllJoin, expectedOp: opt.UnknownOp},
		{joinType: descpb.ExceptAllJoin, expectedOp: opt.UnknownOp},
	}

	for _, tc := range tests {
		t.Run(tc.joinType.String(), func(t *testing.T) {
			require.Equal(t, tc.expectedOp, joinTypeToOp(tc.joinType))
		})
	}
}

// TestDecompileChildrenCommutesJoins verifies that hash and merge joins
// whose joinType is RightSemi or RightAnti have their inputs swapped back to
// optimizer order during decompile. The execbuilder commutes these joins to
// keep the smaller side on the right at execution time (see buildHashJoin
// and buildMergeJoin in execbuilder), but the optimizer plan we're trying
// to match has the original left/right order with SemiJoinOp/AntiJoinOp.
func TestDecompileChildrenCommutesJoins(t *testing.T) {
	left := &Node{op: scanOp}
	right := &Node{op: scanOp}

	tests := []struct {
		name       string
		node       *Node
		expectSwap bool
	}{
		{
			name: "hash inner join not swapped",
			node: &Node{
				op:       hashJoinOp,
				args:     &hashJoinArgs{JoinType: descpb.InnerJoin},
				children: []*Node{left, right},
			},
		},
		{
			name: "hash left semi join not swapped",
			node: &Node{
				op:       hashJoinOp,
				args:     &hashJoinArgs{JoinType: descpb.LeftSemiJoin},
				children: []*Node{left, right},
			},
		},
		{
			name: "hash right semi join swapped",
			node: &Node{
				op:       hashJoinOp,
				args:     &hashJoinArgs{JoinType: descpb.RightSemiJoin},
				children: []*Node{left, right},
			},
			expectSwap: true,
		},
		{
			name: "hash right anti join swapped",
			node: &Node{
				op:       hashJoinOp,
				args:     &hashJoinArgs{JoinType: descpb.RightAntiJoin},
				children: []*Node{left, right},
			},
			expectSwap: true,
		},
		{
			name: "merge inner join not swapped",
			node: &Node{
				op:       mergeJoinOp,
				args:     &mergeJoinArgs{JoinType: descpb.InnerJoin},
				children: []*Node{left, right},
			},
		},
		{
			name: "merge right semi join swapped",
			node: &Node{
				op:       mergeJoinOp,
				args:     &mergeJoinArgs{JoinType: descpb.RightSemiJoin},
				children: []*Node{left, right},
			},
			expectSwap: true,
		},
		{
			name: "merge right anti join swapped",
			node: &Node{
				op:       mergeJoinOp,
				args:     &mergeJoinArgs{JoinType: descpb.RightAntiJoin},
				children: []*Node{left, right},
			},
			expectSwap: true,
		},
		{
			// Apply joins are left-driven and don't get commuted by
			// execbuild, so RightSemi/RightAnti shouldn't arise — but if one
			// somehow appears, decompileChildren should not swap.
			name: "apply join not swapped",
			node: &Node{
				op:       applyJoinOp,
				args:     &applyJoinArgs{JoinType: descpb.LeftSemiJoin},
				children: []*Node{left, right},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.node.decompileChildren()
			require.NoError(t, err)
			require.Len(t, got, 2)
			if tc.expectSwap {
				require.Same(t, right, got[0], "expected swap: got[0] should be the original right")
				require.Same(t, left, got[1], "expected swap: got[1] should be the original left")
			} else {
				require.Same(t, left, got[0], "expected no swap: got[0] should be the original left")
				require.Same(t, right, got[1], "expected no swap: got[1] should be the original right")
			}
		})
	}
}

func TestApplyJoinTypeToOp(t *testing.T) {
	tests := []struct {
		joinType   descpb.JoinType
		expectedOp opt.Operator
	}{
		{joinType: descpb.InnerJoin, expectedOp: opt.InnerJoinApplyOp},
		{joinType: descpb.LeftOuterJoin, expectedOp: opt.LeftJoinApplyOp},
		{joinType: descpb.LeftSemiJoin, expectedOp: opt.SemiJoinApplyOp},
		{joinType: descpb.LeftAntiJoin, expectedOp: opt.AntiJoinApplyOp},
		// Apply joins don't have right/full/outer variants in the optimizer.
		{joinType: descpb.RightOuterJoin, expectedOp: opt.UnknownOp},
		{joinType: descpb.FullOuterJoin, expectedOp: opt.UnknownOp},
		{joinType: descpb.RightSemiJoin, expectedOp: opt.UnknownOp},
		{joinType: descpb.RightAntiJoin, expectedOp: opt.UnknownOp},
	}

	for _, tc := range tests {
		t.Run(tc.joinType.String(), func(t *testing.T) {
			require.Equal(t, tc.expectedOp, applyJoinTypeToOp(tc.joinType))
		})
	}
}

// TestDecompileSetOpAlternates verifies the optimizer-operator alternation
// each set-op gist node decompiles to. The gist preserves the ALL bit but
// drops the set operation type (UNION/INTERSECT/EXCEPT). UnionAll and
// LocalityOptimizedSearch both lower to unionAllOp in the gist, so neither
// can appear in the hash/streaming alternates.
func TestDecompileSetOpAlternates(t *testing.T) {
	tests := []struct {
		name        string
		node        *Node
		expectedOps []opt.Operator
	}{
		{
			name:        "hash set op (distinct)",
			node:        &Node{op: hashSetOpOp, args: &hashSetOpArgs{All: false}},
			expectedOps: []opt.Operator{opt.UnionOp, opt.IntersectOp, opt.ExceptOp},
		},
		{
			name:        "hash set op (all)",
			node:        &Node{op: hashSetOpOp, args: &hashSetOpArgs{All: true}},
			expectedOps: []opt.Operator{opt.IntersectAllOp, opt.ExceptAllOp},
		},
		{
			name:        "streaming set op (distinct)",
			node:        &Node{op: streamingSetOpOp, args: &streamingSetOpArgs{All: false}},
			expectedOps: []opt.Operator{opt.UnionOp, opt.IntersectOp, opt.ExceptOp},
		},
		{
			name:        "streaming set op (all)",
			node:        &Node{op: streamingSetOpOp, args: &streamingSetOpArgs{All: true}},
			expectedOps: []opt.Operator{opt.IntersectAllOp, opt.ExceptAllOp},
		},
		{
			name:        "union all covers UnionAll and LocalityOptimizedSearch",
			node:        &Node{op: unionAllOp, args: &unionAllArgs{}},
			expectedOps: []opt.Operator{opt.UnionAllOp, opt.LocalityOptimizedSearchOp},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ops, _ := tc.node.decompile()
			require.Equal(t, tc.expectedOps, ops)
		})
	}
}

// TestDecompileMatchesLocalityOptimizedSearch verifies that the decompiled
// grammar for a unionAllOp gist node matches both UnionAllOp and
// LocalityOptimizedSearchOp at the root, since execbuilder lowers both to
// ConstructUnionAll and the gist alone cannot distinguish them.
func TestDecompileMatchesLocalityOptimizedSearch(t *testing.T) {
	left := &Node{op: scanOp, args: &scanArgs{}}
	right := &Node{op: scanOp, args: &scanArgs{}}
	root := &Node{
		op:       unionAllOp,
		args:     &unionAllArgs{},
		children: []*Node{left, right},
	}

	pg, err := DecompileToPlanGram(root)
	require.NoError(t, err)

	visited := alternates(t, pg)
	require.True(t, anyMatches(visited, &mockExpr{op: opt.UnionAllOp, children: 2}),
		"expected UnionAllOp match; alternates: %v", grammarsToStrings(visited))
	require.True(t, anyMatches(visited, &mockExpr{op: opt.LocalityOptimizedSearchOp, children: 2}),
		"expected LocalityOptimizedSearchOp match; alternates: %v", grammarsToStrings(visited))
}

// TestDecompileWrapsSimpleProject verifies that a simpleProjectOp node in a
// gist tree decompiles to a "(Project child) | child" alternation, so the
// resulting PlanGram matches both shapes the gist could have come from:
//
//   - the optimizer plan with a real ProjectExpr wrapping the input
//     (e.g. SELECT col FROM ...);
//   - the optimizer plan with no Project at this position, where the
//     simpleProject was added by an execbuilder wrapper such as
//     applyPresentation.
//
// The grammar's behavior is asserted by enumerating its root-level alternates
// and checking that each shape (Project wrapping a Scan, and a bare Scan)
// matches at least one of them.
func TestDecompileWrapsSimpleProject(t *testing.T) {
	// Build a gist node tree: simpleProject -> scan.
	scan := &Node{op: scanOp, args: &scanArgs{}}
	root := &Node{op: simpleProjectOp, children: []*Node{scan}}

	pg, err := DecompileToPlanGram(root)
	require.NoError(t, err)

	t.Run("matches Project wrapping Scan", func(t *testing.T) {
		visited := alternates(t, pg)
		require.True(t, anyMatches(visited, &mockExpr{op: opt.ProjectOp, children: 1}),
			"alternates: %v", grammarsToStrings(visited))
	})
	t.Run("matches bare Scan", func(t *testing.T) {
		visited := alternates(t, pg)
		require.True(t, anyMatches(visited, &mockExpr{op: opt.ScanOp, children: 0}),
			"alternates: %v", grammarsToStrings(visited))
	})
}

// alternates returns the expanded concrete-expression alternates reachable
// from the root of pg, mirroring how enforceProps expands a production with
// multiple rules during optimization.
func alternates(t *testing.T, pg physical.PlanGram) []physical.PlanGram {
	t.Helper()
	var out []physical.PlanGram
	pg.VisitAlternates(func(alt physical.PlanGram) {
		out = append(out, alt)
	})
	return out
}

func anyMatches(alts []physical.PlanGram, e *mockExpr) bool {
	for _, a := range alts {
		if a.Matches(e, nil /* md */) {
			return true
		}
	}
	return false
}

func grammarsToStrings(alts []physical.PlanGram) []string {
	out := make([]string, len(alts))
	for i, a := range alts {
		out[i] = a.String()
	}
	return out
}

// mockExpr is a minimal opt.Expr used to drive PlanGram.Matches in tests.
type mockExpr struct {
	op       opt.Operator
	children int
}

func (m *mockExpr) Op() opt.Operator   { return m.op }
func (m *mockExpr) ChildCount() int    { return m.children }
func (m *mockExpr) Child(int) opt.Expr { return nil }
func (m *mockExpr) Private() any       { return nil }
