// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plangram_test

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/plangram"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestVisibleToPlanGram(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name     string
		expr     memo.RelExpr
		expected bool
	}{
		{name: "Scan", expr: &memo.ScanExpr{}, expected: true},
		{name: "Select", expr: &memo.SelectExpr{}, expected: true},
		{name: "InnerJoin", expr: &memo.InnerJoinExpr{}, expected: true},
		{name: "GroupBy", expr: &memo.GroupByExpr{}, expected: true},
		{name: "Limit", expr: &memo.LimitExpr{}, expected: true},
		{name: "Sort", expr: &memo.SortExpr{}, expected: true},
		{name: "Project", expr: &memo.ProjectExpr{}, expected: true},
		{name: "Distribute is invisible", expr: &memo.DistributeExpr{}, expected: false},
		{name: "Barrier is invisible", expr: &memo.BarrierExpr{}, expected: false},
		{name: "Explain is invisible", expr: &memo.ExplainExpr{}, expected: false},
		{name: "NormCycleTestRel is invisible", expr: &memo.NormCycleTestRelExpr{}, expected: false},
		{name: "MemoCycleTestRel is invisible", expr: &memo.MemoCycleTestRelExpr{}, expected: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, plangram.VisibleToPlanGram(tc.expr))
		})
	}
}

func parsePG(t *testing.T, s string) physical.PlanGram {
	t.Helper()
	pg, err := physical.ParsePlanGram(strings.NewReader(s))
	require.NoError(t, err)
	return pg
}

func TestBuildChildRequired(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("invisible parent passes through", func(t *testing.T) {
		pg := parsePG(t, "root: (Scan);")
		result := plangram.BuildChildRequired(&memo.DistributeExpr{}, pg, 0, nil /* mem */)
		require.True(t, result.Equals(pg))
	})

	t.Run("visible parent matching descends to child", func(t *testing.T) {
		pg := parsePG(t, "root: (Select (Scan));")
		result := plangram.BuildChildRequired(&memo.SelectExpr{}, pg, 0, nil /* mem */)
		require.True(t, result.Matches(&memo.ScanExpr{}, nil /* md */))
	})

	t.Run("visible parent mismatching returns none", func(t *testing.T) {
		pg := parsePG(t, "root: (Select);")
		result := plangram.BuildChildRequired(&memo.ScanExpr{}, pg, 0, nil /* mem */)
		require.True(t, result.Equals(physical.NonePlanGram))
	})

	t.Run("any always matches and returns any for child", func(t *testing.T) {
		result := plangram.BuildChildRequired(&memo.SelectExpr{}, physical.AnyPlanGram, 0, nil /* mem */)
		require.True(t, result.Any())
	})

	t.Run("none never matches visible parent", func(t *testing.T) {
		result := plangram.BuildChildRequired(&memo.SelectExpr{}, physical.NonePlanGram, 0, nil /* mem */)
		require.True(t, result.Equals(physical.NonePlanGram))
	})
}

func TestCanProvide(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("invisible expr always returns true", func(t *testing.T) {
		pg := parsePG(t, "root: scan; scan: (Scan) | (Select);")
		require.True(t, plangram.CanProvide(&memo.DistributeExpr{}, pg))
	})

	t.Run("visible with any returns true", func(t *testing.T) {
		require.True(t, plangram.CanProvide(&memo.ScanExpr{}, physical.AnyPlanGram))
	})

	t.Run("visible with none returns true", func(t *testing.T) {
		require.True(t, plangram.CanProvide(&memo.ScanExpr{}, physical.NonePlanGram))
	})

	t.Run("visible with concrete expr returns true", func(t *testing.T) {
		pg := parsePG(t, "root: (Scan);")
		require.True(t, plangram.CanProvide(&memo.ScanExpr{}, pg))
	})

	t.Run("visible with production returns false", func(t *testing.T) {
		pg := parsePG(t, "root: scan; scan: (Scan) | (Select);")
		require.False(t, plangram.CanProvide(&memo.ScanExpr{}, pg))
	})
}

// addTestTable adds a table with the given name to metadata, returning its
// TableID.
func addTestTable(md *opt.Metadata, name string) opt.TableID {
	tab := &testcat.Table{
		TabName: tree.MakeTableNameWithSchema("t", "public", tree.Name(name)),
	}
	return md.AddTable(tab, &tab.TabName)
}

func TestMatchFields(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var md opt.Metadata
	abcID := addTestTable(&md, "abc")
	xyzID := addTestTable(&md, "xyz")

	t.Run("scan table match", func(t *testing.T) {
		pg := parsePG(t, `root: (Scan Table="abc");`)
		expr := &memo.ScanExpr{}
		expr.Table = abcID
		require.True(t, pg.Matches(expr, &md))
	})

	t.Run("scan table mismatch", func(t *testing.T) {
		pg := parsePG(t, `root: (Scan Table="abc");`)
		expr := &memo.ScanExpr{}
		expr.Table = xyzID
		require.False(t, pg.Matches(expr, &md))
	})

	t.Run("index join table match", func(t *testing.T) {
		pg := parsePG(t, `root: (IndexJoin Table="abc");`)
		expr := &memo.IndexJoinExpr{}
		expr.Table = abcID
		require.True(t, pg.Matches(expr, &md))
	})

	t.Run("lookup join table match", func(t *testing.T) {
		pg := parsePG(t, `root: (LookupJoin Table="xyz");`)
		expr := &memo.LookupJoinExpr{}
		expr.Table = xyzID
		require.True(t, pg.Matches(expr, &md))
	})

	t.Run("inverted join table match", func(t *testing.T) {
		pg := parsePG(t, `root: (InvertedJoin Table="abc");`)
		expr := &memo.InvertedJoinExpr{}
		expr.Table = abcID
		require.True(t, pg.Matches(expr, &md))
	})

	t.Run("zigzag join left table match", func(t *testing.T) {
		pg := parsePG(t, `root: (ZigzagJoin LeftTable="abc");`)
		expr := &memo.ZigzagJoinExpr{}
		expr.LeftTable = abcID
		expr.RightTable = xyzID
		require.True(t, pg.Matches(expr, &md))
	})

	t.Run("zigzag join right table mismatch", func(t *testing.T) {
		pg := parsePG(t, `root: (ZigzagJoin RightTable="abc");`)
		expr := &memo.ZigzagJoinExpr{}
		expr.LeftTable = abcID
		expr.RightTable = xyzID
		require.False(t, pg.Matches(expr, &md))
	})

	t.Run("zigzag join both tables", func(t *testing.T) {
		pg := parsePG(t, `root: (ZigzagJoin LeftTable="abc" RightTable="xyz");`)
		expr := &memo.ZigzagJoinExpr{}
		expr.LeftTable = abcID
		expr.RightTable = xyzID
		require.True(t, pg.Matches(expr, &md))
	})

	t.Run("table field on unsupported op", func(t *testing.T) {
		pg := parsePG(t, `root: (Select Table="abc");`)
		expr := &memo.SelectExpr{}
		require.False(t, pg.Matches(expr, &md))
	})

	t.Run("unknown field key", func(t *testing.T) {
		pg := parsePG(t, `root: (Scan Bogus="abc");`)
		expr := &memo.ScanExpr{}
		expr.Table = abcID
		require.False(t, pg.Matches(expr, &md))
	})

	t.Run("nil metadata with fields returns false", func(t *testing.T) {
		pg := parsePG(t, `root: (Scan Table="abc");`)
		expr := &memo.ScanExpr{}
		expr.Table = abcID
		require.False(t, pg.Matches(expr, nil /* md */))
	})

	t.Run("no fields still matches without metadata", func(t *testing.T) {
		pg := parsePG(t, `root: (Scan);`)
		expr := &memo.ScanExpr{}
		require.True(t, pg.Matches(expr, nil /* md */))
	})
}
