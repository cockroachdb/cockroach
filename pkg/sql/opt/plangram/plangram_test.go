// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plangram_test

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/plangram"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
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
		result := plangram.BuildChildRequired(&memo.DistributeExpr{}, pg, 0)
		require.True(t, result.Equals(pg))
	})

	t.Run("visible parent matching descends to child", func(t *testing.T) {
		pg := parsePG(t, "root: (Select (Scan));")
		result := plangram.BuildChildRequired(&memo.SelectExpr{}, pg, 0)
		require.True(t, result.Matches(&memo.ScanExpr{}))
	})

	t.Run("visible parent mismatching returns none", func(t *testing.T) {
		pg := parsePG(t, "root: (Select);")
		result := plangram.BuildChildRequired(&memo.ScanExpr{}, pg, 0)
		require.True(t, result.Equals(physical.NonePlanGram))
	})

	t.Run("any always matches and returns any for child", func(t *testing.T) {
		result := plangram.BuildChildRequired(&memo.SelectExpr{}, physical.AnyPlanGram, 0)
		require.True(t, result.Any())
	})

	t.Run("none never matches visible parent", func(t *testing.T) {
		result := plangram.BuildChildRequired(&memo.SelectExpr{}, physical.NonePlanGram, 0)
		require.True(t, result.Equals(physical.NonePlanGram))
	})
}

func TestCanProvide(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
