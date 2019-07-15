// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Most of the functionality in statistics.go is tested by the data-driven
// testing in logical_props_factory_test.go. This file contains tests for
// functions in statistics.go that cannot be tested using the data-driven
// testing framework.

// Test getting statistics from constraints that cannot yet be inferred
// by the optimizer.
func TestGetStatsFromConstraint(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	catalog := testcat.New()
	if _, err := catalog.ExecuteDDL(
		"CREATE TABLE sel (a INT, b INT, c INT, d STRING, e STRING)",
	); err != nil {
		t.Fatal(err)
	}

	if _, err := catalog.ExecuteDDL(
		`ALTER TABLE sel INJECT STATISTICS '[
		{
			"columns": ["a"],
			"created_at": "2018-01-01 1:00:00.00000+00:00",
			"row_count": 10000000000,
			"distinct_count": 500
		},
		{
			"columns": ["b"],
			"created_at": "2018-01-01 1:30:00.00000+00:00",
			"row_count": 10000000000,
			"distinct_count": 500
		},
		{
			"columns": ["c"],
			"created_at": "2018-01-01 1:30:00.00000+00:00",
			"row_count": 10000000000,
			"distinct_count": 500
		},
		{
			"columns": ["a","b","c"],
			"created_at": "2018-01-01 1:30:00.00000+00:00",
			"row_count": 10000000000,
			"distinct_count": 9900
		},
		{
			"columns": ["d"],
			"created_at": "2018-01-01 1:30:00.00000+00:00",
			"row_count": 10000000000,
			"distinct_count": 10
		},
		{
			"columns": ["e"],
			"created_at": "2018-01-01 1:30:00.00000+00:00",
			"row_count": 10000000000,
			"distinct_count": 10
		},
		{
			"columns": ["d","e"],
			"created_at": "2018-01-01 1:30:00.00000+00:00",
			"row_count": 10000000000,
			"distinct_count": 100
		}
	]'`); err != nil {
		t.Fatal(err)
	}

	var mem Memo
	mem.Init(&evalCtx)
	tn := tree.NewUnqualifiedTableName("sel")
	tab := catalog.Table(tn)
	tabID := mem.Metadata().AddTable(tab, tn)

	// Test that applyConstraintSet correctly updates the statistics from
	// constraint set cs, and selectivity is calculated correctly.
	statsFunc := func(cs *constraint.Set, expectedStats string, expectedSelectivity float64) {
		t.Helper()

		var cols opt.ColSet
		for i := 0; i < tab.ColumnCount(); i++ {
			cols.Add(tabID.ColumnID(i))
		}

		sb := &statisticsBuilder{}
		sb.init(&evalCtx, mem.Metadata())

		// Make the scan.
		scan := mem.MemoizeScan(&ScanPrivate{Table: tabID, Cols: cols})

		// Make the select.
		sel := mem.MemoizeSelect(scan, TrueFilter)

		relProps := &props.Relational{Cardinality: props.AnyCardinality}
		relProps.NotNullCols = cs.ExtractNotNullCols(&evalCtx)
		s := &relProps.Stats
		s.Init(relProps)

		// Calculate distinct counts.
		sb.applyConstraintSet(cs, true /* tight */, sel, relProps)

		// Calculate row count and selectivity.
		s.RowCount = scan.Relational().Stats.RowCount
		savedRowCount := s.RowCount
		s.ApplySelectivity(sb.selectivityFromDistinctCounts(cols, sel, s))

		// Update null counts.
		sb.updateNullCountsFromProps(sel, relProps, savedRowCount)
		s.ApplySelectivity(sb.selectivityFromNullCounts(cols, sel, s, savedRowCount))

		// Check if the statistics match the expected value.
		testStats(t, s, expectedStats, expectedSelectivity)
	}

	c1 := constraint.ParseConstraint(&evalCtx, "/1: [/2 - /5] [/8 - /10]")
	c2 := constraint.ParseConstraint(&evalCtx, "/2: [/3 - ]")
	c3 := constraint.ParseConstraint(&evalCtx, "/3: [/6 - /6]")
	c12 := constraint.ParseConstraint(&evalCtx, "/1/2/3: [/1/2 - /1/3] [/1/4 - /1]")
	c123 := constraint.ParseConstraint(&evalCtx, "/1/2/3: [/1/2/3 - /1/2/3] [/1/2/5 - /1/2/8]")
	c123n := constraint.ParseConstraint(&evalCtx, "/1/2/3: [/1/2/NULL - /1/2/3] [/1/2/5 - /1/2/8]")
	c32 := constraint.ParseConstraint(&evalCtx, "/3/-2: [/5/3 - /5/2]")
	c321 := constraint.ParseConstraint(&evalCtx, "/-3/2/1: [/5/3/1 - /5/3/4] [/3/5/1 - /3/5/4]")
	c312 := constraint.ParseConstraint(&evalCtx, "/3/1/-2: [/5/3/8 - /5/3/6] [/9/5/4 - /9/5/1]")
	c312n := constraint.ParseConstraint(&evalCtx, "/3/1/-2: [/5/3/8 - /5/3/6] [/9/5/4 - /9/5/NULL]")

	// /4/5: [/'apple'/'cherry' - /'apple'/'mango']
	appleCherry := constraint.MakeCompositeKey(tree.NewDString("apple"), tree.NewDString("cherry"))
	appleMango := constraint.MakeCompositeKey(tree.NewDString("apple"), tree.NewDString("mango"))
	var sp45 constraint.Span
	sp45.Init(appleCherry, constraint.IncludeBoundary, appleMango, constraint.IncludeBoundary)

	var columns45 constraint.Columns
	columns45.Init([]opt.OrderingColumn{4, 5})
	keyCtx45 := constraint.MakeKeyContext(&columns45, &evalCtx)

	cs1 := constraint.SingleConstraint(&c1)
	statsFunc(
		cs1,
		"[rows=140000000, distinct(1)=7, null(1)=0]",
		7.0/500,
	)

	cs2 := constraint.SingleConstraint(&c2)
	statsFunc(
		cs2,
		"[rows=3.33333333e+09, distinct(2)=166.666667, null(2)=0]",
		1.0/3,
	)

	cs3 := constraint.SingleConstraint(&c3)
	statsFunc(
		cs3,
		"[rows=20000000, distinct(3)=1, null(3)=0]",
		1.0/500,
	)

	cs12 := constraint.SingleConstraint(&c12)
	statsFunc(
		cs12,
		"[rows=20000000, distinct(1)=1, null(1)=0, distinct(2)=500, null(2)=0]",
		1.0/500,
	)

	cs123 := constraint.SingleConstraint(&c123)
	statsFunc(
		cs123,
		"[rows=400, distinct(1)=1, null(1)=0, distinct(2)=1, null(2)=0, distinct(3)=5, null(3)=0]",
		5.0/125000000,
	)

	cs123n := constraint.SingleConstraint(&c123n)
	statsFunc(
		cs123n,
		"[rows=40000, distinct(1)=1, null(1)=0, distinct(2)=1, null(2)=0]",
		1.0/250000,
	)

	cs32 := constraint.SingleConstraint(&c32)
	statsFunc(
		cs32,
		"[rows=80000, distinct(2)=2, null(2)=0, distinct(3)=1, null(3)=0]",
		2.0/250000,
	)

	cs321 := constraint.SingleConstraint(&c321)
	statsFunc(
		cs321,
		"[rows=160000, distinct(1)=500, null(1)=0, distinct(2)=2, null(2)=0, distinct(3)=2, null(3)=0]",
		4.0/250000,
	)

	cs312 := constraint.SingleConstraint(&c312)
	statsFunc(
		cs312,
		"[rows=2240, distinct(1)=2, null(1)=0, distinct(2)=7, null(2)=0, distinct(3)=2, null(3)=0]",
		28.0/125000000,
	)

	cs312n := constraint.SingleConstraint(&c312n)
	statsFunc(
		cs312n,
		"[rows=160000, distinct(1)=2, null(1)=0, distinct(3)=2, null(3)=0]",
		1.0/62500,
	)

	cs := cs3.Intersect(&evalCtx, cs123)
	statsFunc(
		cs,
		"[rows=80, distinct(1)=1, null(1)=0, distinct(2)=1, null(2)=0, distinct(3)=1, null(3)=0]",
		1.0/125000000,
	)

	cs = cs32.Intersect(&evalCtx, cs123)
	statsFunc(
		cs,
		"[rows=80, distinct(1)=1, null(1)=0, distinct(2)=1, null(2)=0, distinct(3)=1, null(3)=0]",
		1.0/125000000,
	)

	cs45 := constraint.SingleSpanConstraint(&keyCtx45, &sp45)
	statsFunc(
		cs45,
		"[rows=1e+09, distinct(4)=1, null(4)=0, distinct(5)=10, null(5)=0]",
		1.0/10,
	)
}

func TestTranslateColSet(t *testing.T) {
	test := func(t *testing.T, colSetIn opt.ColSet, from opt.ColList, to opt.ColList, expected opt.ColSet) {
		t.Helper()

		actual := translateColSet(colSetIn, from, to)
		if !actual.Equals(expected) {
			t.Fatalf("\nexpected: %s\nactual  : %s", expected, actual)
		}
	}

	colSetIn, from, to := opt.MakeColSet(1, 2, 3), opt.ColList{1, 2, 3}, opt.ColList{4, 5, 6}
	test(t, colSetIn, from, to, opt.MakeColSet(4, 5, 6))

	colSetIn, from, to = opt.MakeColSet(2, 3), opt.ColList{1, 2, 3}, opt.ColList{4, 5, 6}
	test(t, colSetIn, from, to, opt.MakeColSet(5, 6))

	// colSetIn and colSetOut might not be the same length.
	colSetIn, from, to = opt.MakeColSet(1, 2), opt.ColList{1, 1, 2}, opt.ColList{4, 5, 6}
	test(t, colSetIn, from, to, opt.MakeColSet(4, 5, 6))

	colSetIn, from, to = opt.MakeColSet(1, 2, 3), opt.ColList{1, 2, 3}, opt.ColList{4, 5, 4}
	test(t, colSetIn, from, to, opt.MakeColSet(4, 5))

	colSetIn, from, to = opt.MakeColSet(2), opt.ColList{1, 2, 2}, opt.ColList{4, 5, 6}
	test(t, colSetIn, from, to, opt.MakeColSet(5, 6))
}

func testStats(
	t *testing.T, s *props.Statistics, expectedStats string, expectedSelectivity float64,
) {
	t.Helper()

	actual := s.String()
	if actual != expectedStats {
		t.Fatalf("\nexpected: %s\nactual  : %s", expectedStats, actual)
	}

	if s.Selectivity != expectedSelectivity {
		t.Fatalf("\nexpected: %f\nactual  : %f", expectedSelectivity, s.Selectivity)
	}
}
