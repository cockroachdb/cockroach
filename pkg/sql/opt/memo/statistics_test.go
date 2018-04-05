// Copyright 2018 The Cockroach Authors.
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

package memo

import (
	"testing"

	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// Most of the functionality in statistics.go is tested by the data-driven
// testing in logical_props_factory_test.go. This file contains tests for
// functions in statistics.go that cannot be tested using the data-driven
// testing framework.

// Test getting statistics from constraints that cannot yet be inferred
// by the optimizer.
func TestGetStatsFromConstraint(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	statsFunc123 := func(cs *constraint.Set, expected string) {
		t.Helper()

		colStats := []ColumnStatistics{
			{Cols: util.MakeFastIntSet(1), DistinctCount: 500},
			{Cols: util.MakeFastIntSet(2), DistinctCount: 500},
			{Cols: util.MakeFastIntSet(3), DistinctCount: 500},
			{Cols: util.MakeFastIntSet(1, 2, 3), DistinctCount: 990},
		}
		s := &Statistics{ColStats: colStats, RowCount: 1000}
		s.applyConstraintSet(cs, &evalCtx)
		testStats(t, s, expected)
	}

	statsFunc45 := func(cs *constraint.Set, expected string) {
		t.Helper()

		colStats := []ColumnStatistics{
			{Cols: util.MakeFastIntSet(4), DistinctCount: 10},
			{Cols: util.MakeFastIntSet(5), DistinctCount: 10},
			{Cols: util.MakeFastIntSet(4, 5), DistinctCount: 100},
		}
		s := &Statistics{ColStats: colStats, RowCount: 1000}
		s.applyConstraintSet(cs, &evalCtx)
		testStats(t, s, expected)
	}

	c1 := constraint.ParseConstraint(&evalCtx, "/1: [/2 - /5] [/8 - /10]")
	c2 := constraint.ParseConstraint(&evalCtx, "/2: [/3 - ]")
	c3 := constraint.ParseConstraint(&evalCtx, "/3: [/6 - /6]")
	c12 := constraint.ParseConstraint(&evalCtx, "/1/2/3: [/1/2 - /1/3] [/1/4 - /1]")
	c123 := constraint.ParseConstraint(&evalCtx, "/1/2/3: [/1/2/3 - /1/2/3] [/1/2/5 - /1/2/8]")
	c32 := constraint.ParseConstraint(&evalCtx, "/3/-2: [/5/3 - /5/2]")

	// /4/5: [/'apple'/'cherry' - /'apple'/'mango']
	appleCherry := constraint.MakeCompositeKey(tree.NewDString("apple"), tree.NewDString("cherry"))
	appleMango := constraint.MakeCompositeKey(tree.NewDString("apple"), tree.NewDString("mango"))
	var sp45 constraint.Span
	sp45.Init(appleCherry, constraint.IncludeBoundary, appleMango, constraint.IncludeBoundary)

	var columns45 constraint.Columns
	columns45.Init([]opt.OrderingColumn{4, 5})
	keyCtx45 := constraint.MakeKeyContext(&columns45, &evalCtx)

	cs1 := constraint.SingleConstraint(&c1)
	statsFunc123(
		cs1,
		"stats: [rows=1000, distinct(1)=7, distinct(2)=500, distinct(3)=500, distinct(1-3)=990]",
	)

	cs2 := constraint.SingleConstraint(&c2)
	statsFunc123(
		cs2,
		"stats: [rows=1000, distinct(1)=500, distinct(2)=500, distinct(3)=500, distinct(1-3)=990]",
	)

	cs3 := constraint.SingleConstraint(&c3)
	statsFunc123(
		cs3,
		"stats: [rows=1000, distinct(1)=500, distinct(2)=500, distinct(3)=1, distinct(1-3)=990]",
	)

	cs12 := constraint.SingleConstraint(&c12)
	statsFunc123(
		cs12,
		"stats: [rows=1000, distinct(1)=1, distinct(2)=500, distinct(3)=500, distinct(1-3)=990]",
	)

	cs123 := constraint.SingleConstraint(&c123)
	statsFunc123(
		cs123,
		"stats: [rows=1000, distinct(1)=1, distinct(2)=1, distinct(3)=5, distinct(1-3)=5]",
	)

	cs32 := constraint.SingleConstraint(&c32)
	statsFunc123(
		cs32,
		"stats: [rows=1000, distinct(1)=500, distinct(2)=2, distinct(3)=1, distinct(1-3)=990]",
	)

	cs := cs3.Intersect(&evalCtx, cs123)
	statsFunc123(
		cs,
		"stats: [rows=1000, distinct(1)=1, distinct(2)=1, distinct(3)=1, distinct(1-3)=1]",
	)

	cs = cs32.Intersect(&evalCtx, cs123)
	statsFunc123(
		cs,
		"stats: [rows=1000, distinct(1)=1, distinct(2)=1, distinct(3)=1, distinct(1-3)=1]",
	)

	cs45 := constraint.SingleSpanConstraint(&keyCtx45, &sp45)
	statsFunc45(
		cs45,
		"stats: [rows=1000, distinct(4)=1, distinct(5)=10, distinct(4,5)=100]",
	)
}

func testStats(t *testing.T, s *Statistics, expected string) {
	t.Helper()

	ev := ExprView{}

	tp := treeprinter.New()
	ev.formatStats(tp, s)
	actual := strings.TrimSpace(tp.String())

	if actual != expected {
		t.Fatalf("\nexpected: %s\nactual  : %s", expected, actual)
	}
}
