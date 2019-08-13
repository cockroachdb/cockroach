// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/errors"
)

// IsIndexSkipScan specifies whether the current skips rows based on an
// index prefix.
func (c *CustomFuncs) IsIndexSkipScan(private *memo.ScanPrivate) bool {
	return private.IsIndexSkipScan()
}

// SynthesizeSingleColSkipScanPrivate creates a ScanPrivate that has its
// PrefixSkipLen set to 1.
func (c *CustomFuncs) SynthesizeSingleColSkipScanPrivate(
	private *memo.ScanPrivate,
) *memo.ScanPrivate {
	return c.SynthesizeSkipScanPrivate(private, 1 /* prefixSkipLen */)
}

// SynthesizeSkipScanPrivate creates a ScanPrivate with the specified PrefixSkipLen.
func (c *CustomFuncs) SynthesizeSkipScanPrivate(
	private *memo.ScanPrivate, prefixSkipLen int,
) *memo.ScanPrivate {
	newPrivate := *private
	newPrivate.PrefixSkipLen = prefixSkipLen
	return &newPrivate
}

// ColSetLen returns the length of the colset provided.
func (c *CustomFuncs) ColSetLen(cols opt.ColSet) int {
	return cols.Len()
}

// GroupingCols returns the grouping cols of a GroupBy, ScalarGroupBy or a DistinctOn.
func (c *CustomFuncs) GroupingCols(private *memo.GroupingPrivate) opt.ColSet {
	return private.GroupingCols.Copy()
}

// HasDistinctOnIndexPrefix specifies whether the aggregate has a AggDistinct on
// a prefix of the index scan.
func (c *CustomFuncs) HasDistinctOnIndexPrefix(
	aggregate opt.ScalarExpr, private *memo.ScanPrivate,
) bool {
	switch aggregate.Op() {
	case opt.CountOp, opt.ArrayAggOp, opt.AvgOp, opt.ConcatAggOp, opt.SumIntOp,
		opt.SumOp, opt.SqrDiffOp, opt.VarianceOp, opt.StdDevOp, opt.XorAggOp,
		opt.JsonAggOp, opt.JsonbAggOp, opt.StringAggOp, opt.ConstAggOp,
		opt.ConstNotNullAggOp, opt.AnyNotNullAggOp, opt.FirstAggOp, opt.MaxOp,
		opt.MinOp, opt.BoolAndOp, opt.BoolOrOp:
		distinctAgg, ok := aggregate.Child(0).(*memo.AggDistinctExpr)
		if !ok {
			return false
		}

		variable, ok := distinctAgg.Input.(*memo.VariableExpr)
		if !ok {
			return false
		}

		return c.IsIndexPrefix(private, opt.MakeColSet(variable.Col))

	}
	return false
}

// RemoveAggDistinct removes the AggDistinctExpr from the aggregate's input.
// NOTE: This only works if there is one aggregation.
func (c *CustomFuncs) RemoveAggDistinct(aggregations memo.AggregationsExpr) memo.AggregationsExpr {
	if len(aggregations) != 1 {
		panic(errors.AssertionFailedf("multiple aggregations cannot be present"))
	}

	aggregation := aggregations[0]
	distinctAgg, ok := aggregation.Agg.Child(0).(*memo.AggDistinctExpr)
	if !ok {
		panic(errors.AssertionFailedf("cannot remove AggDistinct if it isn't present"))
	}
	variable, ok := distinctAgg.Input.(*memo.VariableExpr)
	if !ok {
		panic(errors.AssertionFailedf("AggDistinct input is not a variable"))
	}

	var newAgg memo.AggregationsItem
	newAgg.Agg = c.ReplaceAggInputVar(aggregation.Agg, variable)
	newAgg.Col = aggregation.Col
	return memo.AggregationsExpr{newAgg}
}

// IsIndexPrefix specifies whether the provided columns form a prefix of the index.
func (c *CustomFuncs) IsIndexPrefix(private *memo.ScanPrivate, cols opt.ColSet) bool {
	md := c.e.mem.Metadata()
	tab := md.Table(private.Table)
	index := tab.Index(private.Index)

	if cols.Len() > index.KeyColumnCount() {
		return false
	}

	var prefixIndexCols opt.ColSet
	for i := 0; i < cols.Len(); i++ {
		prefixIndexCols.Add(private.Table.ColumnID(index.Column(i).Ordinal))
	}

	return cols.Equals(prefixIndexCols)
}

// ScanCanProvideOrdering returns whether the scan can provide the required ordering
// without needing a sort.
func (c *CustomFuncs) ScanCanProvideOrdering(
	private *memo.ScanPrivate, choice physical.OrderingChoice,
) bool {
	ok, _ := ordering.ScanPrivateCanProvide(c.e.mem.Metadata(), private, &choice)
	return ok
}

// AggregationCols returns the columns of the aggregations.
func (c *CustomFuncs) AggregationCols(aggregations memo.AggregationsExpr) opt.ColSet {
	var aggCols opt.ColSet
	for i := range aggregations {
		aggCols.Add(aggregations[i].Col)
	}

	return aggCols
}

// PruneScanPrivate prunes the columns of the ScanPrivate based on the needed input.
func (c *CustomFuncs) PruneScanPrivate(
	private *memo.ScanPrivate, needed opt.ColSet,
) *memo.ScanPrivate {
	newScanPrivate := private
	newScanPrivate.Cols = needed.Intersection(private.Cols)
	return newScanPrivate
}
