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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/errors"
)

// SynthesizeSingleColIndexSkipScanPrivate creates a ScanPrivate that has its
// PrefixSkipLen set to 1.
func (c *CustomFuncs) SynthesizeSingleColIndexSkipScanPrivate(
	private *memo.ScanPrivate, choice physical.OrderingChoice,
) *memo.IndexSkipScanPrivate {
	return c.SynthesizeIndexSkipScanPrivate(private, 1 /* prefixSkipLen */, choice)
}

// SynthesizeIndexSkipScanPrivate creates a ScanPrivate with the specified PrefixSkipLen.
func (c *CustomFuncs) SynthesizeIndexSkipScanPrivate(
	private *memo.ScanPrivate, prefixSkipLen int, choice physical.OrderingChoice,
) *memo.IndexSkipScanPrivate {
	md := c.e.mem.Metadata()
	tab := md.Table(private.Table)
	index := tab.Index(private.Index)

	newPrivate := memo.IndexSkipScanPrivate{
		ScanPrivate:   *private,
		PrefixSkipLen: prefixSkipLen,
	}

	_, reverse := ordering.ScanPrivateCanProvide(c.e.mem.Metadata(), private, &choice)
	indexSkipOrdering := make(opt.Ordering, 0, index.KeyColumnCount())
	for i := 0; i < index.KeyColumnCount(); i++ {
		indexCol := index.Column(i)
		colID := private.Table.ColumnID(indexCol.Ordinal)
		if !private.Cols.Contains(colID) {
			break
		}
		direction := indexCol.Descending != reverse // != is a boolean XOR.
		indexSkipOrdering = append(indexSkipOrdering, opt.MakeOrderingColumn(colID, direction))
	}

	newPrivate.Ordering = indexSkipOrdering
	newPrivate.Reverse = reverse
	return &newPrivate
}

// ColSetLen returns the length of the colset provided.
func (c *CustomFuncs) ColSetLen(cols opt.ColSet) int {
	return cols.Len()
}

// ExtractGroupingCols returns the grouping cols of a GroupBy, ScalarGroupBy or a DistinctOn.
func (c *CustomFuncs) ExtractGroupingCols(private *memo.GroupingPrivate) opt.ColSet {
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

// PruneIndexSkipScanPrivate prunes the columns of the ScanPrivate based on the needed input.
func (c *CustomFuncs) PruneIndexSkipScanPrivate(
	private *memo.IndexSkipScanPrivate, needed opt.ColSet,
) *memo.IndexSkipScanPrivate {
	newScanPrivate := private
	newScanPrivate.Cols = needed.Intersection(private.Cols)
	return newScanPrivate
}

// SynthesizeLookupJoinPrivate creates a lookup join from an index join.
func (c *CustomFuncs) SynthesizeLookupJoinPrivate(
	private *memo.IndexJoinPrivate, scan *memo.ScanPrivate,
) *memo.LookupJoinPrivate {
	newLookupPrivate := &memo.LookupJoinPrivate{
		JoinType: opt.InnerJoinOp,
		Table:    private.Table,
		Index:    cat.PrimaryIndex,
		Cols:     private.Cols,
	}

	// Set the key columns appropriately.
	md := c.e.mem.Metadata()
	table := md.Table(scan.Table)
	index := table.Index(cat.PrimaryIndex)
	pkCols := make(opt.ColList, 0, index.KeyColumnCount())
	for i := 0; i < index.KeyColumnCount(); i++ {
		pkCols = append(pkCols, scan.Table.ColumnID(index.Column(i).Ordinal))
	}
	newLookupPrivate.KeyCols = pkCols
	return newLookupPrivate
}
