// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bounds

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
)

// ConstructUsingExtremesPredicate returns string of a predicate identifying
// the upper and lower bounds of the stats collection.
func ConstructUsingExtremesPredicate(
	lowerBound tree.Datum, upperBound tree.Datum, columnName string,
) string {
	lbExpr := tree.ComparisonExpr{
		Operator: treecmp.MakeComparisonOperator(treecmp.LT),
		Left:     &tree.ColumnItem{ColumnName: tree.Name(columnName)},
		Right:    lowerBound,
	}

	ubExpr := tree.ComparisonExpr{
		Operator: treecmp.MakeComparisonOperator(treecmp.GT),
		Left:     &tree.ColumnItem{ColumnName: tree.Name(columnName)},
		Right:    upperBound,
	}
	nullExpr := tree.IsNullExpr{
		Expr: &tree.ColumnItem{ColumnName: tree.Name(columnName)},
	}

	pred := tree.OrExpr{
		Left: &nullExpr,
		Right: &tree.OrExpr{
			Left:  &lbExpr,
			Right: &ubExpr,
		},
	}
	return tree.Serialize(&pred)
}

// ConstructUsingExtremesSpans returns a constraint.Spans consisting of a
// lowerbound and upperbound span covering the extremes of an index.
func ConstructUsingExtremesSpans(
	lowerBound tree.Datum, upperBound tree.Datum, index catalog.Index,
) (constraint.Spans, error) {
	var lbSpan constraint.Span
	var ubSpan constraint.Span
	if index.GetKeyColumnDirection(0) == catenumpb.IndexColumn_ASC {
		lbSpan.Init(constraint.EmptyKey, constraint.IncludeBoundary, constraint.MakeKey(lowerBound), constraint.ExcludeBoundary)
		ubSpan.Init(constraint.MakeKey(upperBound), constraint.ExcludeBoundary, constraint.EmptyKey, constraint.IncludeBoundary)
	} else {
		lbSpan.Init(constraint.MakeKey(lowerBound), constraint.ExcludeBoundary, constraint.EmptyKey, constraint.IncludeBoundary)
		ubSpan.Init(constraint.EmptyKey, constraint.IncludeBoundary, constraint.MakeKey(upperBound), constraint.ExcludeBoundary)
	}
	// KV requires that the ranges be in order, so we generate the constraints
	// differently depending on whether we have an ascending or descending
	// index.
	var extremesSpans constraint.Spans
	if index.GetKeyColumnDirection(0) == catenumpb.IndexColumn_ASC {
		extremesSpans.InitSingleSpan(&lbSpan)
		extremesSpans.Append(&ubSpan)
	} else {
		extremesSpans.InitSingleSpan(&ubSpan)
		extremesSpans.Append(&lbSpan)
	}

	return extremesSpans, nil
}

// GetUsingExtremesBounds returns a tree.Datum representing the exclusive upper
// and exclusive lower bounds of the USING EXTREMES span for partial statistics.
func GetUsingExtremesBounds(
	evalCtx *eval.Context, histogram []cat.HistogramBucket,
) (lowerBound tree.Datum, upperBound tree.Datum, _ error) {

	upperBound = histogram[len(histogram)-1].UpperBound
	// Pick the earliest lowerBound that is not null,
	// but if none exist, return error
	for i := range histogram {
		hist := &histogram[i]
		if cmp, err := hist.UpperBound.CompareError(evalCtx, tree.DNull); err != nil {
			return lowerBound, nil, err
		} else if cmp != 0 {
			lowerBound = hist.UpperBound
			break
		}
	}
	if lowerBound == nil {
		return lowerBound, nil,
			pgerror.Newf(
				pgcode.ObjectNotInPrerequisiteState,
				"only NULL values exist in the index, so partial stats cannot be collected")
	}
	return lowerBound, upperBound, nil
}
