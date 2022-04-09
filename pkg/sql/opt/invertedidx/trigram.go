// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package invertedidx

import (
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type trigramFilterPlanner struct {
	tabID           opt.TableID
	index           cat.Index
	computedColumns map[opt.ColumnID]opt.ScalarExpr
}

func (t *trigramFilterPlanner) extractInvertedFilterConditionFromLeaf(
	_ *eval.Context, expr opt.ScalarExpr,
) (
	invertedExpr inverted.Expression,
	remainingFilters opt.ScalarExpr,
	_ *invertedexpr.PreFiltererStateForInvertedFilterer,
) {
	var constantVal opt.ScalarExpr
	var left, right opt.ScalarExpr
	switch e := expr.(type) {
	// Both ILIKE and LIKE are supported because the index entries are always
	// downcased. We re-check the condition no matter what later.
	case *memo.ILikeExpr:
		left, right = e.Left, e.Right
	case *memo.LikeExpr:
		left, right = e.Left, e.Right
	default:
		// Only the above types are supported.
		return inverted.NonInvertedColExpression{}, expr, nil
	}
	if isIndexColumn(t.tabID, t.index, left, t.computedColumns) && memo.CanExtractConstDatum(right) {
		constantVal = right
	} else if isIndexColumn(t.tabID, t.index, right, t.computedColumns) && memo.CanExtractConstDatum(left) {
		constantVal = left
	}
	d := memo.ExtractConstDatum(constantVal)
	if d.ResolvedType() != types.String {
		panic(errors.AssertionFailedf(
			"trying to apply inverted index to unsupported type %s", d.ResolvedType(),
		))
	}
	var err error
	invertedExpr, err = rowenc.EncodeLikeTrigramSpans(d.(*tree.DString))
	if err != nil {
		// An inverted expression could not be extracted.
		return inverted.NonInvertedColExpression{}, expr, nil
	}

	// If the extracted inverted expression is not tight then remaining filters
	// must be applied after the inverted index scan.
	if !invertedExpr.IsTight() {
		remainingFilters = expr
	}

	// We do not currently support pre-filtering for JSON and Array indexes, so
	// the returned pre-filter state is nil.
	return invertedExpr, remainingFilters, nil
}

var _ invertedFilterPlanner = &trigramFilterPlanner{}
