// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package invertedidx

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

type trigramFilterPlanner struct {
	tabID           opt.TableID
	index           cat.Index
	computedColumns map[opt.ColumnID]opt.ScalarExpr
}

var _ invertedFilterPlanner = &trigramFilterPlanner{}

// extractInvertedFilterConditionFromLeaf implements the invertedFilterPlanner
// interface.
func (t *trigramFilterPlanner) extractInvertedFilterConditionFromLeaf(
	_ context.Context, evalCtx *eval.Context, expr opt.ScalarExpr,
) (
	invertedExpr inverted.Expression,
	remainingFilters opt.ScalarExpr,
	_ *invertedexpr.PreFiltererStateForInvertedFilterer,
) {
	var constantVal opt.ScalarExpr
	var left, right opt.ScalarExpr
	var allMustMatch bool
	var commutative bool
	switch e := expr.(type) {
	// Both ILIKE and LIKE are supported because the index entries are always
	// downcased. We re-check the condition no matter what later.
	case *memo.ILikeExpr:
		left, right = e.Left, e.Right
		// If we're doing a LIKE (or ILIKE) expression, we need to construct an AND
		// out of all of the spans: we need to find results that match every single
		// one of the trigrams in the constant datum.
		allMustMatch = true
		// ILIKE is not commutative.
		commutative = false
	case *memo.LikeExpr:
		left, right = e.Left, e.Right
		allMustMatch = true
		// LIKE is not commutative.
		// TODO(mgartner): We might be able to index accelerate expressions in
		// the form 'foo' LIKE col. To correctly handle cases where col is '%',
		// we'd have to write some value to the trigram index and always scan
		// it. We currently do not write anything to the trigram index if the
		// value is '%'.
		commutative = false
	case *memo.EqExpr:
		left, right = e.Left, e.Right
		allMustMatch = true
		// Equality is commutative.
		commutative = true
	case *memo.ModExpr:
		// Do not generate legacy inverted constraints for similarity filters
		// if the text similarity optimization is enabled.
		if evalCtx.SessionData().OptimizerUseTrigramSimilarityOptimization {
			return inverted.NonInvertedColExpression{}, expr, nil
		}

		// Do not plan inverted index scans when the trigram similarity threshold is 0
		// because all strings will be matched.
		if evalCtx.SessionData().TrigramSimilarityThreshold == 0 {
			return inverted.NonInvertedColExpression{}, expr, nil
		}
		// If we're doing a % expression (similarity threshold), we need to
		// construct an OR out of the spans: we need to find results that match any
		// of the trigrams in the constant datum, and we'll filter the results
		// further afterwards.
		left, right = e.Left, e.Right
		allMustMatch = false
		// Similarity is commutative.
		commutative = true
	default:
		// Only the above types are supported.
		return inverted.NonInvertedColExpression{}, expr, nil
	}
	if isIndexColumn(t.tabID, t.index, left, t.computedColumns) && memo.CanExtractConstDatum(right) {
		constantVal = right
	} else if commutative && isIndexColumn(t.tabID, t.index, right, t.computedColumns) &&
		memo.CanExtractConstDatum(left) {
		// Commute the expression if the operator is commutative.
		constantVal = left
	} else {
		// Can only accelerate with a single constant value.
		return inverted.NonInvertedColExpression{}, expr, nil
	}
	d := tree.UnwrapDOidWrapper(memo.ExtractConstDatum(constantVal))
	ds, ok := d.(*tree.DString)
	if !ok {
		panic(errors.AssertionFailedf(
			"trying to apply inverted index to unsupported type %s", d.ResolvedType().SQLStringForError(),
		))
	}
	s := string(*ds)
	var err error
	invertedExpr, err = rowenc.EncodeTrigramSpans(s, allMustMatch)
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
