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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

type tsqueryFilterPlanner struct {
	tabID           opt.TableID
	index           cat.Index
	computedColumns map[opt.ColumnID]opt.ScalarExpr
}

var _ invertedFilterPlanner = &tsqueryFilterPlanner{}

// extractInvertedFilterConditionFromLeaf implements the invertedFilterPlanner
// interface.
func (t *tsqueryFilterPlanner) extractInvertedFilterConditionFromLeaf(
	_ context.Context, _ *eval.Context, expr opt.ScalarExpr,
) (
	invertedExpr inverted.Expression,
	remainingFilters opt.ScalarExpr,
	_ *invertedexpr.PreFiltererStateForInvertedFilterer,
) {
	var constantVal opt.ScalarExpr
	var left, right opt.ScalarExpr
	switch e := expr.(type) {
	case *memo.TSMatchesExpr:
		left, right = e.Left, e.Right
	default:
		// Only the above types are supported.
		return inverted.NonInvertedColExpression{}, expr, nil
	}
	if isIndexColumn(t.tabID, t.index, left, t.computedColumns) && memo.CanExtractConstDatum(right) {
		constantVal = right
	} else if isIndexColumn(t.tabID, t.index, right, t.computedColumns) && memo.CanExtractConstDatum(left) {
		constantVal = left
	} else {
		// Can only accelerate with a single constant value.
		return inverted.NonInvertedColExpression{}, expr, nil
	}
	d := memo.ExtractConstDatum(constantVal)
	if d.ResolvedType() != types.TSQuery {
		panic(errors.AssertionFailedf(
			"trying to apply tsvector inverted index to unsupported type %s", d.ResolvedType(),
		))
	}
	q := d.(*tree.DTSQuery).TSQuery
	var err error
	invertedExpr, err = q.GetInvertedExpr()
	if err != nil {
		// An inverted expression could not be extracted.
		return inverted.NonInvertedColExpression{}, expr, nil
	}

	// If the extracted inverted expression is not tight then remaining filters
	// must be applied after the inverted index scan.
	// TODO(jordan): we could do better here by pruning terms that we successfully
	// turn into inverted expressions during the tsquery tree walk. We'd need to
	// implement a function that removes a term from a tsquery tree.
	if !invertedExpr.IsTight() {
		remainingFilters = expr
	}

	// We do not currently support pre-filtering for text search indexes, so
	// the returned pre-filter state is nil.
	return invertedExpr, remainingFilters, nil
}
