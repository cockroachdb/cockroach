// Copyright 2020 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/idxconstraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NewDatumsToInvertedExpr returns a new DatumsToInvertedExpr. Currently there
// is only one possible implementation returned, geoDatumsToInvertedExpr.
func NewDatumsToInvertedExpr(
	evalCtx *tree.EvalContext, colTypes []*types.T, expr tree.TypedExpr, desc *descpb.IndexDescriptor,
) (invertedexpr.DatumsToInvertedExpr, error) {
	if geoindex.IsEmptyConfig(&desc.GeoConfig) {
		return nil, fmt.Errorf("inverted joins are currently only supported for spatial indexes")
	}

	return NewGeoDatumsToInvertedExpr(evalCtx, colTypes, expr, &desc.GeoConfig)
}

// NewBoundPreFilterer returns a PreFilterer for the given expr where the type
// of the bound param is specified by typ. Unlike the use of PreFilterer in an
// inverted join, where each left value is bound, this function is for the
// invertedFilterer where the param to be bound is already specified as a
// constant in the expr. The callee will bind this parameter and return the
// opaque pre-filtering state for that binding (the interface{}) in the return
// values).
func NewBoundPreFilterer(typ *types.T, expr tree.TypedExpr) (*PreFilterer, interface{}, error) {
	if !typ.Equivalent(types.Geometry) && !typ.Equivalent(types.Geography) {
		return nil, nil, fmt.Errorf("pre-filtering not supported for type %s", typ)
	}
	return newGeoBoundPreFilterer(typ, expr)
}

// constrainPrefixColumns attempts to build a constraint for the non-inverted
// prefix columns of the given index. If a constraint is successfully built, it
// is returned along with remaining filters and ok=true. The function is only
// successful if it can generate a constraint where all spans have the same
// start and end keys for all non-inverted prefix columns. If the index is a
// single-column inverted index, there are no prefix columns to constrain, and
// ok=true is returned.
func constrainPrefixColumns(
	evalCtx *tree.EvalContext,
	factory *norm.Factory,
	filters memo.FiltersExpr,
	tabID opt.TableID,
	index cat.Index,
) (constraint *constraint.Constraint, remainingFilters memo.FiltersExpr, ok bool) {
	tabMeta := factory.Metadata().TableMeta(tabID)
	prefixColumnCount := index.NonInvertedPrefixColumnCount()

	// If this is a single-column inverted index, there are no prefix columns to
	// constrain.
	if prefixColumnCount == 0 {
		return nil, filters, true
	}

	prefixColumns := make([]opt.OrderingColumn, prefixColumnCount)
	var notNullCols opt.ColSet
	for i := range prefixColumns {
		col := index.Column(i)
		colID := tabID.ColumnID(col.Ordinal())
		prefixColumns[i] = opt.MakeOrderingColumn(colID, col.Descending)
		if !col.IsNullable() {
			notNullCols.Add(colID)
		}
	}

	var ic idxconstraint.Instance
	ic.Init(
		filters, nil, /* optionalFilters */
		prefixColumns, notNullCols, tabMeta.ComputedCols,
		false /* isInverted */, evalCtx, factory,
	)
	constraint = ic.Constraint()
	if constraint.Prefix(evalCtx) < prefixColumnCount {
		// If all spans do not have the same start and end keys for all columns,
		// the index cannot be used.
		return nil, nil, false
	}

	// Make a copy of constraint so that the idxconstraint.Instance is not
	// referenced.
	copy := *constraint
	remainingFilters = ic.RemainingFilters()
	return &copy, remainingFilters, true
}
