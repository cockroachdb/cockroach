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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
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
