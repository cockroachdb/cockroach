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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/invertedexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// NewDatumToInvertedExpr returns a new DatumsToInvertedExpr. Currently there
// is only one possible implementation returned, geoDatumToInvertedExpr.
func NewDatumToInvertedExpr(
	evalCtx *tree.EvalContext,
	colTypes []*types.T,
	expr tree.TypedExpr,
	desc *sqlbase.IndexDescriptor,
) (invertedexpr.DatumsToInvertedExpr, error) {
	if geoindex.IsEmptyConfig(&desc.GeoConfig) {
		return nil, fmt.Errorf("inverted joins are currently only supported for geospatial indexes")
	}

	return NewGeoDatumToInvertedExpr(evalCtx, colTypes, expr, &desc.GeoConfig)
}
