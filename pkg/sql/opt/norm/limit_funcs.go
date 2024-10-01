// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package norm

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// LimitGeMaxRows returns true if the given constant limit value is greater than
// or equal to the max number of rows returned by the input expression.
func (c *CustomFuncs) LimitGeMaxRows(limit tree.Datum, input memo.RelExpr) bool {
	limitVal := int64(*limit.(*tree.DInt))
	maxRows := input.Relational().Cardinality.Max
	return limitVal >= 0 && maxRows < math.MaxUint32 && limitVal >= int64(maxRows)
}
