// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
