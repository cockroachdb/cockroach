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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// CommuteInequality swaps the operands of an inequality comparison expression,
// changing the operator to compensate:
//   5 < x
// to:
//   x > 5
func (c *CustomFuncs) CommuteInequality(
	op opt.Operator, left, right opt.ScalarExpr,
) opt.ScalarExpr {
	switch op {
	case opt.GeOp:
		return c.f.ConstructLe(right, left)
	case opt.GtOp:
		return c.f.ConstructLt(right, left)
	case opt.LeOp:
		return c.f.ConstructGe(right, left)
	case opt.LtOp:
		return c.f.ConstructGt(right, left)
	}
	panic(errors.AssertionFailedf("called commuteInequality with operator %s", log.Safe(op)))
}

// NormalizeTupleEquality remaps the elements of two tuples compared for
// equality, like this:
//   (a, b, c) = (x, y, z)
// into this:
//   (a = x) AND (b = y) AND (c = z)
func (c *CustomFuncs) NormalizeTupleEquality(left, right memo.ScalarListExpr) opt.ScalarExpr {
	if len(left) != len(right) {
		panic(errors.AssertionFailedf("tuple length mismatch"))
	}
	if len(left) == 0 {
		// () = (), which is always true.
		return memo.TrueSingleton
	}

	var result opt.ScalarExpr
	for i := range left {
		eq := c.f.ConstructEq(left[i], right[i])
		if result == nil {
			result = eq
		} else {
			result = c.f.ConstructAnd(result, eq)
		}
	}
	return result
}
