// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// Prettier aliases for JoinType values. See the original types for
// descriptions.
const (
	InnerJoin        = JoinType_INNER
	LeftOuterJoin    = JoinType_LEFT_OUTER
	RightOuterJoin   = JoinType_RIGHT_OUTER
	FullOuterJoin    = JoinType_FULL_OUTER
	LeftSemiJoin     = JoinType_LEFT_SEMI
	LeftAntiJoin     = JoinType_LEFT_ANTI
	IntersectAllJoin = JoinType_INTERSECT_ALL
	ExceptAllJoin    = JoinType_EXCEPT_ALL
	RightSemiJoin    = JoinType_RIGHT_SEMI
	RightAntiJoin    = JoinType_RIGHT_ANTI
)

// JoinTypeFromAstString takes a join string as found in a SQL
// statement (e.g. "INNER JOIN") and returns the JoinType.
func JoinTypeFromAstString(joinStr string) JoinType {
	switch joinStr {
	case "", tree.AstInner, tree.AstCross:
		return InnerJoin

	case tree.AstLeft:
		return LeftOuterJoin

	case tree.AstRight:
		return RightOuterJoin

	case tree.AstFull:
		return FullOuterJoin

	default:
		panic(errors.AssertionFailedf("unknown join string %s", joinStr))
	}
}

// IsSetOpJoin returns true if this join is a set operation.
func (j JoinType) IsSetOpJoin() bool {
	return j == IntersectAllJoin || j == ExceptAllJoin
}

// ShouldIncludeLeftColsInOutput returns true if this join should include
// the columns from the left side into the output.
func (j JoinType) ShouldIncludeLeftColsInOutput() bool {
	switch j {
	case RightSemiJoin, RightAntiJoin:
		return false
	default:
		return true
	}
}

// ShouldIncludeRightColsInOutput returns true if this join should include
// the columns from the right side into the output.
func (j JoinType) ShouldIncludeRightColsInOutput() bool {
	switch j {
	case LeftSemiJoin, LeftAntiJoin, IntersectAllJoin, ExceptAllJoin:
		return false
	default:
		return true
	}
}

// IsEmptyOutputWhenRightIsEmpty returns whether this join type will always
// produce an empty output when the right relation is empty.
func (j JoinType) IsEmptyOutputWhenRightIsEmpty() bool {
	switch j {
	case InnerJoin, RightOuterJoin, LeftSemiJoin,
		RightSemiJoin, IntersectAllJoin, RightAntiJoin:
		return true
	default:
		return false
	}
}
