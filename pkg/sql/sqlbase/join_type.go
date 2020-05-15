// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
		panic(fmt.Sprintf("unknown join string %s", joinStr))
	}
}

// IsSetOpJoin returns true if this join is a set operation.
func (j JoinType) IsSetOpJoin() bool {
	return j == IntersectAllJoin || j == ExceptAllJoin
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
