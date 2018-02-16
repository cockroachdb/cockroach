// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
	case tree.AstJoin, tree.AstInnerJoin, tree.AstCrossJoin:
		return InnerJoin

	case tree.AstLeftJoin:
		return LeftOuterJoin

	case tree.AstRightJoin:
		return RightOuterJoin

	case tree.AstFullJoin:
		return FullOuterJoin

	default:
		panic(fmt.Sprintf("unknown join string %s", joinStr))
	}
}
