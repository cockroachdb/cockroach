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

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

func lookupOrIndexJoinCanProvideOrdering(
	expr memo.RelExpr, required *physical.OrderingChoice,
) bool {
	// LookupJoin and IndexJoin can pass through their ordering if the ordering
	// depends only on columns present in the input.
	return isOrderingBoundBy(expr.Child(0).(memo.RelExpr), required)
}

func lookupOrIndexJoinBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	if childIdx != 0 {
		return physical.OrderingChoice{}
	}

	// We may need to remove ordering columns that are not output by the input
	// expression.
	return projectOrderingToInput(parent.Child(0).(memo.RelExpr), required)
}
