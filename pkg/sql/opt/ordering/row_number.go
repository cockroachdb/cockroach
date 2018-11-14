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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

func rowNumberCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// By construction, any prefix of the ordering required of the input is also
	// ordered by the ordinality column. For example, if the required input
	// ordering is +a,+b, then any of these orderings can be provided:
	//
	//   +ord
	//   +a,+ord
	//   +a,+b,+ord
	//
	// As long as the optimizer is enabled, it will have already reduced the
	// ordering required of this operator to take into account that the ordinality
	// column is a key, so there will never be ordering columns after the
	// ordinality column in that case.
	r := &expr.(*memo.RowNumberExpr).RowNumberPrivate
	ordCol := opt.MakeOrderingColumn(r.ColID, false)
	prefix := len(required.Columns)
	for i := range required.Columns {
		if required.MatchesAt(i, ordCol) {
			if i == 0 {
				return true
			}
			prefix = i
			break
		}
	}

	if prefix < len(required.Columns) {
		truncated := required.Copy()
		truncated.Truncate(prefix)
		return r.Ordering.Implies(&truncated)
	}

	return r.Ordering.Implies(required)
}

func rowNumberBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	if childIdx != 0 {
		return physical.OrderingChoice{}
	}
	// RowNumber always requires its internal ordering.
	return parent.(*memo.RowNumberExpr).Ordering
}
