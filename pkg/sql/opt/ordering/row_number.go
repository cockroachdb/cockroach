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
	r := expr.(*memo.RowNumberExpr)
	prefix := rowNumberOrdPrefix(r, required)
	if prefix < len(required.Columns) {
		truncated := required.Copy()
		truncated.Truncate(prefix)
		return r.Ordering.Implies(&truncated)
	}
	return r.Ordering.Implies(required)
}

// rowNumberOrdPrefix is the length of the longest prefix of required.Columns
// before the ordinality column (or the entire length if the ordinality column
// is not in the required ordering).
//
// By construction, any prefix of the ordering internally required of the input
// (i.e. RowNumberPrivate.Ordering) is also ordered by the ordinality column.
// For example, if the internal ordering is +a,+b, then the ord column numbers
// rows in the +a,+b order and any of these required orderings can be provided:
//   +ord
//   +a,+ord
//   +a,+b,+ord
//
// As long as normalization rules are enabled, they will have already reduced
// the ordering required of this operator to take into account that the
// ordinality column is a key, so there will not be ordering columns after the
// ordinality column in that case.
func rowNumberOrdPrefix(r *memo.RowNumberExpr, required *physical.OrderingChoice) int {
	ordCol := opt.MakeOrderingColumn(r.ColID, false /* descending */)
	for i := range required.Columns {
		if required.MatchesAt(i, ordCol) {
			return i
		}
	}
	return len(required.Columns)
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

func rowNumberBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	r := expr.(*memo.RowNumberExpr)
	childProvided := r.Input.ProvidedPhysical().Ordering
	prefix := rowNumberOrdPrefix(r, required)
	if prefix == len(required.Columns) {
		// The required ordering doesn't contain the ordinality column, so it only
		// refers to columns that are in the input.
		return trimProvided(childProvided, required, &r.Relational().FuncDeps)
	}

	truncated := required.Copy()
	truncated.Truncate(prefix)
	// The input's provided ordering satisfies both <truncated> and the RowNumber
	// internal ordering; it may need to be trimmed.
	provided := trimProvided(childProvided, &truncated, &r.Relational().FuncDeps)
	// Add the ordinality column to the provided ordering so that it satisfies
	// <required>.
	provided = append(
		provided[:len(provided):len(provided)], // force reallocation
		opt.MakeOrderingColumn(r.ColID, false /* descending */),
	)
	return provided
}
