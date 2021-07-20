// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

func ordinalityCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	r := expr.(*memo.OrdinalityExpr)
	prefix := ordinalityOrdPrefix(r, required)
	if prefix < len(required.Columns) {
		truncated := required.Copy()
		truncated.Truncate(prefix)
		return r.Ordering.Implies(&truncated)
	}
	return r.Ordering.Implies(required)
}

// ordinalityOrdPrefix is the length of the longest prefix of required.Columns
// before the ordinality column (or the entire length if the ordinality column
// is not in the required ordering).
//
// By construction, any prefix of the ordering internally required of the input
// (i.e. OrdinalityPrivate.Ordering) is also ordered by the ordinality column.
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
func ordinalityOrdPrefix(r *memo.OrdinalityExpr, required *props.OrderingChoice) int {
	ordCol := opt.MakeOrderingColumn(r.ColID, false /* descending */)
	for i := range required.Columns {
		if required.MatchesAt(i, ordCol) {
			return i
		}
	}
	return len(required.Columns)
}

func ordinalityBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	if childIdx != 0 {
		return props.OrderingChoice{}
	}
	// RowNumber always requires its internal ordering.
	return parent.(*memo.OrdinalityExpr).Ordering
}

func ordinalityBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	r := expr.(*memo.OrdinalityExpr)
	childProvided := r.Input.ProvidedPhysical().Ordering
	prefix := ordinalityOrdPrefix(r, required)
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
