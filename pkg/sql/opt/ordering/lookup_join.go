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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
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

func indexJoinBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	rel := expr.Relational()
	return remapProvided(
		expr.Child(0).(memo.RelExpr).ProvidedPhysical().Ordering, &rel.FuncDeps, rel.OutputCols,
	)
}

func lookupJoinBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	// Some of the input columns might not be output columns (if they are not in
	// lookupJoin.Cols) so we might need to remap them. Check for this condition
	// first.
	lookupJoin := expr.(*memo.LookupJoinExpr)
	childProvided := lookupJoin.Input.ProvidedPhysical().Ordering
	needsRemap := false
	for i := range childProvided {
		if !lookupJoin.Cols.Contains(int(childProvided[i].ID())) {
			needsRemap = true
			break
		}
	}
	if !needsRemap {
		// Fast path: we don't need to remap any columns.
		return childProvided
	}

	// The lookup join implicitly adds equality constraints on the lookup columns.
	// Start with the input FDs and apply the equalities.
	var fds props.FuncDepSet
	fds.CopyFrom(&lookupJoin.Input.Relational().FuncDeps)

	md := lookupJoin.Memo().Metadata()
	index := md.Table(lookupJoin.Table).Index(lookupJoin.Index)
	for i, colID := range lookupJoin.KeyCols {
		indexColID := lookupJoin.Table.ColumnID(index.Column(i).Ordinal)
		fds.AddEquivalency(colID, indexColID)
	}

	return remapProvided(childProvided, &fds, lookupJoin.Cols)
}
