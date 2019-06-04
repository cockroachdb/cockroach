// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

func selectCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// Select operator can always pass through ordering to its input.
	return true
}

func selectBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	if childIdx != 0 {
		return physical.OrderingChoice{}
	}
	return trimColumnGroups(required, &parent.(*memo.SelectExpr).Input.Relational().FuncDeps)
}

// trimColumnGroups removes columns from ColumnOrderingChoice groups as
// necessary, so that all columns in each group are equivalent according to
// the given FDs. It is used when the parent expression can have column
// equivalences that the input expression does not (for example a Select with an
// equality condition); the columns in a group must be equivalent at the level
// of the operator where the ordering is required.
func trimColumnGroups(
	required *physical.OrderingChoice, fds *props.FuncDepSet,
) physical.OrderingChoice {
	res := *required
	copied := false
	for i := range res.Columns {
		c := &res.Columns[i]
		eqGroup := fds.ComputeEquivGroup(c.AnyID())
		if !c.Group.SubsetOf(eqGroup) {
			if !copied {
				res = res.Copy()
				copied = true
			}
			res.Columns[i].Group = c.Group.Intersection(eqGroup)
		}
	}
	return res
}

func selectBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	s := expr.(*memo.SelectExpr)
	rel := s.Relational()
	// We don't need to remap columns, but we want to remove columns that are now
	// unnecessary (e.g. because the Select constrains them to be constant).
	return remapProvided(s.Input.ProvidedPhysical().Ordering, &rel.FuncDeps, rel.OutputCols)
}
