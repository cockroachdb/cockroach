// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

func selectCanProvideOrdering(expr memo.RelExpr, required *props.OrderingChoice) bool {
	// Select operator can always pass through ordering to its input.
	return true
}

func selectBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	if childIdx != 0 {
		return props.OrderingChoice{}
	}
	if required.Any() {
		return *required
	}

	child := parent.(*memo.SelectExpr).Input

	// Use interesting orderings from the child to "guide" the required ordering
	// that gets passed down and make it more likely that the child will be able
	// to provide the ordering. This step is needed before the call to
	// trimColumnGroups, since that function will arbitrarily remove columns from
	// each group to ensure all columns in the group are equivalent according to
	// the child FDs.
	//
	// For example, suppose the required ordering is +(1|2),+3, and the child has
	// an interesting ordering of +1,+2,+3. +1,+2,+3 implies +(1|2),+3, so the
	// child can provide the ordering. However, trimColumnGroups will convert
	// +(1|2),+3 to +1,+3, which the child cannot provide. We should instead pass
	// down +1,+2,+3 as the required ordering to avoid an unnecessary sort.
	//
	// See #33023 for more details.
	orders := DeriveInterestingOrderings(child)
	for i := range orders {
		if orders[i].Implies(required) {
			// Get the common prefix of this ordering and the required ordering to
			// make sure the child required ordering is no stronger than needed. Since
			// we know that orders[i] implies required, the result of CommonPrefix
			// will also imply required. For example, if orders[i] is +1,+2 and
			// required is +1, we should only pass down +1 as the child required
			// ordering.
			order := orders[i].CommonPrefix(required)
			required = &order
			break
		}
	}
	return trimColumnGroups(required, &child.Relational().FuncDeps)
}

// trimColumnGroups removes columns from ColumnOrderingChoice groups as
// necessary, so that all columns in each group are equivalent according to
// the given FDs. It is used when the parent expression can have column
// equivalences that the input expression does not (for example a Select with an
// equality condition); the columns in a group must be equivalent at the level
// of the operator where the ordering is required.
func trimColumnGroups(required *props.OrderingChoice, fds *props.FuncDepSet) props.OrderingChoice {
	res := *required
	copied := false
	for i := range res.Columns {
		c := &res.Columns[i]
		if !fds.AreAllColsEquiv(c.Group) {
			if !copied {
				res = res.Copy()
				copied = true
			}
			anyCol := c.AnyID()
			immutableEquivCols := fds.GetImmutableEquivGroup(anyCol)
			if immutableEquivCols.Empty() {
				// The column is equal only to itself.
				res.Columns[i].Group = opt.MakeColSet(anyCol)
			} else {
				res.Columns[i].Group = c.Group.Intersection(immutableEquivCols)
			}
		}
	}
	return res
}

func selectBuildProvided(expr memo.RelExpr, required *props.OrderingChoice) opt.Ordering {
	s := expr.(*memo.SelectExpr)
	rel := s.Relational()
	// We don't need to remap columns, but we want to remove columns that are now
	// unnecessary (e.g. because the Select constrains them to be constant).
	return remapProvided(s.Input.ProvidedPhysical().Ordering, &rel.FuncDeps, rel.OutputCols)
}
