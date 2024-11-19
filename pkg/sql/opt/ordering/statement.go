// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ordering

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/errors"
)

// statementBuildChildReqOrdering removes columns from the ordering that are not
// returned by the expression enclosed by the current statement. If this is not
// possible, statementBuildChildReqOrdering panics.
func statementBuildChildReqOrdering(
	statement memo.RelExpr, childIdx int, ordering props.OrderingChoice,
) props.OrderingChoice {
	if childIdx != 0 {
		return props.OrderingChoice{}
	}
	child := statement.Child(0).(memo.RelExpr)
	childCols := child.Relational().OutputCols
	if ordering.SubsetOfCols(childCols) {
		return ordering
	}
	if !ordering.CanProjectCols(childCols) {
		panic(errors.AssertionFailedf(
			"%s ordering %s requires non-output cols from %s",
			statement.Op(),
			ordering.String(),
			child.Op(),
		))
	}
	projectedOrdering := ordering.Copy()
	projectedOrdering.ProjectCols(childCols)
	return projectedOrdering
}

func explainBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	return statementBuildChildReqOrdering(
		parent, childIdx, parent.(*memo.ExplainExpr).Props.Ordering,
	)
}

func alterTableSplitBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	return statementBuildChildReqOrdering(
		parent, childIdx, parent.(*memo.AlterTableSplitExpr).Props.Ordering,
	)
}

func alterTableUnsplitBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	return statementBuildChildReqOrdering(
		parent, childIdx, parent.(*memo.AlterTableUnsplitExpr).Props.Ordering,
	)
}

func alterTableRelocateBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	return statementBuildChildReqOrdering(
		parent, childIdx, parent.(*memo.AlterTableRelocateExpr).Props.Ordering,
	)
}

func alterRangeRelocateBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	return statementBuildChildReqOrdering(
		parent, childIdx, parent.(*memo.AlterRangeRelocateExpr).Props.Ordering,
	)
}

func controlJobsBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	return statementBuildChildReqOrdering(
		parent, childIdx, parent.(*memo.ControlJobsExpr).Props.Ordering,
	)
}

func cancelQueriesBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	return statementBuildChildReqOrdering(
		parent, childIdx, parent.(*memo.CancelQueriesExpr).Props.Ordering,
	)
}

func cancelSessionsBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	return statementBuildChildReqOrdering(
		parent, childIdx, parent.(*memo.CancelSessionsExpr).Props.Ordering,
	)
}

func exportBuildChildReqOrdering(
	parent memo.RelExpr, required *props.OrderingChoice, childIdx int,
) props.OrderingChoice {
	return statementBuildChildReqOrdering(
		parent, childIdx, parent.(*memo.ExportExpr).Props.Ordering,
	)
}
