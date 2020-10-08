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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

func mutationCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// The mutation operator can always pass through ordering to its input.
	return true
}

func mutationBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	if childIdx != 0 {
		return physical.OrderingChoice{}
	}

	// Remap each of the required columns to corresponding input columns.
	private := parent.Private().(*memo.MutationPrivate)

	optional := private.MapToInputCols(required.Optional)
	columns := make([]physical.OrderingColumnChoice, len(required.Columns))
	for i := range required.Columns {
		colChoice := &required.Columns[i]
		columns[i] = physical.OrderingColumnChoice{
			Group:      private.MapToInputCols(colChoice.Group),
			Descending: colChoice.Descending,
		}
	}
	return physical.OrderingChoice{Optional: optional, Columns: columns}
}

func mutationBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	private := expr.Private().(*memo.MutationPrivate)
	input := expr.Child(0).(memo.RelExpr)
	provided := input.ProvidedPhysical().Ordering

	// Construct FD set that includes mapping to/from input columns. This will
	// be used by remapProvided.
	var fdset props.FuncDepSet
	fdset.CopyFrom(&input.Relational().FuncDeps)
	private.AddEquivTableCols(expr.Memo().Metadata(), &fdset)

	// Ensure that provided ordering only uses projected columns.
	return remapProvided(provided, &fdset, expr.Relational().OutputCols)
}
