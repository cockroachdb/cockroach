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

func mutationCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// Insert operator can always pass through ordering to its input. Note that
	// this is not possible for an INSERT...ON CONFLICT statement, which is one
	// reason it's compiled as an Upsert operator rather than an Insert operator.
	// Upsert is handled separately.
	return true
}

func mutationBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
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
