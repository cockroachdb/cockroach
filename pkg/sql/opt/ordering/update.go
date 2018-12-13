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

func updateCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// Update operator can always pass through ordering to its input.
	return true
}

func updateBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	return *required
}

func updateBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	// It should always be possible to remap the columns in the input's provided
	// ordering.
	upd := expr.(*memo.UpdateExpr)
	return remapProvided(
		upd.Input.ProvidedPhysical().Ordering,
		&upd.Input.Relational().FuncDeps,
		upd.Relational().OutputCols,
	)
}
