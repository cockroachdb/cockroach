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
	// Update requires a certain ordering of its input, but can also pass through
	// a stronger ordering. For example:
	//
	//   SELECT * FROM [UPDATE t1 SET y=1 ORDER BY x] ORDER BY x,y
	//
	// In this case the internal ordering is x+, but we can pass through x+,y+
	// to satisfy both orderings.
	return required.Intersects(&expr.(*memo.UpdateExpr).Ordering)
}

func updateBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	return required.Intersection(&parent.(*memo.UpdateExpr).Ordering)
}

func updateBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	update := expr.(*memo.UpdateExpr)
	provided := update.Input.ProvidedPhysical().Ordering
	inputFDs := &update.Input.Relational().FuncDeps

	// Ensure that provided ordering only uses projected columns.
	provided = remapProvided(provided, inputFDs, update.Relational().OutputCols)

	// The child's provided ordering satisfies both <required> and the Update
	// internal ordering; it may need to be trimmed.
	return trimProvided(provided, required, &expr.Relational().FuncDeps)
}
