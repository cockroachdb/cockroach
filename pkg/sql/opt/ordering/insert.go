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

func insertCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// Insert requires a certain ordering of its input, but can also pass through
	// a stronger ordering. For example:
	//
	//   SELECT * FROM [INSERT INTO t1 SELECT * FROM t2 ORDER BY x] ORDER BY x,y
	//
	// In this case the internal ordering is x+, but we can pass through x+,y+
	// to satisfy both orderings.
	return required.Intersects(&expr.(*memo.InsertExpr).Ordering)
}

func insertBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	return required.Intersection(&parent.(*memo.InsertExpr).Ordering)
}

func insertBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	insert := expr.(*memo.InsertExpr)
	provided := insert.Input.ProvidedPhysical().Ordering
	inputFDs := &insert.Input.Relational().FuncDeps

	// Ensure that provided ordering only uses projected columns.
	provided = remapProvided(provided, inputFDs, insert.Relational().OutputCols)

	// The child's provided ordering satisfies both <required> and the Insert
	// internal ordering; it may need to be trimmed.
	return trimProvided(provided, required, &expr.Relational().FuncDeps)
}
