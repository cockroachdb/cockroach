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
	return *required
}

func selectBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	s := expr.(*memo.SelectExpr)
	rel := s.Relational()
	// We don't need to remap columns, but we want to remove columns that are now
	// unnecessary (e.g. because the Select constrains them to be constant).
	return remapProvided(s.Input.ProvidedPhysical().Ordering, &rel.FuncDeps, rel.OutputCols)
}
