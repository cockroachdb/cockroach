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

func limitOrOffsetCanProvideOrdering(expr memo.RelExpr, required *physical.OrderingChoice) bool {
	// Limit/Offset require a certain ordering of their input, but can also pass
	// through a stronger ordering. For example:
	//
	//   SELECT * FROM (SELECT x, y FROM t ORDER BY x LIMIT 10) ORDER BY x,y
	//
	// In this case the internal ordering is x+, but we can pass through x+,y+
	// to satisfy both orderings.
	return required.Intersects(expr.Private().(*physical.OrderingChoice))
}

func limitOrOffsetBuildChildReqOrdering(
	parent memo.RelExpr, required *physical.OrderingChoice, childIdx int,
) physical.OrderingChoice {
	if childIdx != 0 {
		return physical.OrderingChoice{}
	}
	return required.Intersection(parent.Private().(*physical.OrderingChoice))
}

func limitOrOffsetBuildProvided(expr memo.RelExpr, required *physical.OrderingChoice) opt.Ordering {
	childProvided := expr.Child(0).(memo.RelExpr).ProvidedPhysical().Ordering
	// The child's provided ordering satisfies both <required> and the
	// Limit/Offset internal ordering; it may need to be trimmed.
	return trimProvided(childProvided, required, &expr.Relational().FuncDeps)
}
