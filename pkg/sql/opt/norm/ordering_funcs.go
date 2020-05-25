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

package norm

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

// CanSimplifyLimitOffsetOrdering returns true if the ordering required by the
// Limit or Offset operator can be made less restrictive, so that the input
// operator has more ordering choices.
func (c *CustomFuncs) CanSimplifyLimitOffsetOrdering(
	in memo.RelExpr, ordering physical.OrderingChoice,
) bool {
	return c.canSimplifyOrdering(in, ordering)
}

// SimplifyLimitOffsetOrdering makes the ordering required by the Limit or
// Offset operator less restrictive by removing optional columns, adding
// equivalent columns, and removing redundant columns.
func (c *CustomFuncs) SimplifyLimitOffsetOrdering(
	input memo.RelExpr, ordering physical.OrderingChoice,
) physical.OrderingChoice {
	return c.simplifyOrdering(input, ordering)
}

// CanSimplifyGroupingOrdering returns true if the ordering required by the
// grouping operator can be made less restrictive, so that the input operator
// has more ordering choices.
func (c *CustomFuncs) CanSimplifyGroupingOrdering(
	in memo.RelExpr, private *memo.GroupingPrivate,
) bool {
	return c.canSimplifyOrdering(in, private.Ordering)
}

// SimplifyGroupingOrdering makes the ordering required by the grouping operator
// less restrictive by removing optional columns, adding equivalent columns, and
// removing redundant columns.
func (c *CustomFuncs) SimplifyGroupingOrdering(
	in memo.RelExpr, private *memo.GroupingPrivate,
) *memo.GroupingPrivate {
	// Copy GroupingPrivate to stack and replace Ordering field.
	copy := *private
	copy.Ordering = c.simplifyOrdering(in, private.Ordering)
	return &copy
}

// CanSimplifyRowNumberOrdering returns true if the ordering required by the
// RowNumber operator can be made less restrictive, so that the input operator
// has more ordering choices.
func (c *CustomFuncs) CanSimplifyRowNumberOrdering(
	in memo.RelExpr, private *memo.RowNumberPrivate,
) bool {
	return c.canSimplifyOrdering(in, private.Ordering)
}

// SimplifyRowNumberOrdering makes the ordering required by the RowNumber
// operator less restrictive by removing optional columns, adding equivalent
// columns, and removing redundant columns.
func (c *CustomFuncs) SimplifyRowNumberOrdering(
	in memo.RelExpr, private *memo.RowNumberPrivate,
) *memo.RowNumberPrivate {
	// Copy RowNumberPrivate to stack and replace Ordering field.
	copy := *private
	copy.Ordering = c.simplifyOrdering(in, private.Ordering)
	return &copy
}

// CanSimplifyExplainOrdering returns true if the ordering required by the
// Explain operator can be made less restrictive, so that the input operator
// has more ordering choices.
func (c *CustomFuncs) CanSimplifyExplainOrdering(
	in memo.RelExpr, private *memo.ExplainPrivate,
) bool {
	return c.canSimplifyOrdering(in, private.Props.Ordering)
}

// SimplifyExplainOrdering makes the ordering required by the Explain operator
// less restrictive by removing optional columns, adding equivalent columns, and
// removing redundant columns.
func (c *CustomFuncs) SimplifyExplainOrdering(
	in memo.RelExpr, private *memo.ExplainPrivate,
) *memo.ExplainPrivate {
	// Copy ExplainPrivate and its physical properties to stack and replace
	// Ordering field in the copied properties.
	copy := *private
	copyProps := *private.Props
	copyProps.Ordering = c.simplifyOrdering(in, private.Props.Ordering)
	copy.Props = &copyProps
	return &copy
}

func (c *CustomFuncs) canSimplifyOrdering(in memo.RelExpr, ordering physical.OrderingChoice) bool {
	// If any ordering is allowed, nothing to simplify.
	if ordering.Any() {
		return false
	}
	return ordering.CanSimplify(&in.Relational().FuncDeps)
}

func (c *CustomFuncs) simplifyOrdering(
	in memo.RelExpr, ordering physical.OrderingChoice,
) physical.OrderingChoice {
	simplified := ordering.Copy()
	simplified.Simplify(&in.Relational().FuncDeps)
	return simplified
}
