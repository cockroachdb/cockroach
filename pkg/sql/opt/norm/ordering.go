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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

// CanSimplifyLimitOffsetOrdering returns true if the ordering required by the
// Limit or Offset operator can be made less restrictive, so that the input
// operator has more ordering choices.
func (c *CustomFuncs) CanSimplifyLimitOffsetOrdering(input memo.GroupID, def memo.PrivateID) bool {
	ordering := c.f.mem.LookupPrivate(def).(*props.OrderingChoice)
	return c.canSimplifyOrdering(input, ordering)
}

// SimplifyLimitOffsetOrdering makes the ordering required by the Limit or
// Offset operator less restrictive by removing optional columns, adding
// equivalent columns, and removing redundant columns.
func (c *CustomFuncs) SimplifyLimitOffsetOrdering(
	input memo.GroupID, def memo.PrivateID,
) memo.PrivateID {
	existing := c.f.mem.LookupPrivate(def).(*props.OrderingChoice)
	simplified := c.simplifyOrdering(input, existing)
	return c.f.mem.InternOrderingChoice(&simplified)
}

// CanSimplifyGroupByOrdering returns true if the ordering required by the
// GroupBy operator can be made less restrictive, so that the input operator has
// more ordering choices.
func (c *CustomFuncs) CanSimplifyGroupByOrdering(input memo.GroupID, def memo.PrivateID) bool {
	groupByDef := c.f.mem.LookupPrivate(def).(*memo.GroupByDef)
	return c.canSimplifyOrdering(input, &groupByDef.Ordering)
}

// SimplifyGroupByOrdering makes the ordering required by the GroupBy operator
// less restrictive by removing optional columns, adding equivalent columns, and
// removing redundant columns.
func (c *CustomFuncs) SimplifyGroupByOrdering(
	input memo.GroupID, def memo.PrivateID,
) memo.PrivateID {
	// Copy GroupByDef to stack and replace Ordering field.
	groupByDef := *c.f.mem.LookupPrivate(def).(*memo.GroupByDef)
	groupByDef.Ordering = c.simplifyOrdering(input, &groupByDef.Ordering)
	return c.f.mem.InternGroupByDef(&groupByDef)
}

// CanSimplifyRowNumberOrdering returns true if the ordering required by the
// RowNumber operator can be made less restrictive, so that the input operator
// has more ordering choices.
func (c *CustomFuncs) CanSimplifyRowNumberOrdering(input memo.GroupID, def memo.PrivateID) bool {
	rowNumberDef := c.f.mem.LookupPrivate(def).(*memo.RowNumberDef)
	return c.canSimplifyOrdering(input, &rowNumberDef.Ordering)
}

// SimplifyRowNumberOrdering makes the ordering required by the RowNumber
// operator less restrictive by removing optional columns, adding equivalent
// columns, and removing redundant columns.
func (c *CustomFuncs) SimplifyRowNumberOrdering(
	input memo.GroupID, def memo.PrivateID,
) memo.PrivateID {
	// Copy RowNumberDef to stack and replace Ordering field.
	rowNumberDef := *c.f.mem.LookupPrivate(def).(*memo.RowNumberDef)
	rowNumberDef.Ordering = c.simplifyOrdering(input, &rowNumberDef.Ordering)
	return c.f.mem.InternRowNumberDef(&rowNumberDef)
}

// CanSimplifyExplainOrdering returns true if the ordering required by the
// Explain operator can be made less restrictive, so that the input operator
// has more ordering choices.
func (c *CustomFuncs) CanSimplifyExplainOrdering(input memo.GroupID, def memo.PrivateID) bool {
	explainDef := c.f.mem.LookupPrivate(def).(*memo.ExplainOpDef)
	return c.canSimplifyOrdering(input, &explainDef.Props.Ordering)
}

// SimplifyExplainOrdering makes the ordering required by the Explain operator
// less restrictive by removing optional columns, adding equivalent columns, and
// removing redundant columns.
func (c *CustomFuncs) SimplifyExplainOrdering(
	input memo.GroupID, def memo.PrivateID,
) memo.PrivateID {
	// Copy ExplainOpDef to stack and replace Ordering field.
	explainDef := *c.f.mem.LookupPrivate(def).(*memo.ExplainOpDef)
	explainDef.Props.Ordering = c.simplifyOrdering(input, &explainDef.Props.Ordering)
	return c.f.mem.InternExplainOpDef(&explainDef)
}

func (c *CustomFuncs) canSimplifyOrdering(input memo.GroupID, ordering *props.OrderingChoice) bool {
	// If any ordering is allowed, nothing to simplify.
	if ordering.Any() {
		return false
	}

	fdset := &c.f.mem.GroupProperties(input).Relational.FuncDeps
	return ordering.CanSimplify(fdset)
}

func (c *CustomFuncs) simplifyOrdering(
	input memo.GroupID, ordering *props.OrderingChoice,
) props.OrderingChoice {
	fdset := &c.f.mem.GroupProperties(input).Relational.FuncDeps
	simplified := ordering.Copy()
	simplified.Simplify(fdset)
	return simplified
}
