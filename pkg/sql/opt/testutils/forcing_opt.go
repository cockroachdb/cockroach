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

package testutils

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// forcingOptimizer is a wrapper around an Optimizer which adds low-level
// control, like restricting rule application or the expressions that can be
// part of the final expression.
type forcingOptimizer struct {
	o xform.Optimizer

	root     memo.GroupID
	required *props.Physical

	// remaining is the number of "unused" steps remaining.
	remaining int

	// lastMatched records the name of the rule that was most recently matched
	// by the optimizer.
	lastMatched opt.RuleName

	// lastApplied records the name of the rule that was most recently applied by
	// the optimizer. This is not necessarily the same with lastMatched because
	// normalization rules can run in-between the match and the application of an
	// exploration rule.
	lastApplied opt.RuleName

	// lastAppliedGroup is the group where the last rule was applied (for both
	// normalization and exploration rules).
	lastAppliedGroup memo.GroupID

	// The following fields are only valid if lastApplied is an exploration rule:
	//  - lastExploreSourceExpr is the ordinal of the expression on which the last
	//    explore rule ran.
	//  - lastExploreNewExprs is the set of expression ordinals that were added by
	//    the rule (can be empty).
	// All expressions are in lastAppliedGroup.
	lastExploreSourceExpr memo.ExprOrdinal
	lastExploreNewExprs   util.FastIntSet
}

// newForcingOptimizer creates a forcing optimizer that stops applying any rules
// after <steps> rules are matched. If ignoreNormRules is true, normalization
// rules don't count against this limit.
func newForcingOptimizer(
	tester *OptTester, steps int, ignoreNormRules bool,
) (*forcingOptimizer, error) {
	fo := &forcingOptimizer{
		remaining:   steps,
		lastMatched: opt.InvalidRuleName,
	}
	fo.o.Init(&tester.evalCtx)

	fo.o.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		if ignoreNormRules && ruleName.IsNormalize() {
			return true
		}
		if fo.remaining == 0 {
			return false
		}
		fo.remaining--
		fo.lastMatched = ruleName
		return true
	})

	// Hook the AppliedRule notification in order to track the portion of the
	// expression tree affected by each transformation rule.
	fo.o.NotifyOnAppliedRule(
		func(ruleName opt.RuleName, group memo.GroupID, expr memo.ExprOrdinal, added int) {
			if ignoreNormRules && ruleName.IsNormalize() {
				return
			}
			fo.lastApplied = ruleName
			fo.lastAppliedGroup = group

			if ruleName.IsExplore() {
				// This was an exploration rule. Record the expression on which the rule
				// was applied and the set of expressions that were generated.
				fo.lastExploreSourceExpr = expr
				fo.lastExploreNewExprs = util.FastIntSet{}
				if added > 0 {
					exprCount := fo.o.Memo().ExprCount(group)
					fo.lastExploreNewExprs.AddRange(exprCount-added, exprCount-1)
				}
			}
		},
	)

	var err error
	fo.root, fo.required, err = tester.buildExpr(fo.o.Factory())
	if err != nil {
		return nil, err
	}
	return fo, nil
}

func (fo *forcingOptimizer) optimize() memo.ExprView {
	return fo.o.Optimize(fo.root, fo.required)
}

// restrictToExprs sets up the optimizer to restrict the result to only those
// containing one of the given expressions (all in the same group).
//
// mem is the resulting Memo obtained from another instance of forcingOptimizer, with
// the same configuration.
//
// exprs is a set of ExprOrdinals (in the given group).
func (fo *forcingOptimizer) restrictToExprs(
	mem *memo.Memo, group memo.GroupID, exprs util.FastIntSet,
) {
	coster := newForcingCoster(fo.o.Coster())

	restrictToGroup(coster, mem, fo.root, group)

	for e := 0; e < mem.ExprCount(group); e++ {
		if !exprs.Contains(e) {
			coster.SuppressExpr(memo.ExprID{Group: group, Expr: memo.ExprOrdinal(e)})
		}
	}

	fo.o.SetCoster(coster)
}

// restrictToGroup is a convenience variant of restrictToExprs when all
// expressions in a group are to be retained.
func (fo *forcingOptimizer) restrictToGroup(mem *memo.Memo, group memo.GroupID) {
	coster := newForcingCoster(fo.o.Coster())

	restrictToGroup(coster, mem, fo.root, group)
	fo.o.SetCoster(coster)
}

// restrictToGroup walks the memo and adds expressions which need to be
// suppressed to the forcingCoster so that the optimization result must contain
// an expression in the given target group.
//
// restrictToGroup does this by recursively traversing the memo, starting at the
// root group. If a group expression is not an ancestor of the target group,
// then it is suppressed. If it is an ancestor, then restrictExprs recurses on
// any child group that is an ancestor.
//
// Must be called before optimize().
func restrictToGroup(coster *forcingCoster, mem *memo.Memo, group, target memo.GroupID) {
	if group == target {
		return
	}

	for e := 0; e < mem.ExprCount(group); e++ {
		eid := memo.ExprID{Group: group, Expr: memo.ExprOrdinal(e)}
		found := false
		expr := mem.Expr(eid)
		for g := 0; g < expr.ChildCount(); g++ {
			child := expr.ChildGroup(mem, g)
			if isGroupReachable(mem, child, target) {
				restrictToGroup(coster, mem, child, target)
				found = true
			}
		}

		if !found {
			coster.SuppressExpr(eid)
		}
	}
}

// isGroupReachable returns true if the target group can be "reached" from the
// given group; in other words, if the given group is the target group or one of
// its ancestor groups.
func isGroupReachable(mem *memo.Memo, group, target memo.GroupID) bool {
	if group == target {
		return true
	}

	for e := 0; e < mem.ExprCount(group); e++ {
		eid := memo.ExprID{Group: group, Expr: memo.ExprOrdinal(e)}
		expr := mem.Expr(eid)
		for g := 0; g < expr.ChildCount(); g++ {
			if isGroupReachable(mem, expr.ChildGroup(mem, g), target) {
				return true
			}
		}
	}

	return false
}

// forcingCoster implements the xform.Coster interface so that it can suppress
// expressions in the memo that can't be part of the output tree.
type forcingCoster struct {
	inner      xform.Coster
	suppressed map[memo.ExprID]bool
}

func newForcingCoster(inner xform.Coster) *forcingCoster {
	return &forcingCoster{inner: inner, suppressed: make(map[memo.ExprID]bool)}
}

func (fc *forcingCoster) SuppressExpr(eid memo.ExprID) {
	fc.suppressed[eid] = true
}

// ComputeCost is part of the xform.Coster interface.
func (fc *forcingCoster) ComputeCost(candidate *memo.BestExpr, logical *props.Logical) memo.Cost {
	if fc.suppressed[candidate.Expr()] {
		// Suppressed expressions get assigned MaxCost so that they never have
		// the lowest cost.
		return memo.MaxCost
	}
	return fc.inner.ComputeCost(candidate, logical)
}
