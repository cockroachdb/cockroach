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

type customOpt struct {
	o *xform.Optimizer

	root     memo.GroupID
	required *props.Physical

	// remaining is the number of "unused" steps remaining.
	remaining int

	// lastMatched records the name of the rule that was most recently matched
	// by the optimizer.
	lastMatched opt.RuleName

	// lastApplied records the id of the expression that marks the portion of the
	// tree affected by the most recent rule application. All expressions in the
	// same memo group that are < lastApplied.Expr will assigned an infinite cost
	// by the forcingCoster. Therefore, only expressions >= lastApplied.Expr can
	// be in the output expression tree.
	lastApplied memo.ExprID
}

func newCustomOpt(tester *OptTester, steps int) (*customOpt, error) {
	co := &customOpt{
		o:           xform.NewOptimizer(&tester.evalCtx),
		remaining:   steps,
		lastMatched: opt.InvalidRuleName,
		lastApplied: memo.InvalidExprID,
	}

	co.o.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		if co.remaining == 0 {
			return false
		}
		co.remaining--
		co.lastMatched = ruleName
		return true
	})

	// Hook the AppliedRule notification in order to track the portion of the
	// expression tree affected by each transformation rule.
	co.lastApplied = memo.InvalidExprID
	co.o.NotifyOnAppliedRule(func(ruleName opt.RuleName, group memo.GroupID, added int) {
		if added > 0 {
			// This was an exploration rule that added one or more expressions to
			// an existing group. Record the id of the first of those expressions.
			// Previous expressions will be suppressed.
			ord := memo.ExprOrdinal(co.o.Memo().ExprCount(group) - added)
			co.lastApplied = memo.ExprID{Group: group, Expr: ord}
		} else {
			// This was a normalization that created a new memo group, or it was
			// an exploration rule that didn't add any expressions to the group.
			// Either way, none of the expressions in the group need to be
			// suppressed.
			co.lastApplied = memo.MakeNormExprID(group)
		}
	})

	var err error
	co.root, co.required, err = tester.buildExpr(co.o.Factory())
	if err != nil {
		return nil, err
	}
	return co, nil
}

func (co *customOpt) optimize() memo.ExprView {
	return co.o.Optimize(co.root, co.required)
}

// restrictToExprs sets up the optimizer to restrict the result to only those
// containing one of the given expressions (all in the same group).
//
// mem is the resulting Memo obtained from another instance of customOpt, with
// the same configuration.
//
// exprs is a set of ExprOrdinals (in the given group).
func (co *customOpt) restrictToExprs(mem *memo.Memo, group memo.GroupID, exprs util.FastIntSet) {
	coster := newForcingCoster(co.o.Coster())

	restrictToGroup(coster, mem, co.root, group)

	for e := 0; e < mem.ExprCount(group); e++ {
		if !exprs.Contains(e) {
			coster.SuppressExpr(memo.ExprID{Group: group, Expr: memo.ExprOrdinal(e)})
		}
	}

	co.o.SetCoster(coster)
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
