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
)

// optSteps implements the stepping algorithm used by the OptTester's OptSteps
// command. See the OptTester.OptSteps comment for more details on the command.
//
// The algorithm works as follows:
//   1. The first time optSteps.next() is called, optSteps returns the starting
//      expression tree, with no transformations applied to it.
//
//   2. Each optSteps.next() call after that will perform N+1 transformations,
//      where N is the the number of steps performed during the previous call
//      (starting at 0 with the first call).
//
//   3. Each optSteps.next() call will build the expression tree from scratch
//      and re-run all transformations that were run in the previous call, plus
//      one additional transformation (N+1). Therefore, the output expression
//      tree from each call will differ from the previous call only by the last
//      transformation's changes.
//
//   4. optSteps hooks the optimizer's MatchedRule event in order to limit the
//      number of transformations that can be applied, as well as to record the
//      name of the last rule that was applied, for later output.
//
//   5. While this works well for normalization rules, exploration rules are
//      more difficult. This is because exploration rules are not guaranteed to
//      produce a lower cost tree. Unless extra measures are taken, the returned
//      ExprView would not include the changed portion of the Memo, since
//      ExprView only shows the lowest cost path through the Memo.
//
//   6. To address this issue, optSteps hooks the optimizer's AppliedRule event
//      and records the expression(s) that the last transformation has affected.
//      It then re-runs the optimizer, but this time using a special Coster
//      implementation that fools the optimizer into thinking that the new
//      expression(s) have the lowest cost. The coster does this by assigning an
//      infinite cost to all other expressions in the same group as the new
//      expression(s), as well as in all ancestor groups.
//
type optSteps struct {
	tester *OptTester

	// steps is the maximum number of rules that can be applied by the optimizer
	// during the current iteration.
	steps int

	// remaining is the number of "unused" steps remaining in this iteration.
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

	// ev is the expression tree produced by the most recent optSteps iteration.
	ev memo.ExprView

	// better is true if ev is lower cost than the expression tree produced by
	// the previous iteration of optSteps.
	better bool

	// best is the textual representation of the most recent expression tree that
	// was an improvement over the previous best tree.
	best string
}

func newOptSteps(tester *OptTester) *optSteps {
	return &optSteps{tester: tester}
}

// exprView returns the expression tree produced by the most recent optSteps
// iteration.
func (os *optSteps) exprView() memo.ExprView {
	return os.ev
}

// lastRuleName returns the name of the rule that was most recently matched by
// the optimizer.
func (os *optSteps) lastRuleName() opt.RuleName {
	return os.lastMatched
}

// isBetter returns true if exprView is lower cost than the expression tree
// produced by the previous iteration of optSteps.
func (os *optSteps) isBetter() bool {
	return os.better
}

// done returns true if there are no more rules to apply. Further calls to the
// next method will result in a panic.
func (os *optSteps) done() bool {
	// remaining starts out equal to steps, and is decremented each time a rule
	// is applied. If it never reaches zero, then all possible rules were
	// already applied, and optimization is complete.
	return os.remaining != 0
}

// next triggers the next iteration of optSteps. If there is no error, then
// results of the iteration can be accessed via the exprView, lastRuleName, and
// isBetter methods.
func (os *optSteps) next() error {
	if os.done() {
		panic("iteration already complete")
	}

	// Create optimizer that will run for a fixed number of steps.
	o := os.createOptimizer(os.steps)
	root, required, err := os.tester.buildExpr(o.Factory())
	if err != nil {
		return err
	}

	// Hook the AppliedRule notification in order to track the portion of the
	// expression tree affected by each transformation rule.
	os.lastApplied = memo.InvalidExprID
	o.NotifyOnAppliedRule(func(ruleName opt.RuleName, group memo.GroupID, added int) {
		if added > 0 {
			// This was an exploration rule that added one or more expressions to
			// an existing group. Record the id of the first of those expressions.
			// Previous expressions will be suppressed.
			ord := memo.ExprOrdinal(o.Memo().ExprCount(group) - added)
			os.lastApplied = memo.ExprID{Group: group, Expr: ord}
		} else {
			// This was a normalization that created a new memo group, or it was
			// an exploration rule that didn't add any expressions to the group.
			// Either way, none of the expressions in the group need to be
			// suppressed.
			os.lastApplied = memo.MakeNormExprID(group)
		}
	})

	os.ev = o.Optimize(root, required)
	text := os.ev.String()

	// If the expression text changes, then it must have gotten better.
	os.better = text != os.best
	if os.better {
		os.best = text
	} else if !os.done() {
		// The expression is not better, so suppress the lowest cost expressions
		// so that the changed portions of the tree will be part of the output.
		o2 := os.createOptimizer(os.steps)
		root, required, err := os.tester.buildExpr(o2.Factory())
		if err != nil {
			return err
		}

		// Set up the coster that will assign infinite costs to the expressions
		// that need to be suppressed.
		coster := newForcingCoster(o2.Coster())
		os.suppressExprs(coster, o.Memo(), root)

		o2.SetCoster(coster)
		os.ev = o2.Optimize(root, required)
	}

	os.steps++
	return nil
}

func (os *optSteps) createOptimizer(steps int) *xform.Optimizer {
	o := xform.NewOptimizer(&os.tester.evalCtx)

	// Override NotifyOnMatchedRule to stop optimizing after "steps" rule matches.
	os.remaining = steps
	o.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		if os.remaining == 0 {
			return false
		}
		os.remaining--
		os.lastMatched = ruleName
		return true
	})

	return o
}

// suppressExprs walks the tree and adds expressions which need to be suppressed
// to the forcingCoster. The expressions which need to suppressed are:
//   1. Expressions in the same group as the lastApplied expression, but with
//      a lower ordinal position in the group.
//   2. Expressions in ancestor groups of the lastApplied expression that are
//      not themselves ancestors of the lastApplied group.
//
// suppressExprs does this by recursively traversing the memo, starting at the
// root group. If a group expression is not an ancestor of the last applied
// group, then it is suppressed. If it is an ancestor, then suppressExprs
// recurses on any child group that is an ancestor.
func (os *optSteps) suppressExprs(coster *forcingCoster, mem *memo.Memo, group memo.GroupID) {
	for e := 0; e < mem.ExprCount(group); e++ {
		eid := memo.ExprID{Group: group, Expr: memo.ExprOrdinal(e)}
		if eid.Group == os.lastApplied.Group {
			if eid.Expr < os.lastApplied.Expr {
				coster.SuppressExpr(eid)
			}
			continue
		}

		found := false
		expr := mem.Expr(eid)
		for g := 0; g < expr.ChildCount(); g++ {
			child := expr.ChildGroup(mem, g)
			if os.findLastApplied(mem, child) {
				os.suppressExprs(coster, mem, child)
				found = true
				break
			}
		}

		if !found {
			coster.SuppressExpr(eid)
		}
	}
}

// findLastApplied returns true if the given group is the last applied group or
// one of its ancestor groups.
func (os *optSteps) findLastApplied(mem *memo.Memo, group memo.GroupID) bool {
	if group == os.lastApplied.Group {
		return true
	}

	for e := 0; e < mem.ExprCount(group); e++ {
		eid := memo.ExprID{Group: group, Expr: memo.ExprOrdinal(e)}
		expr := mem.Expr(eid)
		for g := 0; g < expr.ChildCount(); g++ {
			if os.findLastApplied(mem, expr.ChildGroup(mem, g)) {
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
