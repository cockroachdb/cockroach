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

	fo *forcingOptimizer

	// steps is the maximum number of rules that can be applied by the optimizer
	// during the current iteration.
	steps int

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
	return os.fo.lastMatched
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
	return os.fo != nil && os.fo.remaining != 0
}

// next triggers the next iteration of optSteps. If there is no error, then
// results of the iteration can be accessed via the exprView, lastRuleName, and
// isBetter methods.
func (os *optSteps) next() error {
	if os.done() {
		panic("iteration already complete")
	}

	fo, err := newForcingOptimizer(os.tester, os.steps)
	if err != nil {
		return err
	}

	os.fo = fo
	os.ev = fo.optimize()
	text := os.ev.String()

	// If the expression text changes, then it must have gotten better.
	os.better = text != os.best
	if os.better {
		os.best = text
	} else if !os.done() {
		// The expression is not better, so suppress the lowest cost expressions
		// so that the changed portions of the tree will be part of the output.
		fo2, err := newForcingOptimizer(os.tester, os.steps)
		if err != nil {
			return err
		}

		if fo.lastApplied.IsNormalize() || fo.lastExploreNewExprs.Empty() {
			// This was a normalization that created a new memo group, or it was
			// an exploration rule that didn't add any expressions to the group.
			// Either way, none of the expressions in the group need to be
			// suppressed.
			fo2.restrictToGroup(fo.o.Memo(), fo.lastAppliedGroup)
		} else {
			// This was an exploration rule that added one or more expressions to
			// an existing group. Suppress all other expressions in the group.
			fo2.restrictToExprs(fo.o.Memo(), fo.lastAppliedGroup, fo.lastExploreNewExprs)
		}
		os.ev = fo2.optimize()
	}

	os.steps++
	return nil
}
