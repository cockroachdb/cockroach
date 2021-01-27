// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opttester

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
//      where N is the number of steps performed during the previous call
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
//      Expr would not include the changed portion of the Memo, since Expr only
//      shows the lowest cost path through the Memo.
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

	// expr is the expression tree produced by the most recent optSteps iteration.
	expr opt.Expr

	// better is true if expr is lower cost than the expression tree produced by
	// the previous iteration of optSteps.
	better bool

	// best is the textual representation of the most recent expression tree that
	// was an improvement over the previous best tree.
	best string
}

func newOptSteps(tester *OptTester) *optSteps {
	return &optSteps{tester: tester}
}

// Root returns the node tree produced by the most recent optSteps iteration.
func (os *optSteps) Root() opt.Expr {
	return os.expr
}

// LastRuleName returns the name of the rule that was most recently matched by
// the optimizer.
func (os *optSteps) LastRuleName() opt.RuleName {
	return os.fo.lastMatched
}

// IsBetter returns true if root is lower cost than the expression tree
// produced by the previous iteration of optSteps.
func (os *optSteps) IsBetter() bool {
	return os.better
}

// Done returns true if there are no more rules to apply. Further calls to the
// next method will result in a panic.
func (os *optSteps) Done() bool {
	// remaining starts out equal to steps, and is decremented each time a rule
	// is applied. If it never reaches zero, then all possible rules were
	// already applied, and optimization is complete.
	return os.fo != nil && os.fo.remaining != 0
}

// Next triggers the next iteration of optSteps. If there is no error, then
// results of the iteration can be accessed via the Root, LastRuleName, and
// IsBetter methods.
func (os *optSteps) Next() error {
	if os.Done() {
		panic("iteration already complete")
	}

	fo, err := newForcingOptimizer(os.tester, os.steps, false /* ignoreNormRules */, true /* disableCheckExpr */)
	if err != nil {
		return err
	}

	os.fo = fo
	os.expr = fo.Optimize()
	text := os.expr.String()

	// If the expression text changes, then it must have gotten better.
	os.better = text != os.best
	if os.better {
		os.best = text
	} else if !os.Done() {
		// The expression is not better, so suppress the lowest cost expressions
		// so that the changed portions of the tree will be part of the output.
		fo2, err := newForcingOptimizer(os.tester, os.steps, false /* ignoreNormRules */, true /* disableCheckExpr */)
		if err != nil {
			return err
		}

		if fo.lastAppliedSource == nil {
			// This was a normalization that created a new memo group.
			fo2.RestrictToExpr(fo.LookupPath(fo.lastAppliedTarget))
		} else if fo.lastAppliedTarget == nil {
			// This was an exploration rule that didn't add any expressions to the
			// group, so only ancestor groups need to be suppressed.
			path := fo.LookupPath(fo.lastAppliedSource)
			fo2.RestrictToExpr(path[:len(path)-1])
		} else {
			// This was an exploration rule that added one or more expressions to
			// an existing group. Suppress all other members of the group.
			member := fo.lastAppliedTarget.(memo.RelExpr)
			for member != nil {
				fo2.RestrictToExpr(fo.LookupPath(member))
				member = member.NextExpr()
			}
		}
		os.expr = fo2.Optimize()
	}

	os.steps++
	return nil
}
