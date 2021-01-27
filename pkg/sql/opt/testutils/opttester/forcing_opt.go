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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
)

// forcingOptimizer is a wrapper around an Optimizer which adds low-level
// control, like restricting rule application or the expressions that can be
// part of the final expression.
type forcingOptimizer struct {
	o xform.Optimizer

	groups memoGroups

	coster forcingCoster

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

	// lastAppliedSource is the expression matched by an exploration rule, or is
	// nil for a normalization rule.
	lastAppliedSource opt.Expr

	// lastAppliedTarget is the new expression constructed by a normalization or
	// exploration rule. For an exploration rule, it can be nil if no expressions
	// were constructed, or can have additional expressions beyond the first that
	// are accessible via NextExpr links.
	lastAppliedTarget opt.Expr
}

// newForcingOptimizer creates a forcing optimizer that stops applying any rules
// after <steps> rules are matched. If ignoreNormRules is true, normalization
// rules don't count against this limit. If disableCheckExpr is true, expression
// validation in CheckExpr will not run.
func newForcingOptimizer(
	tester *OptTester, steps int, ignoreNormRules bool, disableCheckExpr bool,
) (*forcingOptimizer, error) {
	fo := &forcingOptimizer{
		remaining:   steps,
		lastMatched: opt.InvalidRuleName,
	}
	fo.o.Init(&tester.evalCtx, tester.catalog)
	fo.o.Factory().FoldingControl().AllowStableFolds()
	fo.coster.Init(&fo.o, &fo.groups)
	fo.o.SetCoster(&fo.coster)

	fo.o.NotifyOnMatchedRule(func(ruleName opt.RuleName) bool {
		if ignoreNormRules && ruleName.IsNormalize() {
			return true
		}
		if fo.remaining == 0 {
			return false
		}
		if tester.Flags.DisableRules.Contains(int(ruleName)) {
			return false
		}
		fo.remaining--
		fo.lastMatched = ruleName
		return true
	})

	// Hook the AppliedRule notification in order to track the portion of the
	// expression tree affected by each transformation rule.
	fo.o.NotifyOnAppliedRule(
		func(ruleName opt.RuleName, source, target opt.Expr) {
			if ignoreNormRules && ruleName.IsNormalize() {
				return
			}
			fo.lastApplied = ruleName
			fo.lastAppliedSource = source
			fo.lastAppliedTarget = target
		},
	)

	fo.o.Memo().NotifyOnNewGroup(func(expr opt.Expr) {
		fo.groups.AddGroup(expr)
	})

	if disableCheckExpr {
		fo.o.Memo().DisableCheckExpr()
	}

	if err := tester.buildExpr(fo.o.Factory()); err != nil {
		return nil, err
	}
	return fo, nil
}

func (fo *forcingOptimizer) Optimize() opt.Expr {
	expr, err := fo.o.Optimize()
	if err != nil {
		// Print the full error (it might contain a stack trace).
		fmt.Printf("%+v\n", err)
		panic(err)
	}
	return expr
}

// LookupPath returns the path of the given node.
func (fo *forcingOptimizer) LookupPath(target opt.Expr) []memoLoc {
	return fo.groups.FindPath(fo.o.Memo().RootExpr(), target)
}

// RestrictToExpr sets up the optimizer to restrict the result to only those
// expression trees which include the given expression path.
func (fo *forcingOptimizer) RestrictToExpr(path []memoLoc) {
	for _, l := range path {
		fo.coster.RestrictGroupToMember(l)
	}
}

// forcingCoster implements the xform.Coster interface so that it can suppress
// expressions in the memo that can't be part of the output tree.
type forcingCoster struct {
	o      *xform.Optimizer
	groups *memoGroups

	inner xform.Coster

	restricted map[groupID]memberOrd
}

func (fc *forcingCoster) Init(o *xform.Optimizer, groups *memoGroups) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*fc = forcingCoster{
		o:      o,
		groups: groups,
		inner:  o.Coster(),
	}
}

// RestrictGroupToMember forces the expression in the given location to be the
// best expression for its group.
func (fc *forcingCoster) RestrictGroupToMember(loc memoLoc) {
	if fc.restricted == nil {
		fc.restricted = make(map[groupID]memberOrd)
	}
	fc.restricted[loc.group] = loc.member
}

// ComputeCost is part of the xform.Coster interface.
func (fc *forcingCoster) ComputeCost(e memo.RelExpr, required *physical.Required) memo.Cost {
	if fc.restricted != nil {
		loc := fc.groups.MemoLoc(e)
		if mIdx, ok := fc.restricted[loc.group]; ok && loc.member != mIdx {
			return memo.MaxCost
		}
	}

	return fc.inner.ComputeCost(e, required)
}
