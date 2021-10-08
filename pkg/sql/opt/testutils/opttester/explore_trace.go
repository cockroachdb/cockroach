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

// exploreTracer implements the stepping algorithm used by the OptTester's
// ExploreTrace command. See the OptTester.ExploreTrace comment for more details
// on the command.
//
// The algorithm is similar to optsteps, with the exception that we always let
// normalization rules pass through; and instead of tracking the best expression
// via diffs, we just show (separately) what each rule application does.
type exploreTracer struct {
	tester *OptTester
	fo     *forcingOptimizer

	srcExpr  opt.Expr
	newExprs []opt.Expr

	// steps is the maximum number of exploration rules that can be applied by the
	// optimizer during the current iteration.
	steps int
}

func newExploreTracer(tester *OptTester) *exploreTracer {
	return &exploreTracer{tester: tester, steps: 1}
}

func (et *exploreTracer) LastRuleName() opt.RuleName {
	return et.fo.lastApplied
}

func (et *exploreTracer) SrcExpr() opt.Expr {
	return et.srcExpr
}

func (et *exploreTracer) NewExprs() []opt.Expr {
	return et.newExprs
}

// Done returns true if there are no more rules to apply. Further calls to the
// next method will result in a panic.
func (et *exploreTracer) Done() bool {
	// remaining starts out equal to steps, and is decremented each time a rule
	// is applied. If it never reaches zero, then all possible rules were
	// already applied, and optimization is complete.
	return et.fo != nil && et.fo.remaining != 0
}

func (et *exploreTracer) Next() error {
	if et.Done() {
		panic("iteration already complete")
	}

	fo, err := newForcingOptimizer(et.tester, et.steps, true /* ignoreNormRules */, false /* disableCheckExpr */)
	if err != nil {
		return err
	}
	et.fo = fo
	fo.Optimize()
	if fo.remaining != 0 {
		return nil
	}

	// Compute the lowest cost tree for the source expression.
	et.srcExpr = et.restrictToExpr(fo.LookupPath(fo.lastAppliedSource))

	// Compute the lowest code tree for any target expressions.
	et.newExprs = et.newExprs[:0]
	if fo.lastAppliedTarget != nil {
		et.newExprs = append(et.newExprs, et.restrictToExpr(fo.LookupPath(fo.lastAppliedTarget)))

		if rel, ok := fo.lastAppliedTarget.(memo.RelExpr); ok {
			for {
				rel = rel.NextExpr()
				if rel == nil {
					break
				}
				et.newExprs = append(et.newExprs, et.restrictToExpr(fo.LookupPath(rel)))
			}
		}
	}

	et.steps++
	return nil
}

func (et *exploreTracer) restrictToExpr(path []memoLoc) opt.Expr {
	fo2, err := newForcingOptimizer(et.tester, et.steps, true /* ignoreNormRules */, false /* disableCheckExpr */)
	if err != nil {
		// We should have already built the query successfully once.
		panic(err)
	}
	fo2.RestrictToExpr(path)
	return fo2.Optimize()
}
