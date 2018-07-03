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
	"github.com/cockroachdb/cockroach/pkg/util"
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

	fo *forcingOptimizer

	srcExpr  memo.ExprView
	newExprs []memo.ExprView

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

func (et *exploreTracer) SrcExpr() memo.ExprView {
	return et.srcExpr
}

func (et *exploreTracer) NewExprs() []memo.ExprView {
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

	fo, err := newForcingOptimizer(et.tester, et.steps, true /* ignoreNormRules */)
	if err != nil {
		return err
	}
	et.fo = fo
	fo.optimize()
	if fo.remaining != 0 {
		return nil
	}

	et.srcExpr = et.lastAppliedGroupExpr(fo.lastExploreSourceExpr)
	et.newExprs = make([]memo.ExprView, fo.lastExploreNewExprs.Len())
	for i, expr := range fo.lastExploreNewExprs.Ordered() {
		et.newExprs[i] = et.lastAppliedGroupExpr(memo.ExprOrdinal(expr))
	}
	et.steps++
	return nil
}

func (et *exploreTracer) lastAppliedGroupExpr(expr memo.ExprOrdinal) memo.ExprView {
	fo2, err := newForcingOptimizer(et.tester, et.steps, true /* ignoreNormRules */)
	if err != nil {
		// We should have already built the query successfully once.
		panic(err)
	}
	fo2.restrictToExprs(et.fo.o.Memo(), et.fo.lastAppliedGroup, util.MakeFastIntSet(int(expr)))
	return fo2.optimize()
}
