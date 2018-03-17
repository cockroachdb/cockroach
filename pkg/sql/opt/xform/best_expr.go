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

package xform

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
)

// bestExpr references the lowest cost expression in a memo group for a given
// set of required physical properties. This may be any kind of expression,
// including an enforcer expression like Sort. bestExpr also stores the ids of
// its child expressions, which are also the lowest cost expressions in their
// respective groups. Recursively, this allows the best expression tree to be
// efficiently extracted from the memo.
// NOTE: Do not reorder the fields in bestExpr, as they are arranged to
//       minimize its memory footprint.
type bestExpr struct {
	// eid references the lowest cost expression in the group. If op is an
	// enforcer, then eid references the non-enforcer expression that is
	// wrapped by the enforcer (could be several enforcer wrappers).
	eid exprID

	// required is the set of physical properties that must be provided by this
	// lowest cost expression. An expression that cannot provide these properties
	// cannot be referenced by this bestExpr, no matter how low its cost.
	required opt.PhysicalPropsID

	// op is the operator type of the lowest cost expression.
	op opt.Operator

	// initialCount is the number of children that have been added to the
	// initial field. Its value is always <= than opt.MaxOperands.
	initialCount uint8

	// initial is inline storage for the first several children. Most operators
	// have a small, fixed number of children and will never need to allocate
	// the remainder slice.
	initial [opt.MaxOperands]bestExprID

	// remainder stores any additional children once the initial array is full.
	// This will only be used for operators that have a list operand.
	remainder []bestExprID
}

// makeBestExpr constructs a new candidate bestExpr for the given expression,
// with respect to the given physical properties. This will be passed to
// memoGroup.ratchetBestExpr in order to check whether it has a lower cost than
// the current best expression for the group.
func makeBestExpr(op opt.Operator, eid exprID, required opt.PhysicalPropsID) bestExpr {
	return bestExpr{op: op, eid: eid, required: required}
}

// initialized returns true once makeBestExpr has been called to initialize the
// bestExpr.
func (be *bestExpr) initialized() bool {
	return be.required != 0
}

// childCount returns the number of children added to the best expression.
func (be *bestExpr) childCount() int {
	return int(be.initialCount) + len(be.remainder)
}

// child returns the id of the nth child of the best expression. It can be used
// in concert with childCount to iterate over the children:
//   for i := 0; i < be.childCount(); i++ {
//     child := be.child(i)
//   }
func (be *bestExpr) child(nth int) bestExprID {
	if nth < int(be.initialCount) {
		return be.initial[nth]
	}
	return be.remainder[nth-opt.MaxOperands]
}

// addChild adds the identifier of the next child of the best expression. Each
// child should also be the lowest cost expression in its respective group.
func (be *bestExpr) addChild(best bestExprID) {
	if be.initialCount < opt.MaxOperands {
		be.initial[be.initialCount] = best
		be.initialCount++
	} else {
		be.remainder = append(be.remainder, best)
	}
}

// private returns the private value associated with this expression, or nil if
// there is none.
func (be *bestExpr) private(mem *memo) interface{} {
	// Enforcers can wrap expressions with private values, so don't return any
	// wrapped private value.
	if isEnforcerLookup[be.op] {
		return nil
	}
	return mem.lookupExpr(be.eid).private(mem)
}
