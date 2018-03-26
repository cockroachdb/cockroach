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

package memo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
)

// BestExprID uniquely identifies a BestExpr stored in the memo by pairing the
// ID of its group with the ordinal position of the BestExpr within that group.
type BestExprID struct {
	group   GroupID
	ordinal bestOrdinal
}

// UnknownBestExprID is the uninitialized BestExprID.
var UnknownBestExprID = BestExprID{}

// BestExpr references the lowest cost expression in a memo group for a given
// set of required physical properties. This may be any kind of expression,
// including an enforcer expression like Sort. BestExpr also stores the ids of
// its child expressions, which are also the lowest cost expressions in their
// respective groups. Recursively, this allows the best expression tree to be
// efficiently extracted from the memo.
// NOTE: Do not reorder the fields in BestExpr, as they are arranged to
//       minimize its memory footprint.
type BestExpr struct {
	// eid references the lowest cost expression in the group. If op is an
	// enforcer, then eid references the non-enforcer expression that is
	// wrapped by the enforcer (could be several enforcer wrappers).
	eid ExprID

	// required is the set of physical properties that must be provided by this
	// lowest cost expression. An expression that cannot provide these properties
	// cannot be the best expression, no matter how low its cost.
	required PhysicalPropsID

	// op is the operator type of the lowest cost expression.
	op opt.Operator

	// initialCount is the number of children that have been added to the
	// initial field. Its value is always <= than opt.MaxOperands.
	initialCount uint8

	// initial is inline storage for the first several children. Most operators
	// have a small, fixed number of children and will never need to allocate
	// the remainder slice.
	initial [opt.MaxOperands]BestExprID

	// remainder stores any additional children once the initial array is full.
	// This will only be used for operators that have a list operand.
	remainder []BestExprID

	// cost is the estimated execution cost for this expression. The best
	// expression for a given group and set of physical properties is the
	// expression with the lowest cost.
	cost Cost
}

// MakeBestExpr constructs a new candidate BestExpr for the given expression,
// with respect to the given physical properties. This will be passed to
// group.RatchetBestExpr in order to check whether it has a lower cost than
// the current best expression for the group.
func MakeBestExpr(op opt.Operator, eid ExprID, required PhysicalPropsID) BestExpr {
	return BestExpr{op: op, eid: eid, required: required, cost: MaxCost}
}

// Operator returns the type of the expression.
func (be *BestExpr) Operator() opt.Operator {
	return be.op
}

// Group returns the memo group which contains this best expression.
func (be *BestExpr) Group() GroupID {
	return be.eid.Group
}

// Required is the set of physical properties that must be provided by this
// lowest cost expression. An expression that cannot provide these properties
// cannot be the best expression, no matter how low its cost.
func (be *BestExpr) Required() PhysicalPropsID {
	return be.required
}

// SetCost updates the cost of the best expression. This is used by the coster
// to set the cost of a candidate best expression.
func (be *BestExpr) SetCost(cost Cost) {
	if be.cost != MaxCost {
		panic("SetCost should only be used once to set the cost")
	}
	be.cost = cost
}

// ChildCount returns the number of children added to the best expression.
func (be *BestExpr) ChildCount() int {
	return int(be.initialCount) + len(be.remainder)
}

// Child returns the id of the nth child of the best expression. It can be used
// in concert with ChildCount to iterate over the children:
//   for i := 0; i < be.ChildCount(); i++ {
//     child := be.child(i)
//   }
func (be *BestExpr) Child(nth int) BestExprID {
	if nth < int(be.initialCount) {
		return be.initial[nth]
	}

	// Remainder slice is only used if there are more operands than
	// opt.MaxOperands.
	return be.remainder[nth-opt.MaxOperands]
}

// AddChild adds the identifier of the next child of the best expression. Each
// child should also be the lowest cost expression in its respective group.
func (be *BestExpr) AddChild(best BestExprID) {
	if be.initialCount < opt.MaxOperands {
		be.initial[be.initialCount] = best
		be.initialCount++
	} else {
		be.remainder = append(be.remainder, best)
	}
}

// Private returns the private value associated with this expression, or nil if
// there is none.
func (be *BestExpr) Private(mem *Memo) interface{} {
	// Don't return private values from expressions wrapped by enforcers.
	if isEnforcerLookup[be.op] {
		return nil
	}
	return mem.Expr(be.eid).Private(mem)
}

// initialized returns true once MakeBestExpr has been called to initialize the
// BestExpr.
func (be *BestExpr) initialized() bool {
	return be.required != 0
}

// ratchetCost overwrites this expression with the candidate expression if it
// has a lower cost.
func (be *BestExpr) ratchetCost(candidate *BestExpr) {
	if candidate.cost.Less(be.cost) {
		*be = *candidate
	}
}
