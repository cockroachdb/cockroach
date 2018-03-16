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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"golang.org/x/tools/container/intsets"
)

//go:generate optgen -out rule_name.og.go rulenames ../ops/*.opt rules/*.opt

//go:generate stringer -output=rule_name_string.go -type=RuleName rule_name.og.go

// OptimizeSteps is passed to NewOptimizer, and specifies the maximum number
// of normalization and exploration transformations that will be applied by the
// optimizer. This can be used to effectively disable the optimizer (if set to
// zero), to debug the optimizer (by disabling optimizations past a certain
// point), or to limit the running time of the optimizer.
type OptimizeSteps int

const (
	// OptimizeNone instructs the optimizer to suppress all transformations.
	// The unaltered input expression tree will become the output expression
	// tree. This effectively disables the optimizer.
	OptimizeNone = OptimizeSteps(0)

	// OptimizeAll instructs the optimizer to continue applying transformations
	// until the best plan has been found. There is no limit to the number of
	// steps that the optimizer will take to get there.
	OptimizeAll = OptimizeSteps(intsets.MaxInt)
)

// Optimizer transforms an input expression tree into the logically equivalent
// output expression tree with the lowest possible execution cost.
//
// To use the optimizer, construct an input expression tree by invoking
// construction methods on the Optimizer.Factory instance. The factory
// transforms the input expression into its canonical form as part of
// construction. Pass the root of the tree constructed by the factory to the
// Optimize method, along with a set of required physical properties that the
// expression must provide. The optimizer will return an ExprView over the
// output expression tree with the lowest cost.
type Optimizer struct {
	f   *factory
	mem *memo

	// stateMap allocates temporary storage that's used to speed up optimization.
	// This state could be discarded once optimization is complete.
	stateMap map[optStateKey]int
	state    []optState
}

// NewOptimizer constructs an instance of the optimizer. The maxSteps parameter
// limits the number of normalization and exploration transformations that will
// be applied by the optimizer. If maxSteps is zero, then the unaltered input
// expression tree becomes the output expression tree (because no transforms
// are applied).
func NewOptimizer(evalCtx *tree.EvalContext, maxSteps OptimizeSteps) *Optimizer {
	f := newFactory(evalCtx, maxSteps)
	return &Optimizer{f: f, mem: f.mem, stateMap: make(map[optStateKey]int)}
}

// Factory returns a factory interface that the caller uses to construct an
// input expression tree. The root of the resulting tree can be passed to the
// Optimize method in order to find the lowest cost plan.
func (o *Optimizer) Factory() opt.Factory {
	return o.f
}

// LastRuleName returns the last rule that was triggered by the optimizer. This
// is useful for single-stepping through optimizer rule applications.
func (o *Optimizer) LastRuleName() RuleName {
	return o.f.lastRuleName
}

// Optimize returns the expression which satisfies the required physical
// properties at the lowest possible execution cost, but is still logically
// equivalent to the given expression. If there is a cost "tie", then any one
// of the qualifying lowest cost expressions may be selected by the optimizer.
func (o *Optimizer) Optimize(root opt.GroupID, requiredProps *opt.PhysicalProps) ExprView {
	required := o.mem.internPhysicalProps(requiredProps)
	state := o.optimizeGroup(root, required)
	return makeExprView(o.mem, state.best)
}

// optimizeGroup enumerates expression trees rooted in the given memo group and
// finds the expression tree with the lowest cost (i.e. the "best") that
// provides the given required physical properties. Enforcers are added as
// needed to provide the required properties.
//
// The following is a simplified walkthrough of how the optimizer might handle
// the following SQL query:
//
//   SELECT * FROM a WHERE x=1 ORDER BY y
//
// Before the optimizer is invoked, the memo group contains a single normalized
// expression:
//
//   memo
//    ├── 5: (select 1 4)
//    ├── 4: (eq 3 2)
//    ├── 3: (variable x)
//    ├── 2: (const 1)
//    └── 1: (scan a)
//
// Optimization begins at the root of the memo (group #5), and calls
// optimizeGroup with the properties required of that group ("ordering:y").
// optimizeGroup then calls optimizeExpr for the Select expression, which
// checks whether the expression can provide the required properties. Since
// Select is a pass-through operator, it can provide the properties by passing
// through the requirement to its input. Accordingly, optimizeExpr recursively
// invokes optimizeGroup on select's input child (group #1), with the same set
// of required properties.
//
// Now the same set of steps are applied to group #1. However, the Scan
// expression cannot provide the required ordering (say because it's ordered on
// x rather than y). The optimizer must add a Sort enforcer. It does this by
// recursively invoking optimizeGroup on the same group #1, but this time
// without the ordering requirement. The Scan operator is capable of meeting
// these reduced requirements, so it is costed and added as the current lowest
// cost expression (bestExpr) for that group for that set of properties (i.e.
// the empty set).
//
//   memo
//    ├── 5: (select 1 4)
//    ├── 4: (eq 3 2)
//    ├── 3: (variable x)
//    ├── 2: (const 1)
//    └── 1: (scan a)
//         └── "" [cost=100.0]
//              └── best: (scan a)
//
// The recursion pops up a level, and now the Sort enforcer knows its input,
// and so it too can be costed (cost of input + extra cost of sort) and added
// as the best expression for the property set with the ordering requirement.
//
//   memo
//    ├── 5: (select 1 4)
//    ├── 4: (eq 3 2)
//    ├── 3: (variable x)
//    ├── 2: (const 1)
//    └── 1: (scan a)
//         ├── "" [cost=100.0]
//         │    └── best: (scan a)
//         └── "ordering:y" [cost=150.0]
//              └── best: (sort 1)
//
// Recursion pops up another level, and the Select operator now knows its input
// (the Sort of the Scan). It then moves on to its scalar filter child and
// optimizes it and its descendants, which is relatively uninteresting since
// there are no enforcers to consider. Once all children have been optimized,
// the Select operator can now be costed and added as the best expression for
// the ordering requirement. It requires the same ordering requirement from its
// input child (i.e. the scan).
//
//   memo
//    ├── 5: (select 1 4)
//    │    └── "ordering:y" [cost=160.0]
//    │         └── best: (select 1="ordering:y" 4)
//    ├── 4: (eq 3 2)
//    ├── 3: (variable x)
//    ├── 2: (const 1)
//    └── 1: (scan a)
//         ├── "" [cost=100.0]
//         │    └── best: (scan a)
//         └── "ordering:y" [cost=150.0]
//              └── best: (sort 1)
//
// But the process is not yet complete. After traversing the Select child
// groups, optimizeExpr generates an alternate plan that satisfies the ordering
// property by using a top-level enforcer. It does this by recursively invoking
// optimizeGroup for group #5, but without the ordering requirement, analogous
// to what it did for group #1. This triggers optimization for each child group
// of the Select operator. But this time, the memo already has fully-costed
// best expressions available for both the Input and Filter children, and so
// returns them immediately with no extra work. The Select expression is now
// costed and added as the best expression without an ordering requirement.
//
//   memo
//    ├── 5: (select 1 4)
//    │    ├── "" [cost=110.0]
//    │    │    └── best: (select 1 4)
//    │    └── "ordering:y" [cost=160.0]
//    │         └── best: (select 1="ordering:y" 4)
//    ├── 4: (eq 3 2)
//    ├── 3: (variable x)
//    ├── 2: (const 1)
//    └── 1: (scan a)
//         ├── "" [cost=100.0]
//         │    └── best: (scan a)
//         └── "ordering:y" [cost=150.0]
//              └── best: (sort 1)
//
// Finally, the Sort enforcer for group #5 has its input and can be costed. But
// rather than costing 50.0 like the other Sort enforcer, this one only costs
// 1.0, because it's sorting a tiny set of filtered rows. That means its total
// cost is only 111.0, which makes it the new best expression for group #5 with
// an ordering requirement:
//
//   memo
//    ├── 5: (select 1 4)
//    │    ├── "" [cost=110.0]
//    │    │    └── best: (select 1 4)
//    │    └── "ordering:y" [cost=111.0]
//    │         └── best: (sort 5)
//    ├── 4: (eq 3 2)
//    ├── 3: (variable x)
//    ├── 2: (const 1)
//    └── 1: (scan a)
//         ├── "" [cost=100.0]
//         │    └── best: (scan a)
//         └── "ordering:y" [cost=150.0]
//              └── best: (sort 1)
//
// Now the memo has been fully optimized, and the best expression for group #5
// and "ordering:y" can be recursively extracted by ExprView:
//
//   sort
//    ├── columns: x:1(int) y:2(int)
//    ├── ordering: +2
//    └── select
//         ├── columns: x:1(int) y:2(int)
//         ├── scan
//         │    └── columns: x:1(int) y:2(int)
//         └── eq [type=bool]
//              ├── variable: a.x [type=int]
//              └── const: 1 [type=int]
//
func (o *Optimizer) optimizeGroup(group opt.GroupID, required opt.PhysicalPropsID) *optState {
	// If this group is already fully optimized, then return the already
	// prepared best expression (won't ever get better than this).
	state := o.ensureOptState(group, required)
	if state.fullyOptimized {
		return state
	}

	groupFullyOptimized := true

	mgrp := o.mem.lookupGroup(group)
	for i := 0; i < mgrp.exprCount(); i++ {
		eid := exprID{group: group, expr: exprOrdinal(i)}

		// If this expression has already been fully optimized for the given
		// required properties, then skip it, since it won't get better.
		if state.isExprFullyOptimized(eid) {
			continue
		}

		// Optimize the expression with respect to the required properties.
		state = o.optimizeExpr(eid, required)

		// If any of the expressions have not yet been fully optimized, then
		// the group is not yet fully optimized.
		if !state.isExprFullyOptimized(eid) {
			groupFullyOptimized = false
		}
	}

	if groupFullyOptimized {
		// If exploration and costing of this group for the given required
		// properties is complete, then skip it in all future optimization
		// passes.
		state.fullyOptimized = true
	}

	return state
}

// optimizeExpr determines whether the given expression can provide the
// required properties. If so, it recursively optimizes the expression's child
// groups and computes the cost of the expression. In addition, optimizeExpr
// calls enforceProps to check whether enforcers can provide the required
// properties at a lower cost. The lowest cost expression is saved to the memo
// group.
func (o *Optimizer) optimizeExpr(eid exprID, required opt.PhysicalPropsID) *optState {
	fullyOptimized := true

	// If the expression cannot provide the required properties, then don't
	// continue. But what if the expression is able to provide a subset of the
	// properties? That case is taken care of by enforceProps, which will
	// recursively optimize the group with property subsets and then add
	// enforcers to provide the remainder.
	physPropsFactory := &physicalPropsFactory{mem: o.mem}
	if physPropsFactory.canProvide(eid, required) {
		e := o.mem.lookupExpr(eid)
		candidateBest := makeBestExpr(e.op, eid, required)
		for child := 0; child < e.childCount(); child++ {
			childGroup := e.childGroup(o.mem, child)

			// Given required parent properties, get the properties required from
			// the nth child.
			childRequired := physPropsFactory.constructChildProps(eid, required, child)

			// Recursively optimize the child group with respect to that set of
			// properties.
			childState := o.optimizeGroup(childGroup, childRequired)

			// Remember best child in BestExpr in case this becomes the lowest
			// cost expression.
			candidateBest.addChild(childState.best)

			// If any child expression is not fully optimized, then the parent
			// expression is also not fully optimized.
			if !childState.fullyOptimized {
				fullyOptimized = false
			}
		}

		// Check whether this is the new lowest cost expression.
		o.ratchetCost(eid.group, &candidateBest)
	}

	// Compute the cost for enforcers to provide the required properties. This
	// may be lower than the expression providing the properties itself. For
	// example, it might be better to sort the results of a hash join than to
	// use the results of a merge join that are already sorted, but at the cost
	// of requiring one of the merge join children to be sorted.
	fullyOptimized = o.enforceProps(eid, required) && fullyOptimized

	// Get the lowest cost expression after considering enforcers, and mark it
	// as fully optimized if all the combinations have been considered.
	state := o.lookupOptState(eid.group, required)
	if fullyOptimized {
		state.markExprAsFullyOptimized(eid)
	}
	return state
}

// enforceProps costs an expression where one of the physical properties has
// been provided by an enforcer rather than by the expression itself. There are
// two reasons why this is necessary/desirable:
//   1. The expression may not be able to provide the property on its own. For
//      example, a hash join cannot provide ordered results.
//   2. The enforcer might be able to provide the property at lower overall
//      cost. For example, an enforced sort on top of a hash join might be
//      lower cost than a merge join that is already sorted, but at the cost of
//      requiring one of its children to be sorted.
//
// Note that enforceProps will recursively optimize this same group, but with
// one less required physical property. The recursive call will eventually make
// its way back here, at which point another physical property will be stripped
// off, and so on. Afterwards, the group will have computed a bestExpr for each
// sublist of physical properties, from all down to none.
func (o *Optimizer) enforceProps(eid exprID, required opt.PhysicalPropsID) (fullyOptimized bool) {
	props := o.mem.lookupPhysicalProps(required)
	innerProps := *props

	// Ignore the Presentation property, since any relational or enforcer
	// operator can provide it.
	innerProps.Presentation = nil

	// Strip off one property that can be enforced. Other properties will be
	// stripped by recursively optimizing the group with successively fewer
	// properties. The properties are stripped off in a heuristic order, from
	// least likely to be expensive to enforce to most likely.
	var enforcerOp opt.Operator
	if props.Ordering.Defined() {
		enforcerOp = opt.SortOp
		innerProps.Ordering = nil
	} else {
		// No remaining properties, so no more enforcers.
		if innerProps.Defined() {
			panic(fmt.Sprintf("unhandled physical property: %v", innerProps))
		}
		return
	}

	// Recursively optimize the same group, but now with respect to the "inner"
	// properties (which are a sublist of the required properties).
	innerRequired := o.mem.internPhysicalProps(&innerProps)
	innerState := o.optimizeGroup(eid.group, innerRequired)
	fullyOptimized = innerState.fullyOptimized

	// Check whether this is the new lowest cost expression with the enforcer
	// added.
	candidateBest := makeBestExpr(enforcerOp, eid, required)
	candidateBest.addChild(innerState.best)
	o.ratchetCost(eid.group, &candidateBest)

	// Enforcer expression is fully optimized if its input expression is fully
	// optimized.
	return fullyOptimized
}

// ratchetCost computes the cost of the candidate expression, and then checks
// whether it's lower than the cost of the existing best expression in the
// group. If so, then the candidate becomes the new lowest cost expression.
// TODO(andyk): Actually run the costing function in the future.
func (o *Optimizer) ratchetCost(group opt.GroupID, candidate *bestExpr) {
	best := o.lookupOptState(group, candidate.required).best
	o.mem.ratchetBestExpr(best, candidate)
}

// lookupOptState looks up the state associated with the given group and
// properties. If no state exists yet, then lookupOptState returns nil.
// NOTE: The returned optState reference is only valid until the next call to
//       ensureOptState, since it may cause a resize of the optState slice.
func (o *Optimizer) lookupOptState(group opt.GroupID, required opt.PhysicalPropsID) *optState {
	index, ok := o.stateMap[optStateKey{group: group, required: required}]
	if !ok {
		return nil
	}
	return &o.state[index]
}

// ensureOptState looks up the state associated with the given group and
// properties. If none is associated yet, then ensureOptState allocates new
// state and returns it.
func (o *Optimizer) ensureOptState(group opt.GroupID, required opt.PhysicalPropsID) *optState {
	key := optStateKey{group: group, required: required}
	index, ok := o.stateMap[key]
	if !ok {
		mgrp := o.mem.lookupGroup(group)
		best := mgrp.ensureBestExpr(required)
		index = len(o.state)
		o.state = append(o.state, optState{best: best})
		o.stateMap[key] = index
	}
	return &o.state[index]
}

// optStateKey associates optState with a group that is being optimized with
// respect to a set of physical properties.
type optStateKey struct {
	group    opt.GroupID
	required opt.PhysicalPropsID
}

// optState is temporary storage that's associated with each group that's
// optimized (or same group with different sets of physical properties). The
// optimizer stores various flags and other state here that allows it to do
// quicker lookups and short-circuit already traversed parts of the expression
// tree.
type optState struct {
	// best identifies the lowest cost expression in the memo group for a given
	// set of physical properties.
	best bestExprID

	// fullyOptimized is set to true once the lowest cost expression has been
	// found for a memo group, with respect to the required properties. A lower
	// cost expression will never be found, no matter how many additional
	// optimization passes are made.
	fullyOptimized bool

	// fullyOptimizedExprs contains the set of expression ids (exprIDs) that
	// have been fully optimized for the required properties. These never need
	// to be recosted, no matter how many additional optimization passes are
	// made.
	fullyOptimizedExprs util.FastIntSet
}

// isExprFullyOptimized returns true if the given expression has been fully
// optimized for the required properties. The expression never needs to be
// recosted, no matter how many additional optimization passes are made.
func (os *optState) isExprFullyOptimized(eid exprID) bool {
	return os.fullyOptimizedExprs.Contains(int(eid.expr))
}

// markExprAsFullyOptimized marks the given expression as fully optimized for
// the required properties. The expression never needs to be recosted, no
// matter how many additional optimization passes are made.
func (os *optState) markExprAsFullyOptimized(eid exprID) {
	if os.fullyOptimized {
		panic("best expression is already fully optimized")
	}
	if os.isExprFullyOptimized(eid) {
		panic("memo expression is already fully optimized for required physical properties")
	}
	os.fullyOptimizedExprs.Add(int(eid.expr))
}
