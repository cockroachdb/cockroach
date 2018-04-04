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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
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
	evalCtx  *tree.EvalContext
	f        *norm.Factory
	mem      *memo.Memo
	coster   coster
	explorer explorer

	// stateMap allocates temporary storage that's used to speed up optimization.
	// This state could be discarded once optimization is complete.
	stateMap   map[optStateKey]*optState
	stateAlloc optStateAlloc

	// onRuleMatch is the callback function that is invoked each time a normalize
	// rule has been matched by the factory. It can be set via a call to the
	// SetOnRuleMatch method.
	onRuleMatch func(ruleName opt.RuleName) bool
}

// NewOptimizer constructs an instance of the optimizer.
func NewOptimizer(evalCtx *tree.EvalContext) *Optimizer {
	f := norm.NewFactory(evalCtx)
	o := &Optimizer{
		evalCtx:  evalCtx,
		f:        f,
		mem:      f.Memo(),
		stateMap: make(map[optStateKey]*optState),
	}
	o.coster.init(o.mem)
	o.explorer.init(o)
	return o
}

// Factory returns a factory interface that the caller uses to construct an
// input expression tree. The root of the resulting tree can be passed to the
// Optimize method in order to find the lowest cost plan.
func (o *Optimizer) Factory() *norm.Factory {
	return o.f
}

// DisableOptimizations disables all transformation rules, including normalize
// and explore rules. The unaltered input expression tree becomes the output
// expression tree (because no transforms are applied).
func (o *Optimizer) DisableOptimizations() {
	o.SetOnRuleMatch(func(opt.RuleName) bool { return false })
}

// SetOnRuleMatch sets a callback function which is invoked each time an
// optimization rule (Normalize or Explore rule) has been matched by the
// optimizer. If the function returns false, then the rule is not applied. By
// default, all rules are applied, but callers can set the callback function to
// override the default behavior. In addition, callers can invoke the
// DisableOptimizations convenience method to disable all rules.
func (o *Optimizer) SetOnRuleMatch(onRuleMatch func(ruleName opt.RuleName) bool) {
	o.onRuleMatch = onRuleMatch

	// Also pass through the call to the factory so that normalization rules
	// make same callback.
	o.f.SetOnRuleMatch(onRuleMatch)
}

// Memo returns the memo structure that the optimizer is using to optimize.
func (o *Optimizer) Memo() *memo.Memo {
	return o.mem
}

// Optimize returns the expression which satisfies the required physical
// properties at the lowest possible execution cost, but is still logically
// equivalent to the given expression. If there is a cost "tie", then any one
// of the qualifying lowest cost expressions may be selected by the optimizer.
func (o *Optimizer) Optimize(root memo.GroupID, requiredProps *memo.PhysicalProps) memo.ExprView {
	required := o.mem.InternPhysicalProps(requiredProps)
	state := o.optimizeGroup(root, required)
	return memo.MakeExprView(o.mem, state.best)
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
// cost expression (BestExpr) for that group for that set of properties (i.e.
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
func (o *Optimizer) optimizeGroup(group memo.GroupID, required memo.PhysicalPropsID) *optState {
	// If this group is already fully optimized, then return the already
	// prepared best expression (won't ever get better than this).
	state := o.ensureOptState(group, required)
	if state.fullyOptimized {
		return state
	}

	// Iterate until the group has been fully optimized.
	for {
		fullyOptimized := true

		for i := 0; i < o.mem.ExprCount(group); i++ {
			eid := memo.ExprID{Group: group, Expr: memo.ExprOrdinal(i)}

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
				fullyOptimized = false
			}
		}

		// Now generate new expressions that are logically equivalent to other
		// expressions in this group.
		if !o.explorer.exploreGroup(group).fullyExplored {
			fullyOptimized = false
		}

		if fullyOptimized {
			// If exploration and costing of this group for the given required
			// properties is complete, then skip it in all future optimization
			// passes.
			state.fullyOptimized = true
			break
		}
	}

	return state
}

// optimizeExpr determines whether the given expression can provide the
// required properties. If so, it recursively optimizes the expression's child
// groups and computes the cost of the expression. In addition, optimizeExpr
// calls enforceProps to check whether enforcers can provide the required
// properties at a lower cost. The lowest cost expression is saved to the memo
// group.
func (o *Optimizer) optimizeExpr(eid memo.ExprID, required memo.PhysicalPropsID) *optState {
	// Compute the cost for enforcers to provide the required properties. This
	// may be lower than the expression providing the properties itself. For
	// example, it might be better to sort the results of a hash join than to
	// use the results of a merge join that are already sorted, but at the cost
	// of requiring one of the merge join children to be sorted.
	fullyOptimized := o.enforceProps(eid, required)

	// If the expression cannot provide the required properties, then don't
	// continue. But what if the expression is able to provide a subset of the
	// properties? That case is taken care of by enforceProps, which will
	// recursively optimize the group with property subsets and then add
	// enforcers to provide the remainder.
	physPropsFactory := &physicalPropsFactory{mem: o.mem}
	if physPropsFactory.canProvide(eid, required) {
		e := o.mem.Expr(eid)
		candidateBest := memo.MakeBestExpr(e.Operator(), eid, required)
		for child := 0; child < e.ChildCount(); child++ {
			childGroup := e.ChildGroup(o.mem, child)

			// Given required parent properties, get the properties required from
			// the nth child.
			childRequired := physPropsFactory.constructChildProps(eid, required, child)

			// Recursively optimize the child group with respect to that set of
			// properties.
			childState := o.optimizeGroup(childGroup, childRequired)

			// Remember best child in BestExpr in case this becomes the lowest
			// cost expression.
			candidateBest.AddChild(childState.best)

			// If any child expression is not fully optimized, then the parent
			// expression is also not fully optimized.
			if !childState.fullyOptimized {
				fullyOptimized = false
			}
		}

		// Check whether this is the new lowest cost expression.
		o.ratchetCost(&candidateBest)
	}

	// Get the lowest cost expression after considering enforcers, and mark it
	// as fully optimized if all the combinations have been considered.
	state := o.lookupOptState(eid.Group, required)
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
// off, and so on. Afterwards, the group will have computed a BestExpr for each
// sublist of physical properties, from all down to none.
func (o *Optimizer) enforceProps(
	eid memo.ExprID, required memo.PhysicalPropsID,
) (fullyOptimized bool) {
	props := o.mem.LookupPhysicalProps(required)
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
		return true
	}

	// Recursively optimize the same group, but now with respect to the "inner"
	// properties (which are a sublist of the required properties).
	innerRequired := o.mem.InternPhysicalProps(&innerProps)
	innerState := o.optimizeGroup(eid.Group, innerRequired)
	fullyOptimized = innerState.fullyOptimized

	// Check whether this is the new lowest cost expression with the enforcer
	// added.
	candidateBest := memo.MakeBestExpr(enforcerOp, eid, required)
	candidateBest.AddChild(innerState.best)
	o.ratchetCost(&candidateBest)

	// Enforcer expression is fully optimized if its input expression is fully
	// optimized.
	return fullyOptimized
}

// ratchetCost computes the cost of the candidate expression, and then checks
// whether it's lower than the cost of the existing best expression in the
// group. If so, then the candidate becomes the new lowest cost expression.
func (o *Optimizer) ratchetCost(candidate *memo.BestExpr) {
	group := candidate.Group()
	o.coster.computeCost(candidate, o.mem.GroupProperties(group))
	state := o.lookupOptState(group, candidate.Required())
	if state.best == memo.UnknownBestExprID {
		// Lazily allocate the best expression only when it's needed.
		state.best = o.mem.EnsureBestExpr(group, candidate.Required())
	}
	o.mem.RatchetBestExpr(state.best, candidate)
}

// lookupOptState looks up the state associated with the given group and
// properties. If no state exists yet, then lookupOptState returns nil.
func (o *Optimizer) lookupOptState(group memo.GroupID, required memo.PhysicalPropsID) *optState {
	return o.stateMap[optStateKey{group: group, required: required}]
}

// ensureOptState looks up the state associated with the given group and
// properties. If none is associated yet, then ensureOptState allocates new
// state and returns it.
func (o *Optimizer) ensureOptState(group memo.GroupID, required memo.PhysicalPropsID) *optState {
	key := optStateKey{group: group, required: required}
	state, ok := o.stateMap[key]
	if !ok {
		state = o.stateAlloc.allocate()
		o.stateMap[key] = state
	}
	return state
}

// optStateKey associates optState with a group that is being optimized with
// respect to a set of physical properties.
type optStateKey struct {
	group    memo.GroupID
	required memo.PhysicalPropsID
}

// optState is temporary storage that's associated with each group that's
// optimized (or same group with different sets of physical properties). The
// optimizer stores various flags and other state here that allows it to do
// quicker lookups and short-circuit already traversed parts of the expression
// tree.
type optState struct {
	// best identifies the lowest cost expression in the memo group for a given
	// set of physical properties.
	best memo.BestExprID

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

	// explore is used by the explorer to store intermediate state so that
	// redundant work is minimized.
	explore exploreState
}

// isExprFullyOptimized returns true if the given expression has been fully
// optimized for the required properties. The expression never needs to be
// recosted, no matter how many additional optimization passes are made.
func (os *optState) isExprFullyOptimized(eid memo.ExprID) bool {
	return os.fullyOptimizedExprs.Contains(int(eid.Expr))
}

// markExprAsFullyOptimized marks the given expression as fully optimized for
// the required properties. The expression never needs to be recosted, no
// matter how many additional optimization passes are made.
func (os *optState) markExprAsFullyOptimized(eid memo.ExprID) {
	if os.fullyOptimized {
		panic("best expression is already fully optimized")
	}
	if os.isExprFullyOptimized(eid) {
		panic("memo expression is already fully optimized for required physical properties")
	}
	os.fullyOptimizedExprs.Add(int(eid.Expr))
}

// initialPageSize is the number of structs that fit into the first page to be
// allocated. Each subsequent page doubles in size, up to maxPageSize.
const initialPageSize = 4

// maxPageSize is the number of structs that fit into the maximum page to be
// allocated.
const maxPageSize = 1024

// optStateAlloc alloctates pages of optState structs. Each page is twice the
// size of the previous, up to a maximum size. This is preferable to a slice
// of optState structs because pointers are not invalidated when a resize
// occurs, and because there's no need to retain a stable index.
type optStateAlloc struct {
	page []optState
}

// allocate returns a pointer to a new, empty optState struct. The pointer is
// stable, meaning that its location won't change as other optState structs are
// allocated.
func (a optStateAlloc) allocate() *optState {
	if a.page == nil {
		a.page = make([]optState, 0, initialPageSize)
	} else if len(a.page) == cap(a.page) {
		newSize := len(a.page) * 2
		if newSize > maxPageSize {
			newSize = maxPageSize
		}
		a.page = make([]optState, 0, newSize)
	}
	a.page = a.page[:len(a.page)+1]
	return &a.page[len(a.page)-1]
}
