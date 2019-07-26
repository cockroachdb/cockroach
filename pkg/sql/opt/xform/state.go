// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"container/heap"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// optState contains all the relevant information about the states
// of the different groups in the memo.
type optState struct {
	// stateMap allocates temporary storage that's used to speed up optimization.
	// This state could be discarded once optimization is complete.
	stateMap   map[groupStateKey]*groupState
	stateAlloc groupStateAlloc

	// alternate stores information about what plan we want the optimizer
	// to pick. By default this is set to 0, indicating the best plan found
	// is picked. If alternate plan is set to n, then teh optimizer picks
	// the n-th best plan.
	alternate int

	// alternateExprHeap is a min heap containing all the enumerated plans
	// for the query
	alternateExprHeap alternateExprHeap

	// candidateState stores values that can be used to restore the state
	// of all the group states for the candidate.
	candidateState map[candidate]candidateCurrentState
}

func (o *optState) init(alternate int) {
	o.stateMap = make(map[groupStateKey]*groupState)
	o.alternate = alternate

	if alternate > 0 {
		heap.Init(&o.alternateExprHeap)
		o.candidateState = make(map[candidate]candidateCurrentState)
	}
}

func (o *optState) clean() {
	for _, groupState := range o.stateMap {
		groupState.setCurrentCandidate(0)
	}

	o.stateMap = nil
	o.alternateExprHeap = nil
	o.candidateState = nil
}

// lookupOptState looks up the state associated with the given group and
// properties. If no state exists yet, then lookupOptState returns nil.
func (o *optState) lookupOptState(grp memo.RelExpr, required *physical.Required) *groupState {
	return o.stateMap[groupStateKey{group: grp, required: required}]
}

// ensureOptState looks up the state associated with the given group and
// properties. If none is associated yet, then ensureOptState allocates new
// state and returns it.
func (o *optState) ensureOptState(grp memo.RelExpr, required *physical.Required) *groupState {
	key := groupStateKey{group: grp, required: required}
	state, ok := o.stateMap[key]
	if !ok {
		state = o.stateAlloc.allocate()
		state.required = required
		o.stateMap[key] = state
	}
	return state
}

// ratchetCost computes the cost of the candidate expression, and then checks
// whether it's lower than the cost of the existing best expression in the
// group. If so, then the candidate becomes the new lowest cost expression.
func (o *optState) ratchetCost(state *groupState, candidateExpr memo.RelExpr, cost memo.Cost) {
	if state.best == nil || cost.Less(state.cost) {
		state.best = candidateExpr
		state.cost = cost
	}

	// We must store more information as they might be needed when generating
	// alternate plans.
	if o.alternate > 0 {
		if state.candidates == nil {
			state.candidates = make([]candidate, 0, o.alternate)
		}
		state.candidates = append(state.candidates, candidate{candidateExpr, cost})
	}
}

// sortCandidates sorts all the candidates in all the group states.
// A sorted order of candidates is required before we can begin finding
// alternate plans.
func (o *optState) sortCandidates() {
	for _, state := range o.stateMap {
		sort.Slice(state.candidates, func(i, j int) bool {
			return state.candidates[i].cost < state.candidates[j].cost
		})
	}
}

// restoreFromCandidateState restores all the candidate positions based
// on the candidateState provided.
func (o *optState) restoreFromCandidateState(cs candidateCurrentState) {
	for groupKey, groupState := range o.stateMap {
		groupState.setCurrentCandidate(cs.currentStates[groupKey])
	}
}

// getCurrentState creates a candidateCurrentState based on the current
// current position layout.
func (o *optState) getCurrentState() candidateCurrentState {
	var cs candidateCurrentState
	cs.currentStates = make(map[groupStateKey]int)
	for groupKey, groupState := range o.stateMap {
		cs.currentStates[groupKey] = groupState.getCurrentCandidate()
	}

	return cs
}

// candidate stores an expression with its corresponding cost. It is used
// to store alternate expressions for a given group state.
type candidate struct {
	candidateExpr opt.Expr
	cost          memo.Cost
}

type alternateExprHeap []candidate

func (h alternateExprHeap) Len() int           { return len(h) }
func (h alternateExprHeap) Less(i, j int) bool { return h[i].cost < h[j].cost }
func (h alternateExprHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *alternateExprHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(candidate))
}

func (h *alternateExprHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// candidateCurrentStates stores the current candidate position for
// each group state at the time the candidate was inserted into the heap.
type candidateCurrentState struct {
	currentStates map[groupStateKey]int
}

// groupStateKey associates groupState with a group that is being optimized with
// respect to a set of physical properties.
type groupStateKey struct {
	group    memo.RelExpr
	required *physical.Required
}

// groupState is temporary storage that's associated with each group that's
// optimized (or same group with different sets of physical properties). The
// optimizer stores various flags and other state here that allows it to do
// quicker lookups and short-circuit already traversed parts of the expression
// tree.
type groupState struct {
	// best identifies the lowest cost expression in the memo group for a given
	// set of physical properties.
	best memo.RelExpr

	// required is the set of physical properties that must be provided by this
	// lowest cost expression. An expression that cannot provide these properties
	// cannot be the best expression, no matter how low its cost.
	required *physical.Required

	// cost is the estimated execution cost for this expression. The best
	// expression for a given group and set of physical properties is the
	// expression with the lowest cost.
	cost memo.Cost

	// fullyOptimized is set to true once the lowest cost expression has been
	// found for a memo group, with respect to the required properties. A lower
	// cost expression will never be found, no matter how many additional
	// optimization passes are made.
	fullyOptimized bool

	// fullyOptimizedExprs contains the set of ordinal positions of each member
	// expression in the group that has been fully optimized for the required
	// properties. These never need to be recosted, no matter how many additional
	// optimization passes are made.
	fullyOptimizedExprs util.FastIntSet

	// explore is used by the explorer to store intermediate state so that
	// redundant work is minimized.
	explore exploreState

	// candidates stores all the expressions in the groupState after being costed.
	// candidates are only needed if an alternate expression is being looked for.
	// TODO(ridwanmsharif): We should bound this by |alternate|. Using a heap here
	//  could be useful.
	candidates []candidate

	// currentCandididate denotes an index in candidates that is being considered
	// for this group state.
	currentCandidate int
}

// currCandidate returns the index of the current candidate expression in
// the group state candidates.
func (os *groupState) getCurrentCandidate() int {
	return os.currentCandidate
}

// setCurrentCandidate sets the current candidate index.
func (os *groupState) setCurrentCandidate(candidate int) {
	os.currentCandidate = candidate
}

// bestExpr returns the best expression for this group state based on what has been
// fixed. If we're looking for the optimal plan, this returns the lowest cost
// expression. Otherwise this returns the lowest cost candidate expression given that
// the groupState is being fixed.
func (os *groupState) bestExpr() memo.RelExpr {
	if len(os.candidates) == 0 {
		return os.best
	}

	// There are alternate ones.
	return os.candidates[os.getCurrentCandidate()].candidateExpr.(memo.RelExpr)
}

// bestCost returns the cost opf the best expression for this group state based
// on what has been fixed. If we're looking for the optimal plan, this returns the
// cost of the best expression. Otherwise this returns the cost of the lowest cost
// candidate expression given that the groupState is being fixed.
func (os *groupState) bestCost() memo.Cost {
	if len(os.candidates) == 0 {
		return os.cost
	}

	// There are alternate ones.
	return os.candidates[os.getCurrentCandidate()].cost
}

// isMemberFullyOptimized returns true if the group member at the given ordinal
// position has been fully optimized for the required properties. The expression
// never needs to be recosted, no matter how many additional optimization passes
// are made.
func (os *groupState) isMemberFullyOptimized(ord int) bool {
	return os.fullyOptimizedExprs.Contains(ord)
}

// markMemberAsFullyOptimized marks the group member at the given ordinal
// position as fully optimized for the required properties. The expression never
// needs to be recosted, no matter how many additional optimization passes are
// made.
func (os *groupState) markMemberAsFullyOptimized(ord int) {
	if os.fullyOptimized {
		panic(errors.AssertionFailedf("best expression is already fully optimized"))
	}
	if os.isMemberFullyOptimized(ord) {
		panic(errors.AssertionFailedf("memo expression is already fully optimized for required physical properties"))
	}
	os.fullyOptimizedExprs.Add(ord)
}

// groupStateAlloc allocates pages of groupState structs. This is preferable to
// a slice of groupState structs because pointers are not invalidated when a
// resize occurs, and because there's no need to retain a stable index.
type groupStateAlloc struct {
	page []groupState
}

// allocate returns a pointer to a new, empty groupState struct. The pointer is
// stable, meaning that its location won't change as other groupState structs
// are allocated.
func (a *groupStateAlloc) allocate() *groupState {
	if len(a.page) == 0 {
		a.page = make([]groupState, 8)
	}
	state := &a.page[0]
	a.page = a.page[1:]
	return state
}

// setAlternateExprTree traverses the memo and recursively updates child pointers
// so that they point to the nth lowest cost expression tree rather than to the
// normalized expression tree.
// See comment above setLowestCostTree for more detail.
func (o *Optimizer) setAlternateExprTree(
	parent opt.Expr, parentProps *physical.Required, nth int,
) opt.Expr {
	// Before we proceed, we must sort all the candidates based on cost.
	o.optState.sortCandidates()

	var relParent memo.RelExpr
	var relCost memo.Cost
	switch t := parent.(type) {
	case memo.RelExpr:
		state := o.optState.lookupOptState(t.FirstExpr(), parentProps)
		relParent, relCost = state.bestExpr(), state.bestCost()
		parent = relParent

	case memo.ScalarPropsExpr:
		// Short-circuit traversal of scalar expressions with no nested subquery,
		// since there's only one possible tree.
		if !t.ScalarProps(o.mem).HasSubquery {
			return parent
		}
	}

	currCandidate := candidate{o.setLowestCostTree(parent, parentProps, true), relCost}
	var seenExprs []opt.Expr

	// Save the current state.
	oldState := o.optState.getCurrentState()

	for i := 0; i < o.optState.alternate; i++ {
		// Restore the candidate state.
		candidateState, ok := o.optState.candidateState[currCandidate]
		if !ok {
			candidateState = oldState
		}

		// Use the candidate state.
		o.optState.restoreFromCandidateState(candidateState)

		// For each group state, generate a new expression to add to the heap.
		for _, groupState := range o.optState.stateMap {
			if groupState.getCurrentCandidate()+1 >= len(groupState.candidates) {
				continue
			}

			// Update the current position.
			groupState.setCurrentCandidate(groupState.getCurrentCandidate() + 1)

			// The best plan generated now will respect the current position.
			newPlan := o.setLowestCostTree(currCandidate.candidateExpr, parentProps, false)
			isNewPlan := true
			for _, seenCandidate := range o.optState.alternateExprHeap {
				if newPlan == seenCandidate.candidateExpr {
					isNewPlan = false
					break
				}
			}

			// Has this plan been seen before?
			for _, seenExpr := range seenExprs {
				if newPlan == seenExpr {
					isNewPlan = false
					break
				}
			}

			if isNewPlan {
				newPlanCost := o.recomputeCostImpl(newPlan, parentProps, o.coster)
				newCandidate := candidate{newPlan, newPlanCost}

				o.optState.candidateState[newCandidate] = o.optState.getCurrentState()
				heap.Push(&o.optState.alternateExprHeap, newCandidate)
			}

			// Restore the old current position.
			groupState.setCurrentCandidate(groupState.getCurrentCandidate() - 1)
		}

		// Restore the old state.
		o.optState.restoreFromCandidateState(oldState)

		// Set up the next iteration.
		if len(o.optState.alternateExprHeap) > 0 {
			currCandidate = heap.Pop(&o.optState.alternateExprHeap).(candidate)
		}

		seenExprs = append(seenExprs, currCandidate.candidateExpr)
	}

	// If the candidate has been seen before, get a new one.
	for {
		isNewPlan := true
		for _, seenExpr := range seenExprs {
			if currCandidate.candidateExpr == seenExpr {
				isNewPlan = false
				break
			}
		}

		if len(o.optState.alternateExprHeap) == 0 || isNewPlan {
			break
		}

		if !isNewPlan {
			currCandidate = heap.Pop(&o.optState.alternateExprHeap).(candidate)
		}
	}

	o.optState.clean()
	return currCandidate.candidateExpr
}
