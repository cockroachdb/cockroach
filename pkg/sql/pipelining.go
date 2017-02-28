// Copyright 2017 The Cockroach Authors.
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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package sql

import (
	"sync"

	"container/list"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// PipelineQueue maintains a set of planNodes running with pipelined execution.
// It uses a DependencyAnalyzer to determine dependencies between plans. Using
// this knowledge, the queue provides the following guarantees about the execution
// of plans:
// 1. No two plans will ever be run concurrently if they are dependent of one another.
// 2. If two dependent plans are added to the queue, the plan added first will be
//    executed before the plan added second.
// 3. No plans will begin execution once an error has been seen until Wait is
//    called to drain the plans and reset the error state.
//
// The queue performs all computation on pointers to planNode interfaces. This is
// because it wants to operate on unique objects, and equality of interfaces does
// not necessarily imply pointer equality.
type PipelineQueue struct {
	running  planNodeSet
	pending  list.List // pendingPlan elements
	analyzer DependencyAnalyzer

	mu           syncutil.Mutex
	runningGroup sync.WaitGroup
	err          error
}

// planNodeSet is a set of planNodes.
type planNodeSet map[*planNode]struct{}

// pendingPlan is a plan that is waiting for dependent plans to finish execution
// before running itself.
type pendingPlan struct {
	plan    *planNode
	prereqs planNodeSet

	// closed when the plan is ready to run. Passed an error if the plan should
	// abort before running because an error in an earlier plan was observed.
	ready chan error
}

// MakePipelineQueue creates a new empty PipelineQueue that uses the provided
// DependencyAnalyzer to determine plan dependencies.
func MakePipelineQueue(analyzer DependencyAnalyzer) PipelineQueue {
	return PipelineQueue{
		running:  make(planNodeSet),
		analyzer: analyzer,
	}
}

// Add inserts a new plan in the queue and executes the provided function when
// appropriate, obeying the guarantees made by the PipelineQueue.
//
// Add should not be called concurrently with Wait. See Wait's comment for more
// details.
func (pq *PipelineQueue) Add(plan *planNode, exec func() error) {
	ready, abortEarly := pq.insertNewPlanInQueue(plan)
	if abortEarly {
		return
	}

	pq.runningGroup.Add(1)
	go func() {
		defer pq.runningGroup.Done()

		if ready != nil {
			// The plan that is blocking us will run us once it finishes by
			// closing the ready channel. See runUnblockedPlans.
			if prereqErr := <-ready; prereqErr != nil {
				// If a prerequisite saw an error, abort without attempting
				// to run.
				return
			}
		}

		// Execute the plan.
		err := exec()

		pq.mu.Lock()
		defer pq.mu.Unlock()

		// Remove the current plan from the running set and handle error states.
		delete(pq.running, plan)

		if pq.err != nil {
			// Nothing to do if this pipeline batch has already seen an error.
			return
		}

		if err != nil {
			// If it did throw an error and we have not already seen an error
			// since the last Wait, set the error state and abort all pending
			// plans.
			pq.err = err
			pq.abortPendingPlansLocked(err)
		} else {
			// If execution of the current plan threw no errors, we can safely
			// unblock all plans that were dependent on this plan.
			pq.execUnblockedPlansLocked(plan)
		}
	}()
}

// insertPlanInQueue inserts the planNode in the queue. It returns a nil channel
// if the the plan can be run immediately. If not, returns a channel that should
// be read from to block until the plan is ready to run. It also returns a boolean
// indicating if the plan should be aborted early before running at all. This will
// be set to true when the current execution batch has already seen an error and
// there is no reason to even attempt to run the current plan.
func (pq *PipelineQueue) insertNewPlanInQueue(plan *planNode) (ready chan error, abortEarly bool) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.err != nil {
		// If the error is already set, there is no reason to add the
		// new plan to the queue. We just drop it on the floor.
		return nil, true
	}

	if prereqs := pq.prereqsForPlan(plan); len(prereqs) > 0 {
		ready := make(chan error)
		pq.pending.PushBack(pendingPlan{
			plan:    plan,
			prereqs: prereqs,
			ready:   ready,
		})
		return ready, false
	}
	pq.running[plan] = struct{}{}
	return nil, false
}

// prereqsForPlan determines the set of plans currently running and in the
// pending list that a new plan is dependent on. Returns nil if the plan
// has no prerequisites and can be run immediately.
func (pq *PipelineQueue) prereqsForPlan(newPlan *planNode) planNodeSet {
	var prereqs planNodeSet

	// Add all plans from the running set that this new plan is dependent on.
	for runningPlan := range pq.running {
		if !pq.analyzer.Independent(runningPlan, newPlan) {
			if prereqs == nil {
				prereqs = make(planNodeSet)
			}
			prereqs[runningPlan] = struct{}{}
		}
	}
	// Add all plans from the pending queue that this new plan is dependent on.
	for e := pq.pending.Front(); e != nil; e = e.Next() {
		pendingPlan := e.Value.(pendingPlan).plan
		if !pq.analyzer.Independent(pendingPlan, newPlan) {
			if prereqs == nil {
				prereqs = make(planNodeSet)
			}
			prereqs[pendingPlan] = struct{}{}
		}
	}

	return prereqs
}

// execUnblockedPlans begins all plans in the pending list that are not blocked
// by dependent plans.
func (pq *PipelineQueue) execUnblockedPlansLocked(removedPlan *planNode) {
	// Iterate over all pending plans (in order) and run the ones that are
	// no longer dependent on any plan ahead of them.
	for e := pq.pending.Front(); e != nil; {
		// We may Remove this node, so perform the Next seek early.
		cur := e
		e = e.Next()

		pendingPlan := cur.Value.(pendingPlan)
		delete(pendingPlan.prereqs, removedPlan)
		if len(pendingPlan.prereqs) == 0 {
			// The plan's last prereq was the removedPlan, run it.
			pq.running[pendingPlan.plan] = struct{}{}
			pq.pending.Remove(cur)
			close(pendingPlan.ready)
		}
	}
}

// abortPendingPlansLocked sends an error on each of the pending plans'
// ready channel to indicate that they should not even attempt to run.
// It then clears the pending plan list.
func (pq *PipelineQueue) abortPendingPlansLocked(err error) {
	for e := pq.pending.Front(); e != nil; e = e.Next() {
		pendingPlan := e.Value.(pendingPlan)
		pendingPlan.ready <- err
		close(pendingPlan.ready)
	}
	pq.pending.Init()
}

// Len returns the number of plans in the PipelineQueue.
func (pq *PipelineQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.running) + pq.pending.Len()
}

// Wait blocks until the PipelineQueue finishes executing all plans. It then
// returns the error of the last batch of pipelined execution before reseting
// the error to allow for future use.
//
// Wait can not be called concurrently with Add. If we need to lift this
// restriction, consider replacing the sync.WaitGroup with a syncutil.RWMutex,
// which will provide the desired starvation and ordering properties. Those
// being that once Wait is called, future Adds will not be reordered ahead
// of Waits attempts to drain all running and pending plans.
func (pq *PipelineQueue) Wait() error {
	pq.runningGroup.Wait()

	// There is no race condition between waiting on the WaitGroup and locking
	// the mutex because PipelineQueue.Wait cannot be called concurrently with
	// Add. We lock only because Err may be called concurrently.
	pq.mu.Lock()
	err := pq.err
	pq.err = nil
	pq.mu.Unlock()
	return err
}

// Err returns the PipelineQueue's error.
func (pq *PipelineQueue) Err() error {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.err
}

// DependencyAnalyzer determines if plans are independent of one another, where
// independent plans are defined by whether their execution could be safely reordered
// without having an effect on their runtime semantics or on their results. This
// means that DependencyAnalyzer can be used to test whether it is safe for multiple
// statements to be run concurrently by the PipelineQueue.
type DependencyAnalyzer interface {
	// Independent determines if the provided planNodes are independent from one
	// another. Implementations of Independent are always commutative.
	Independent(*planNode, *planNode) bool
}

// dependencyAnalyzerFunc is an implementation of DependencyAnalyzer that defers
// to a function for all dependency decisions.
type dependencyAnalyzerFunc func(*planNode, *planNode) bool

func (f dependencyAnalyzerFunc) Independent(p1 *planNode, p2 *planNode) bool {
	return f(p1, p2)
}

// NoDependenciesAnalyzer is a DependencyAnalyzer that performs no analysis on
// planNodes and asserts that all plans are independent.
var NoDependenciesAnalyzer DependencyAnalyzer = dependencyAnalyzerFunc(func(
	_ *planNode, _ *planNode,
) bool {
	return true
})
