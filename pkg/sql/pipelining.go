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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// PipelineQueue maintains a set of planNodes running with pipelined execution.
// Pipelined execution means that multiple statements run asynchronously, with
// their results mocked out to the client and with independent statements allowed
// to run in parallel. Any errors seen when running these statements are delayed
// until the pipelined execution is "synchronized" on the next non-pipelined
// statement. The syntax to pipeline statement execution is the statement with
// RETURNING NOTHING appended to it. The feature is described further in
// docs/RFCS/sql_pipelining.md.
//
// It uses a DependencyAnalyzer to determine dependencies between plans. Using
// this knowledge, the queue provides the following guarantees about the execution
// of plans:
// 1. No two plans will ever be run concurrently if they are dependent of one another.
// 2. If two dependent plans are added to the queue, the plan added first will be
//    executed before the plan added second.
// 3. No plans will begin execution once an error has been seen until Wait is
//    called to drain the plans and reset the error state.
//
type PipelineQueue struct {
	// analyzer is a DependencyAnalyzer that computes when certain plans are dependent
	// on one another. It determines if it is safe to run plans concurrently.
	analyzer DependencyAnalyzer

	// plans is a set of all running and pending plans, with their corresponding "done"
	// channels. These channels are closed when the plan has finished executing.
	plans map[planNode]doneChan

	// err is the first error seen since the last call to PipelineQueue.Wait. Referred to
	// as the current "pipeline batch's error".
	err error

	mu           syncutil.Mutex
	runningGroup sync.WaitGroup
}

// doneChan is a channel that is closed when a plan finishes execution.
type doneChan chan struct{}

// MakePipelineQueue creates a new empty PipelineQueue that uses the provided
// DependencyAnalyzer to determine plan dependencies.
func MakePipelineQueue(analyzer DependencyAnalyzer) PipelineQueue {
	return PipelineQueue{
		plans:    make(map[planNode]doneChan),
		analyzer: analyzer,
	}
}

// Add inserts a new plan in the queue and executes the provided function when
// all plans that it depends on have completed successfully, obeying the guarantees
// made by the PipelineQueue above. The exec function should be used to run the
// planNode and return any error observed during its execution.
//
// Add should not be called concurrently with Wait. See Wait's comment for more
// details.
func (pq *PipelineQueue) Add(ctx context.Context, plan planNode, exec func(planNode) error) {
	prereqs, finishLocked := pq.insertInQueue(ctx, plan)
	pq.runningGroup.Add(1)
	go func() {
		defer pq.runningGroup.Done()

		// Block on the execution of each prerequisite plan blocking us.
		for _, prereq := range prereqs {
			<-prereq
		}

		// Don't bother executing if an error has already been set.
		if abort := func() bool {
			pq.mu.Lock()
			defer pq.mu.Unlock()
			if pq.err != nil {
				finishLocked()
				return true
			}
			return false
		}(); abort {
			return
		}

		// Execute the plan.
		err := exec(plan)

		pq.mu.Lock()
		defer pq.mu.Unlock()

		if pq.err == nil {
			// If we have not already seen an error since the last Wait, set the
			// error state.
			pq.err = err
		}

		finishLocked()
	}()
}

// insertInQueue inserts the planNode in the queue. It returns a list of the "done"
// channels of prerequisite blocking the new plan from executing. It also returns a
// function to call when the new plan has finished executing. This function must be
// called while pq.mu is held.
func (pq *PipelineQueue) insertInQueue(ctx context.Context, newPlan planNode) ([]doneChan, func()) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	// Determine the set of prerequisite plans.
	prereqs := pq.prereqsForPlanLocked(ctx, newPlan)

	// Insert newPlan in running set.
	newDoneChan := make(doneChan)
	pq.plans[newPlan] = newDoneChan
	finish := func() {
		// Remove the current plan from the running set and signal to dependent
		// plans that we're done by closing our done channel.
		delete(pq.plans, newPlan)
		close(newDoneChan)

		// Remove the current plan from the DependencyAnalyzer, in case it was
		// caching any state about the plan.
		pq.analyzer.Clear(newPlan)
	}
	return prereqs, finish
}

// prereqsForPlanLocked determines the set of plans currently running and pending
// that a new plan is dependent on. It returns a slice of doneChans for each plan
// in this set. Returns a nil slice if the plan has no prerequisites and can be run
// immediately.
func (pq *PipelineQueue) prereqsForPlanLocked(ctx context.Context, newPlan planNode) []doneChan {
	// Add all plans from the plan set that this new plan is dependent on.
	var prereqs []doneChan
	for plan, doneChan := range pq.plans {
		if !pq.analyzer.Independent(ctx, plan, newPlan) {
			prereqs = append(prereqs, doneChan)
		}
	}
	return prereqs
}

// Len returns the number of plans in the PipelineQueue.
func (pq *PipelineQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.plans)
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
	defer pq.mu.Unlock()
	err := pq.err
	pq.err = nil
	return err
}

// Err returns the PipelineQueue's error.
func (pq *PipelineQueue) Err() error {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.err
}

// DependencyAnalyzer determines if plans are independent of one another, where
// independent plans are defined by whether their execution could be safely
// reordered without having an effect on their runtime semantics or on their
// results. DependencyAnalyzer is used by PipelineQueue to test whether it is
// safe for multiple statements to be run concurrently.
//
// DependencyAnalyzer implementations do not need to be safe to use from multiple
// goroutines concurrently.
type DependencyAnalyzer interface {
	// Independent determines if the provided planNodes are independent from one
	// another. Implementations of Independent are always commutative.
	Independent(context.Context, planNode, planNode) bool
	// Clear is a hint to the DependencyAnalyzer that the provided planNode will
	// no longer be needed. It is useful for DependencyAnalyzers that cache state
	// on the planNodes.
	Clear(planNode)
}

var _ DependencyAnalyzer = dependencyAnalyzerFunc(nil)
var _ DependencyAnalyzer = &spanBasedDependencyAnalyzer{}

// dependencyAnalyzerFunc is an implementation of DependencyAnalyzer that defers
// to a function for all dependency decisions.
type dependencyAnalyzerFunc func(planNode, planNode) bool

func (f dependencyAnalyzerFunc) Independent(_ context.Context, p1 planNode, p2 planNode) bool {
	return f(p1, p2)
}

func (f dependencyAnalyzerFunc) Clear(_ planNode) {}

// NoDependenciesAnalyzer is a DependencyAnalyzer that performs no analysis on
// planNodes and asserts that all plans are independent.
var NoDependenciesAnalyzer DependencyAnalyzer = dependencyAnalyzerFunc(func(
	_ planNode, _ planNode,
) bool {
	return true
})

// planSpans holds the read and write spans that a planNode will touch during
// execution. It represents the effect of a plan's execution and is used to
// determine if two plans are independent.
type planSpans struct {
	read  interval.RangeGroup
	write interval.RangeGroup
}

// spanBasedDependencyAnalyzer determines planNode independence based off of
// the read and write spans that a pair of planNodes interact with. The
// implementation of DependencyAnalyzer expects all planNodes to implement the
// spanCollector interface, and will panic if they do not.
type spanBasedDependencyAnalyzer struct {
	// spanCache caches the read and write spans of all active planNodes, so
	// that the spans only need to be collected once.
	spanCache map[planNode]planSpans
}

// NewSpanBasedDependencyAnalyzer creates a new SpanBasedDependencyAnalyzer.
func NewSpanBasedDependencyAnalyzer() DependencyAnalyzer {
	return &spanBasedDependencyAnalyzer{
		spanCache: make(map[planNode]planSpans),
	}
}

func (a *spanBasedDependencyAnalyzer) Independent(
	ctx context.Context, p1 planNode, p2 planNode,
) bool {
	s1, s2 := a.spansForPlan(ctx, p1), a.spansForPlan(ctx, p2)
	if interval.RangeGroupsOverlap(s1.write, s2.write) {
		return false
	}
	if interval.RangeGroupsOverlap(s1.read, s2.write) {
		return false
	}
	if interval.RangeGroupsOverlap(s1.write, s2.read) {
		return false
	}
	return true
}

func (a *spanBasedDependencyAnalyzer) spansForPlan(ctx context.Context, p planNode) planSpans {
	if spans, ok := a.spanCache[p]; ok {
		return spans
	}

	readSpans, writeSpans, err := p.Spans(ctx)
	if err != nil {
		panic(err)
	}
	spans := planSpans{
		read:  rangeGroupFromSpans(readSpans),
		write: rangeGroupFromSpans(writeSpans),
	}
	a.spanCache[p] = spans
	return spans
}

func rangeGroupFromSpans(spans roachpb.Spans) interval.RangeGroup {
	rg := interval.NewRangeList()
	for _, s := range spans {
		rg.Add(s.AsRange())
	}
	return rg
}

func (a *spanBasedDependencyAnalyzer) Clear(p planNode) {
	delete(a.spanCache, p)
}

// IsStmtPipelined determines if a given statement's execution should be pipelined.
// This means that its results should be mocked out, and that it should be run
// asynchronously.
func IsStmtPipelined(stmt parser.Statement) bool {
	pipelinedRetClause := func(ret parser.ReturningClause) bool {
		_, ok := ret.(*parser.ReturningNothing)
		return ok
	}
	switch s := stmt.(type) {
	case *parser.Delete:
		return pipelinedRetClause(s.Returning)
	case *parser.Insert:
		return pipelinedRetClause(s.Returning)
	case *parser.Update:
		return pipelinedRetClause(s.Returning)
	}
	return false
}
