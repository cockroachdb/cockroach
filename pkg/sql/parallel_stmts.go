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

// ParallelizeQueue maintains a set of plans running with parallelized execution.
// Parallelized execution means that multiple statements run asynchronously, with
// their results mocked out to the client and with independent statements allowed
// to run in parallel. Any errors seen when running these statements are delayed
// until the parallelized execution is "synchronized" on the next non-parallelized
// statement. The syntax to parallelize statement execution is the statement with
// RETURNING NOTHING appended to it. The feature is described further in
// docs/RFCS/sql_parallelization.md.
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
type ParallelizeQueue struct {
	// analyzer is a DependencyAnalyzer that computes when certain plans are dependent
	// on one another. It determines if it is safe to run plans concurrently.
	analyzer DependencyAnalyzer

	// plans is a set of all running and pending plans, with their corresponding "done"
	// channels. These channels are closed when the plan has finished executing.
	plans map[plan]doneChan

	// err is the first error seen since the last call to ParallelizeQueue.Wait.
	// Referred to as the current "parallel batch's error".
	err error

	mu           syncutil.Mutex
	runningGroup sync.WaitGroup
}

// doneChan is a channel that is closed when a plan finishes execution.
type doneChan chan struct{}

// MakeParallelizeQueue creates a new empty ParallelizeQueue that uses the provided
// DependencyAnalyzer to determine plan dependencies.
func MakeParallelizeQueue(analyzer DependencyAnalyzer) ParallelizeQueue {
	return ParallelizeQueue{
		plans:    make(map[plan]doneChan),
		analyzer: analyzer,
	}
}

// Add inserts a new plan in the queue and executes the provided function when
// all plans that it depends on have completed successfully, obeying the guarantees
// made by the ParallelizeQueue above. The exec function should be used to run the
// plan and return any error observed during its execution.
//
// Add should not be called concurrently with Wait. See Wait's comment for more
// details.
func (pq *ParallelizeQueue) Add(ctx context.Context, p plan, exec func(plan) error) {
	prereqs, finishLocked := pq.insertInQueue(ctx, p)
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
		err := exec(p)

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

// insertInQueue inserts the plan in the queue. It returns a list of the "done"
// channels of prerequisite blocking the new plan from executing. It also returns a
// function to call when the new plan has finished executing. This function must be
// called while pq.mu is held.
func (pq *ParallelizeQueue) insertInQueue(ctx context.Context, p plan) ([]doneChan, func()) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	// Determine the set of prerequisite plans.
	prereqs := pq.prereqsForPlanLocked(ctx, p)

	// Insert p in running set.
	newDoneChan := make(doneChan)
	pq.plans[p] = newDoneChan
	finish := func() {
		// Remove the current plan from the running set and signal to dependent
		// plans that we're done by closing our done channel.
		delete(pq.plans, p)
		close(newDoneChan)

		// Remove the current plan from the DependencyAnalyzer, in case it was
		// caching any state about the plan.
		pq.analyzer.Clear(p)
	}
	return prereqs, finish
}

// prereqsForPlanLocked determines the set of plans currently running and pending
// that a new plan is dependent on. It returns a slice of doneChans for each plan
// in this set. Returns a nil slice if the plan has no prerequisites and can be run
// immediately.
func (pq *ParallelizeQueue) prereqsForPlanLocked(ctx context.Context, p plan) []doneChan {
	// First, submit the plan to the analyzer for analysis. This assures
	// that the analysis takes place before the plan is executed, even if
	// no calls to analyzer.Independent are necessary at this time.
	pq.analyzer.Analyze(ctx, p)

	// Next, add all plans from the plan set that this new plan is dependent on.
	var prereqs []doneChan
	for plan, doneChan := range pq.plans {
		if !pq.analyzer.Independent(plan, p) {
			prereqs = append(prereqs, doneChan)
		}
	}
	return prereqs
}

// Len returns the number of plans in the ParallelizeQueue.
func (pq *ParallelizeQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.plans)
}

// Wait blocks until the ParallelizeQueue finishes executing all plans. It then
// returns the error of the last batch of parallelized execution before reseting
// the error to allow for future use.
//
// Wait can not be called concurrently with Add. If we need to lift this
// restriction, consider replacing the sync.WaitGroup with a syncutil.RWMutex,
// which will provide the desired starvation and ordering properties. Those
// being that once Wait is called, future Adds will not be reordered ahead
// of Waits attempts to drain all running and pending plans.
func (pq *ParallelizeQueue) Wait() error {
	pq.runningGroup.Wait()

	// There is no race condition between waiting on the WaitGroup and locking
	// the mutex because ParallelizeQueue.Wait cannot be called concurrently with
	// Add. We lock only because Err may be called concurrently.
	pq.mu.Lock()
	defer pq.mu.Unlock()
	err := pq.err
	pq.err = nil
	return err
}

// Err returns the ParallelizeQueue's error.
func (pq *ParallelizeQueue) Err() error {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.err
}

// DependencyAnalyzer determines if plans are independent of one another, where
// independent plans are defined by whether their execution could be safely
// reordered without having an effect on their runtime semantics or on their
// results. DependencyAnalyzer is used by ParallelizeQueue to test whether it is
// safe for multiple statements to be run concurrently.
//
// DependencyAnalyzer implementations do not need to be safe to use from multiple
// goroutines concurrently.
type DependencyAnalyzer interface {
	// Analyze collects any upfront analysis that is necessary to make future
	// independence decisions about the plan. It should be called before calling
	// Independent for each plan, and the plan provided should not be running
	// when Analyze is called. Analyze is allowed to mutate the plan if
	// necessary (i.e. add SpanConstraints).
	Analyze(context.Context, plan)
	// Independent determines if the provided plans are independent from one
	// another. Either plan may be running when Independent is called, so it
	// will not modify the plans in any way. Implementations of Independent
	// are always commutative.
	Independent(plan, plan) bool
	// Clear is a hint to the DependencyAnalyzer that the provided plan will
	// no longer be needed. It is useful for DependencyAnalyzers that cache
	// state on the plans during Analyze.
	Clear(plan)
}

var _ DependencyAnalyzer = dependencyAnalyzerFunc(nil)
var _ DependencyAnalyzer = &spanBasedDependencyAnalyzer{}

// dependencyAnalyzerFunc is an implementation of DependencyAnalyzer that defers
// to a function for all dependency decisions.
type dependencyAnalyzerFunc func(plan, plan) bool

func (f dependencyAnalyzerFunc) Independent(p1 plan, p2 plan) bool {
	return f(p1, p2)
}

func (f dependencyAnalyzerFunc) Analyze(_ context.Context, _ plan) {}
func (f dependencyAnalyzerFunc) Clear(_ plan)                      {}

// NoDependenciesAnalyzer is a DependencyAnalyzer that performs no analysis on
// plans and asserts that all plans are independent.
var NoDependenciesAnalyzer DependencyAnalyzer = dependencyAnalyzerFunc(func(
	_ plan, _ plan,
) bool {
	return true
})

// planAnalysis holds the read and write spans that a plan will touch during
// execution, along with other information necessary to determine plan independence.
type planAnalysis struct {
	read          interval.RangeGroup
	write         interval.RangeGroup
	hasOrderingFn bool
}

// spanBasedDependencyAnalyzer determines plan independence based off of
// the read and write spans that a pair of plans interact with. The
// implementation of DependencyAnalyzer expects all plans to implement the
// spanCollector interface, and will panic if they do not.
type spanBasedDependencyAnalyzer struct {
	// spanCache caches the analysis results of all active plans, so
	// that the analysis only needs to be performed once.
	analysisCache map[plan]planAnalysis
}

// NewSpanBasedDependencyAnalyzer creates a new SpanBasedDependencyAnalyzer.
func NewSpanBasedDependencyAnalyzer() DependencyAnalyzer {
	return &spanBasedDependencyAnalyzer{
		analysisCache: make(map[plan]planAnalysis),
	}
}

func (a *spanBasedDependencyAnalyzer) Analyze(ctx context.Context, p plan) {
	// Collect the read and write spans that the plan will touch. While doing
	// so, set these as SpanConstraints on the plan to bind the plan to this
	// limitted set of key range accesses.
	readSpans, writeSpans, err := p.queryPlan.Spans(ctx)
	if err != nil {
		panic(err)
	}
	readRanges, writeRanges := rangeGroupFromSpans(readSpans), rangeGroupFromSpans(writeSpans)
	p.planner.sc.PermittedReads = readRanges
	p.planner.sc.PermittedWrites = writeRanges

	// Determine if the plan contains any functions that preclude it from
	// being reordered.
	hasOrderingFn := containsOrderingFunction(ctx, p)

	a.analysisCache[p] = planAnalysis{
		read:          readRanges,
		write:         writeRanges,
		hasOrderingFn: hasOrderingFn,
	}
}

func (a *spanBasedDependencyAnalyzer) Independent(p1 plan, p2 plan) bool {
	a1, ok1 := a.analysisCache[p1]
	a2, ok2 := a.analysisCache[p2]
	if !ok1 || !ok2 {
		panic("Analyze must be called for each plan before Independent")
	}
	if a1.hasOrderingFn || a2.hasOrderingFn {
		return false
	}
	if interval.RangeGroupsOverlap(a1.write, a2.write) {
		return false
	}
	if interval.RangeGroupsOverlap(a1.read, a2.write) {
		return false
	}
	if interval.RangeGroupsOverlap(a1.write, a2.read) {
		return false
	}
	return true
}

func (a *spanBasedDependencyAnalyzer) Clear(p plan) {
	delete(a.analysisCache, p)
}

// orderingFunctions is a set of all functions that preclude statement independence.
// These functions are contractually monotonic within a transaction and thus prevents
// reordering.
var orderingFunctions = map[string]struct{}{
	"statement_timestamp": {},
}

func containsOrderingFunction(ctx context.Context, p plan) bool {
	sawOrderingFn := false
	po := planObserver{expr: func(_, _ string, n int, expr parser.Expr) {
		if f, ok := expr.(*parser.FuncExpr); ok {
			if _, ok := orderingFunctions[f.Func.String()]; ok {
				sawOrderingFn = true
			}
		}
	}}
	if err := walkPlan(ctx, p.queryPlan, po); err != nil {
		panic(err)
	}
	return sawOrderingFn
}

func rangeGroupFromSpans(spans roachpb.Spans) interval.RangeGroup {
	rg := interval.NewRangeList()
	for _, s := range spans {
		rg.Add(s.AsRange())
	}
	return rg
}

// IsStmtParallelized determines if a given statement's execution should be
// parallelized. This means that its results should be mocked out, and that
// it should be run asynchronously and in parallel with other statements that
// are independent.
func IsStmtParallelized(stmt parser.Statement) bool {
	parallelizedRetClause := func(ret parser.ReturningClause) bool {
		_, ok := ret.(*parser.ReturningNothing)
		return ok
	}
	switch s := stmt.(type) {
	case *parser.Delete:
		return parallelizedRetClause(s.Returning)
	case *parser.Insert:
		return parallelizedRetClause(s.Returning)
	case *parser.Update:
		return parallelizedRetClause(s.Returning)
	}
	return false
}
