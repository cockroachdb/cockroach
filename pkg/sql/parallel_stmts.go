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

package sql

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// ParallelizeQueue maintains a set of planNodes running with parallelized execution.
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
//    called to drain the plans and reset the error-set state.
//
type ParallelizeQueue struct {
	// analyzer is a DependencyAnalyzer that computes when certain plans are dependent
	// on one another. It determines if it is safe to run plans concurrently.
	analyzer DependencyAnalyzer

	// plans is a set of all running and pending plans, with their corresponding "done"
	// channels. These channels are closed when the plan has finished executing.
	plans map[planNode]doneChan

	// errs is the set of error seen since the last call to Wait. Referred to as
	// the current "parallel batch's error-set". This will be populated by any
	// errors returned from the exec functions passed to Add.
	errs []error

	mu           syncutil.Mutex
	runningGroup sync.WaitGroup
}

// doneChan is a channel that is closed when a plan finishes execution.
type doneChan chan struct{}

// MakeParallelizeQueue creates a new empty ParallelizeQueue that uses the provided
// DependencyAnalyzer to determine plan dependencies.
func MakeParallelizeQueue(analyzer DependencyAnalyzer) ParallelizeQueue {
	return ParallelizeQueue{
		plans:    make(map[planNode]doneChan),
		analyzer: analyzer,
	}
}

// Add inserts the current plan in the queue and executes the provided function
// when all plans that it depends on have completed successfully, obeying the
// guarantees made by the ParallelizeQueue above.
//
// Add should not be called concurrently with Wait. See Wait's comment for more
// details.
func (pq *ParallelizeQueue) Add(params runParams, exec func() error) error {
	prereqs, finishLocked, err := pq.insertInQueue(params)
	if err != nil {
		params.p.curPlan.close(params.ctx)
		return err
	}

	pq.runningGroup.Add(1)
	go func() {
		defer pq.runningGroup.Done()
		// Be careful to close the planner's top-level node, and not just
		// the hash key retrieved above -- there may be more work done by
		// this close() method than plan.Close().
		defer params.p.curPlan.close(params.ctx)

		// Block on the execution of each prerequisite plan blocking us.
		for _, prereq := range prereqs {
			<-prereq
		}

		// Don't bother executing if an error has already been set.
		if abort := func() bool {
			pq.mu.Lock()
			defer pq.mu.Unlock()
			if len(pq.errs) > 0 {
				finishLocked()
				return true
			}
			return false
		}(); abort {
			return
		}

		// Execute the plan.
		// No need to give it the plan it needs to operate on -- it will
		// know about the current plan from its captured planner
		// reference.
		err := exec()

		pq.mu.Lock()
		if err != nil {
			pq.errs = append(pq.errs, err)
		}
		finishLocked()
		pq.mu.Unlock()
	}()
	return nil
}

// insertInQueue inserts the planNode in the queue. It returns a list of the "done"
// channels of prerequisite blocking the new plan from executing. It also returns a
// function to call when the new plan has finished executing. This function must be
// called while pq.mu is held.
func (pq *ParallelizeQueue) insertInQueue(params runParams) ([]doneChan, func(), error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	// Determine the set of prerequisite plans.
	prereqs, err := pq.prereqsForPlanLocked(params)
	if err != nil {
		return nil, nil, err
	}

	// Insert the current plan in running set.
	newPlan := params.p.curPlan.plan
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
	return prereqs, finish, nil
}

// prereqsForPlanLocked determines the set of plans currently running and pending
// that a new plan is dependent on. It returns a slice of doneChans for each plan
// in this set. Returns a nil slice if the plan has no prerequisites and can be run
// immediately.
func (pq *ParallelizeQueue) prereqsForPlanLocked(params runParams) ([]doneChan, error) {
	// First, submit the planNode to the analyzer for analysis. This assures
	// that the analysis takes place before the plan is executed, even if
	// no calls to analyzer.Independent are necessary at this time.
	if err := pq.analyzer.Analyze(params); err != nil {
		return nil, err
	}

	// Add all plans from the plan set that this new plan is dependent on.
	var prereqs []doneChan
	newPlan := params.p.curPlan.plan
	for plan, doneChan := range pq.plans {
		if !pq.analyzer.Independent(plan, newPlan) {
			prereqs = append(prereqs, doneChan)
		}
	}
	return prereqs, nil
}

// Len returns the number of plans in the ParallelizeQueue.
func (pq *ParallelizeQueue) Len() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.plans)
}

// Wait blocks until the ParallelizeQueue finishes executing all plans. It then
// returns the error-set of the last batch of parallelized execution before reseting
// the error-set to allow for future use.
//
// Wait can not be called concurrently with Add. If we need to lift this
// restriction, consider replacing the sync.WaitGroup with a syncutil.RWMutex,
// which will provide the desired starvation and ordering properties. Those
// being that once Wait is called, future Adds will not be reordered ahead
// of Waits attempts to drain all running and pending plans.
func (pq *ParallelizeQueue) Wait() []error {
	pq.runningGroup.Wait()

	// There is no race condition between waiting on the WaitGroup and locking
	// the mutex because ParallelizeQueue.Wait cannot be called concurrently with
	// Add. We lock only because Errs may be called concurrently.
	pq.mu.Lock()
	defer pq.mu.Unlock()
	errs := pq.errs
	pq.errs = nil
	return errs
}

// Errs returns the ParallelizeQueue's error-set.
func (pq *ParallelizeQueue) Errs() []error {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return pq.errs
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
	// independence decisions about the current plan. It must be called before
	// calling Independent for each planNode, and the current plan provided must
	// not be running when Analyze is called. Analyze is allowed to mutate the
	// provided planner if necessary.
	Analyze(runParams) error
	// Independent determines if the provided planNodess are independent from
	// one another. Either planNode may be running when Independent is called,
	// so the method will not modify the plans in any way. Implementations of
	// Independent are always commutative.
	Independent(planNode, planNode) bool
	// Clear is a hint to the DependencyAnalyzer that the provided plan will
	// no longer be needed. It is useful for DependencyAnalyzers that cache
	// state on the planNodes during Analyze.
	Clear(planNode)
}

var _ DependencyAnalyzer = dependencyAnalyzerFunc(nil)
var _ DependencyAnalyzer = &spanBasedDependencyAnalyzer{}

// dependencyAnalyzerFunc is an implementation of DependencyAnalyzer that defers
// to a function for all dependency decisions.
type dependencyAnalyzerFunc func(planNode, planNode) bool

func (f dependencyAnalyzerFunc) Independent(p1 planNode, p2 planNode) bool {
	return f(p1, p2)
}

func (f dependencyAnalyzerFunc) Analyze(_ runParams) error { return nil }
func (f dependencyAnalyzerFunc) Clear(_ planNode)          {}

// NoDependenciesAnalyzer is a DependencyAnalyzer that performs no analysis on
// planNodes and asserts that all plans are independent.
var NoDependenciesAnalyzer DependencyAnalyzer = dependencyAnalyzerFunc(func(
	_ planNode, _ planNode,
) bool {
	return true
})

// planAnalysis holds the read and write spans that a planNode will touch during
// execution, along with other information necessary to determine plan independence.
type planAnalysis struct {
	read          interval.RangeGroup
	write         interval.RangeGroup
	hasOrderingFn bool
}

// spanBasedDependencyAnalyzer determines planNode independence based off of
// the read and write spans that a pair of planNodes interact with. The
// implementation of DependencyAnalyzer expects all planNodes to implement the
// spanCollector interface, and will panic if they do not.
type spanBasedDependencyAnalyzer struct {
	// spanCache caches the analysis results of all active planNodes, so
	// that the analysis only needs to be performed once.
	analysisCache map[planNode]planAnalysis
}

// NewSpanBasedDependencyAnalyzer creates a new SpanBasedDependencyAnalyzer.
func NewSpanBasedDependencyAnalyzer() DependencyAnalyzer {
	return &spanBasedDependencyAnalyzer{
		analysisCache: make(map[planNode]planAnalysis),
	}
}

func (a *spanBasedDependencyAnalyzer) Analyze(params runParams) error {
	p := params.p.curPlan.plan
	readSpans, writeSpans, err := params.p.curPlan.collectSpans(params)
	if err != nil {
		return err
	}
	hasOrderingFn := containsOrderingFunction(params.ctx, p)
	analysis := planAnalysis{
		read:          rangeGroupFromSpans(readSpans),
		write:         rangeGroupFromSpans(writeSpans),
		hasOrderingFn: hasOrderingFn,
	}
	a.analysisCache[p] = analysis
	return nil
}

func (a *spanBasedDependencyAnalyzer) Independent(p1 planNode, p2 planNode) bool {
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

func (a *spanBasedDependencyAnalyzer) Clear(p planNode) {
	delete(a.analysisCache, p)
}

// orderingFunctions is a set of all functions that preclude statement independence.
// These functions are contractually monotonic within a transaction and thus prevents
// reordering.
var orderingFunctions = map[string]struct{}{
	"statement_timestamp": {},
}

func containsOrderingFunction(ctx context.Context, plan planNode) bool {
	sawOrderingFn := false
	po := planObserver{expr: func(_, _ string, n int, expr tree.Expr) {
		if f, ok := expr.(*tree.FuncExpr); ok {
			if _, ok := orderingFunctions[f.Func.String()]; ok {
				sawOrderingFn = true
			}
		}
	}}
	if err := walkPlan(ctx, plan, po); err != nil {
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
func IsStmtParallelized(stmt Statement) bool {
	parallelizedRetClause := func(ret tree.ReturningClause) bool {
		_, ok := ret.(*tree.ReturningNothing)
		return ok
	}
	switch s := stmt.AST.(type) {
	case *tree.Delete:
		return parallelizedRetClause(s.Returning)
	case *tree.Insert:
		return parallelizedRetClause(s.Returning)
	case *tree.Update:
		return parallelizedRetClause(s.Returning)
	}
	return false
}
