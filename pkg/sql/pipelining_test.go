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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func newPlanNode() planNode {
	return &emptyNode{}
}

// assertLen asserts the number of plans in the PipelineQueue.
func assertLen(t *testing.T, pq *PipelineQueue, exp int) {
	if l := pq.Len(); l != exp {
		t.Errorf("expected plan count of %d, found %d", exp, l)
	}
}

// assertLenEventually is like assertLen, but can be used in racy situations
// where proper synchronization can not be performed.
func assertLenEventually(t *testing.T, pq *PipelineQueue, exp int) {
	testutils.SucceedsSoon(t, func() error {
		if l := pq.Len(); l != exp {
			return errors.Errorf("expected plan count of %d, found %d", exp, l)
		}
		return nil
	})
}

// waitAndAssertEmptyWithErr waits for the PipelineQueue to drain, then asserts
// that the queue is empty. It returns the error produced by PipelineQueue.Wait.
func waitAndAssertEmptyWithErr(t *testing.T, pq *PipelineQueue) error {
	err := pq.Wait()
	if l := pq.Len(); l != 0 {
		t.Errorf("expected empty PipelineQueue, found %d plans remaining", l)
	}
	return err
}

func waitAndAssertEmpty(t *testing.T, pq *PipelineQueue) {
	if err := waitAndAssertEmptyWithErr(t, pq); err != nil {
		t.Fatalf("unexpected error waiting for pipeline queue to drain: %v", err)
	}
}

// TestPipelineQueueNoDependencies tests three plans run through a PipelineQueue when
// none of the plans are dependent on each other. Because of their independence, we
// use channels to guarantee deterministic execution.
func TestPipelineQueueNoDependencies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var res []int
	run1, run2, run3 := make(chan struct{}), make(chan struct{}), make(chan struct{})

	// Executes: plan3 -> plan1 -> plan2.
	pq := MakePipelineQueue(NoDependenciesAnalyzer)
	pq.Add(newPlanNode(), func(plan planNode) error {
		<-run1
		res = append(res, 1)
		assertLen(t, &pq, 3)
		close(run3)
		return nil
	})
	pq.Add(newPlanNode(), func(plan planNode) error {
		<-run2
		res = append(res, 2)
		assertLenEventually(t, &pq, 1)
		return nil
	})
	pq.Add(newPlanNode(), func(plan planNode) error {
		<-run3
		res = append(res, 3)
		assertLenEventually(t, &pq, 2)
		close(run2)
		return nil
	})
	close(run1)

	waitAndAssertEmpty(t, &pq)
	exp := []int{1, 3, 2}
	if !reflect.DeepEqual(res, exp) {
		t.Fatalf("expected pipeline side effects %v, found %v", exp, res)
	}
}

// TestPipelineQueueAllDependent tests three plans run through a PipelineQueue when
// all of the plans are dependent on each other. Because of their dependence, we
// need no extra synchronization to guarantee deterministic execution.
func TestPipelineQueueAllDependent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var res []int
	run := make(chan struct{})
	analyzer := dependencyAnalyzerFunc(func(p1 planNode, p2 planNode) bool {
		return false
	})

	// Executes: plan1 -> plan2 -> plan3.
	pq := MakePipelineQueue(analyzer)
	pq.Add(newPlanNode(), func(plan planNode) error {
		<-run
		res = append(res, 1)
		assertLen(t, &pq, 3)
		return nil
	})
	pq.Add(newPlanNode(), func(plan planNode) error {
		res = append(res, 2)
		assertLen(t, &pq, 2)
		return nil
	})
	pq.Add(newPlanNode(), func(plan planNode) error {
		res = append(res, 3)
		assertLen(t, &pq, 1)
		return nil
	})
	close(run)

	waitAndAssertEmpty(t, &pq)
	exp := []int{1, 2, 3}
	if !reflect.DeepEqual(res, exp) {
		t.Fatalf("expected pipeline side effects %v, found %v", exp, res)
	}
}

// TestPipelineQueueSingleDependency tests three plans where one is dependent on
// another. Because one plan is dependent, it will be held in the pending queue
// until the prerequisite plan completes execution.
func TestPipelineQueueSingleDependency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var res []int
	plan1, plan2, plan3 := newPlanNode(), newPlanNode(), newPlanNode()
	run1, run3 := make(chan struct{}), make(chan struct{})
	analyzer := dependencyAnalyzerFunc(func(p1 planNode, p2 planNode) bool {
		if (p1 == plan1 && p2 == plan2) || (p1 == plan2 && p2 == plan1) {
			// plan1 and plan2 are dependent
			return false
		}
		return true
	})

	// Executes: plan3 -> plan1 -> plan2.
	pq := MakePipelineQueue(analyzer)
	pq.Add(plan1, func(plan planNode) error {
		<-run1
		res = append(res, 1)
		assertLenEventually(t, &pq, 2)
		return nil
	})
	pq.Add(plan2, func(plan planNode) error {
		res = append(res, 2)
		assertLen(t, &pq, 1)
		return nil
	})
	pq.Add(plan3, func(plan planNode) error {
		<-run3
		res = append(res, 3)
		assertLen(t, &pq, 3)
		close(run1)
		return nil
	})
	close(run3)

	waitAndAssertEmpty(t, &pq)
	exp := []int{3, 1, 2}
	if !reflect.DeepEqual(res, exp) {
		t.Fatalf("expected pipeline side effects %v, found %v", exp, res)
	}
}

// TestPipelineQueueError tests three plans where one is dependent on another
// and the prerequisite plan throws an error.
func TestPipelineQueueError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var res []int
	plan1, plan2, plan3 := newPlanNode(), newPlanNode(), newPlanNode()
	run1, run3 := make(chan struct{}), make(chan struct{})
	planErr := errors.Errorf("plan1 will throw this error")
	analyzer := dependencyAnalyzerFunc(func(p1 planNode, p2 planNode) bool {
		if (p1 == plan1 && p2 == plan2) || (p1 == plan2 && p2 == plan1) {
			// plan1 and plan2 are dependent
			return false
		}
		return true
	})

	// Executes: plan3 -> plan1 (error!) -> plan2 (dropped).
	pq := MakePipelineQueue(analyzer)
	pq.Add(plan1, func(plan planNode) error {
		<-run1
		res = append(res, 1)
		assertLenEventually(t, &pq, 2)
		return planErr
	})
	pq.Add(plan2, func(plan planNode) error {
		// Should never be called. We assert this using the res slice, because
		// we can't call t.Fatalf in a different goroutine.
		res = append(res, 2)
		return nil
	})
	pq.Add(plan3, func(plan planNode) error {
		<-run3
		res = append(res, 3)
		assertLen(t, &pq, 3)
		close(run1)
		return nil
	})
	close(run3)

	resErr := waitAndAssertEmptyWithErr(t, &pq)
	if resErr != planErr {
		t.Fatalf("expected plan1 to throw error %v, found %v", planErr, resErr)
	}

	exp := []int{3, 1}
	if !reflect.DeepEqual(res, exp) {
		t.Fatalf("expected pipeline side effects %v, found %v", exp, res)
	}
}

// TestPipelineQueueAddAfterError tests that if a plan is added to a PipelineQueue
// after an error has been produced but before Wait has been called, that the plan
// will never be run. It then tests that once Wait has been called, the error state
// will be cleared.
func TestPipelineQueueAddAfterError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var res []int
	plan1, plan2, plan3 := newPlanNode(), newPlanNode(), newPlanNode()
	planErr := errors.Errorf("plan1 will throw this error")

	// Executes: plan1 (error!) -> plan2 (dropped) -> plan3.
	pq := MakePipelineQueue(NoDependenciesAnalyzer)
	pq.Add(plan1, func(plan planNode) error {
		res = append(res, 1)
		assertLen(t, &pq, 1)
		return planErr
	})
	testutils.SucceedsSoon(t, func() error {
		// We need this, because any signal from within plan1's execution could
		// race with the beginning of plan2.
		if pqErr := pq.Err(); pqErr == nil {
			return errors.Errorf("plan1 not yet run")
		}
		return nil
	})

	pq.Add(plan2, func(plan planNode) error {
		// Should never be called. We assert this using the res slice, because
		// we can't call t.Fatalf in a different goroutine.
		res = append(res, 2)
		return nil
	})

	// Wait for the pipeline queue to clear and assert that we see the
	// correct error.
	resErr := waitAndAssertEmptyWithErr(t, &pq)
	if resErr != planErr {
		t.Fatalf("expected plan1 to throw error %v, found %v", planErr, resErr)
	}

	pq.Add(plan3, func(plan planNode) error {
		// Will be called, because the error is cleared when Wait is called.
		res = append(res, 3)
		assertLen(t, &pq, 1)
		return nil
	})

	waitAndAssertEmpty(t, &pq)
	exp := []int{1, 3}
	if !reflect.DeepEqual(res, exp) {
		t.Fatalf("expected pipeline side effects %v, found %v", exp, res)
	}
}
