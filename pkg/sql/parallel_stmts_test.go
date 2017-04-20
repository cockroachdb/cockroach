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
	"fmt"
	"reflect"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func newPlanNode() planNode {
	return &emptyNode{}
}

// assertLen asserts the number of plans in the ParallelizeQueue.
func assertLen(t *testing.T, pq *ParallelizeQueue, exp int) {
	if l := pq.Len(); l != exp {
		t.Errorf("expected plan count of %d, found %d", exp, l)
	}
}

// assertLenEventually is like assertLen, but can be used in racy situations
// where proper synchronization can not be performed.
func assertLenEventually(t *testing.T, pq *ParallelizeQueue, exp int) {
	testutils.SucceedsSoon(t, func() error {
		if l := pq.Len(); l != exp {
			return errors.Errorf("expected plan count of %d, found %d", exp, l)
		}
		return nil
	})
}

// waitAndAssertEmptyWithErr waits for the ParallelizeQueue to drain, then asserts
// that the queue is empty. It returns the error produced by ParallelizeQueue.Wait.
func waitAndAssertEmptyWithErr(t *testing.T, pq *ParallelizeQueue) error {
	err := pq.Wait()
	if l := pq.Len(); l != 0 {
		t.Errorf("expected empty ParallelizeQueue, found %d plans remaining", l)
	}
	return err
}

func waitAndAssertEmpty(t *testing.T, pq *ParallelizeQueue) {
	if err := waitAndAssertEmptyWithErr(t, pq); err != nil {
		t.Fatalf("unexpected error waiting for ParallelizeQueue to drain: %v", err)
	}
}

// TestParallelizeQueueNoDependencies tests three plans run through a ParallelizeQueue
// when none of the plans are dependent on each other. Because of their independence,
// we use channels to guarantee deterministic execution.
func TestParallelizeQueueNoDependencies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	var res []int
	run1, run2, run3 := make(chan struct{}), make(chan struct{}), make(chan struct{})

	// Executes: plan3 -> plan1 -> plan2.
	pq := MakeParallelizeQueue(NoDependenciesAnalyzer)
	pq.Add(ctx, newPlanNode(), func(plan planNode) error {
		<-run1
		res = append(res, 1)
		assertLen(t, &pq, 3)
		close(run3)
		return nil
	})
	pq.Add(ctx, newPlanNode(), func(plan planNode) error {
		<-run2
		res = append(res, 2)
		assertLenEventually(t, &pq, 1)
		return nil
	})
	pq.Add(ctx, newPlanNode(), func(plan planNode) error {
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
		t.Fatalf("expected parallel execution side effects %v, found %v", exp, res)
	}
}

// TestParallelizeQueueAllDependent tests three plans run through a ParallelizeQueue
// when all of the plans are dependent on each other. Because of their dependence, we
// need no extra synchronization to guarantee deterministic execution.
func TestParallelizeQueueAllDependent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	var res []int
	run := make(chan struct{})
	analyzer := dependencyAnalyzerFunc(func(p1 planNode, p2 planNode) bool {
		return false
	})

	// Executes: plan1 -> plan2 -> plan3.
	pq := MakeParallelizeQueue(analyzer)
	pq.Add(ctx, newPlanNode(), func(plan planNode) error {
		<-run
		res = append(res, 1)
		assertLen(t, &pq, 3)
		return nil
	})
	pq.Add(ctx, newPlanNode(), func(plan planNode) error {
		res = append(res, 2)
		assertLen(t, &pq, 2)
		return nil
	})
	pq.Add(ctx, newPlanNode(), func(plan planNode) error {
		res = append(res, 3)
		assertLen(t, &pq, 1)
		return nil
	})
	close(run)

	waitAndAssertEmpty(t, &pq)
	exp := []int{1, 2, 3}
	if !reflect.DeepEqual(res, exp) {
		t.Fatalf("expected parallel execution side effects %v, found %v", exp, res)
	}
}

// TestParallelizeQueueSingleDependency tests three plans where one is dependent on
// another. Because one plan is dependent, it will be held in the pending queue
// until the prerequisite plan completes execution.
func TestParallelizeQueueSingleDependency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

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
	pq := MakeParallelizeQueue(analyzer)
	pq.Add(ctx, plan1, func(plan planNode) error {
		<-run1
		res = append(res, 1)
		assertLenEventually(t, &pq, 2)
		return nil
	})
	pq.Add(ctx, plan2, func(plan planNode) error {
		res = append(res, 2)
		assertLen(t, &pq, 1)
		return nil
	})
	pq.Add(ctx, plan3, func(plan planNode) error {
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
		t.Fatalf("expected parallel execution side effects %v, found %v", exp, res)
	}
}

// TestParallelizeQueueError tests three plans where one is dependent on another
// and the prerequisite plan throws an error.
func TestParallelizeQueueError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

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
	pq := MakeParallelizeQueue(analyzer)
	pq.Add(ctx, plan1, func(plan planNode) error {
		<-run1
		res = append(res, 1)
		assertLenEventually(t, &pq, 2)
		return planErr
	})
	pq.Add(ctx, plan2, func(plan planNode) error {
		// Should never be called. We assert this using the res slice, because
		// we can't call t.Fatalf in a different goroutine.
		res = append(res, 2)
		return nil
	})
	pq.Add(ctx, plan3, func(plan planNode) error {
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
		t.Fatalf("expected parallel execution side effects %v, found %v", exp, res)
	}
}

// TestParallelizeQueueAddAfterError tests that if a plan is added to a ParallelizeQueue
// after an error has been produced but before Wait has been called, that the plan
// will never be run. It then tests that once Wait has been called, the error state
// will be cleared.
func TestParallelizeQueueAddAfterError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	var res []int
	plan1, plan2, plan3 := newPlanNode(), newPlanNode(), newPlanNode()
	planErr := errors.Errorf("plan1 will throw this error")

	// Executes: plan1 (error!) -> plan2 (dropped) -> plan3.
	pq := MakeParallelizeQueue(NoDependenciesAnalyzer)
	pq.Add(ctx, plan1, func(plan planNode) error {
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

	pq.Add(ctx, plan2, func(plan planNode) error {
		// Should never be called. We assert this using the res slice, because
		// we can't call t.Fatalf in a different goroutine.
		res = append(res, 2)
		return nil
	})

	// Wait for the ParallelizeQueue to clear and assert that we see the
	// correct error.
	resErr := waitAndAssertEmptyWithErr(t, &pq)
	if resErr != planErr {
		t.Fatalf("expected plan1 to throw error %v, found %v", planErr, resErr)
	}

	pq.Add(ctx, plan3, func(plan planNode) error {
		// Will be called, because the error is cleared when Wait is called.
		res = append(res, 3)
		assertLen(t, &pq, 1)
		return nil
	})

	waitAndAssertEmpty(t, &pq)
	exp := []int{1, 3}
	if !reflect.DeepEqual(res, exp) {
		t.Fatalf("expected parallel execution side effects %v, found %v", exp, res)
	}
}

func planNodeForQuery(
	t *testing.T, s serverutils.TestServerInterface, sql string,
) (planNode, func()) {
	kvDB := s.KVClient().(*client.DB)
	txn := client.NewTxn(kvDB)
	txn.Proto().OrigTimestamp = s.Clock().Now()
	p := makeInternalPlanner("plan", txn, security.RootUser, &MemoryMetrics{})
	p.session.leases.leaseMgr = s.LeaseManager().(*LeaseManager)
	p.session.Database = "test"

	stmts, err := p.parser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected to parse 1 statement, got: %d", len(stmts))
	}
	stmt := stmts[0]
	plan, err := p.makePlan(context.TODO(), stmt, false /* autoCommit */)
	if err != nil {
		t.Fatal(err)
	}
	return plan, func() {
		finishInternalPlanner(p)
	}
}

func TestSpanBasedDependencyAnalyzer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	if _, err := db.Exec(`CREATE DATABASE test`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`SET DATABASE = test`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE foo (k INT PRIMARY KEY)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`
		CREATE TABLE bar (
			k INT PRIMARY KEY, 
			v INT, 
			a INT, 
			UNIQUE INDEX idx(v)
		)
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE fks (f INT REFERENCES foo)`); err != nil {
		t.Fatal(err)
	}

	for _, test := range []struct {
		query1, query2 string
		independent    bool
	}{
		// The dependency analyzer is only used for RETURNING NOTHING statements
		// at the moment, but it should work on all statement types.
		{`SELECT * FROM foo`, `SELECT * FROM bar`, true},
		{`SELECT * FROM foo`, `SELECT * FROM bar@idx`, true},
		{`SELECT * FROM foo`, `SELECT * FROM foo`, true},
		{`SELECT * FROM foo`, `SELECT * FROM fks`, true},

		{`DELETE FROM foo`, `DELETE FROM bar`, true},
		{`DELETE FROM foo`, `DELETE FROM foo`, false},
		{`DELETE FROM foo`, `DELETE FROM bar WHERE (SELECT k = 1 FROM foo)`, false},
		{`DELETE FROM foo`, `DELETE FROM bar WHERE (SELECT f = 1 FROM fks)`, true},
		{`DELETE FROM foo`, `DELETE FROM fks`, false},
		{`DELETE FROM bar`, `DELETE FROM fks`, true},
		{`DELETE FROM foo`, `SELECT * FROM foo`, false},
		{`DELETE FROM foo`, `SELECT * FROM bar`, true},
		{`DELETE FROM bar`, `SELECT * FROM bar`, false},
		{`DELETE FROM bar`, `SELECT * FROM bar@idx`, false},

		{`INSERT INTO foo VALUES (1)`, `INSERT INTO bar VALUES (1)`, true},
		{`INSERT INTO foo VALUES (1)`, `INSERT INTO foo VALUES (1)`, false},
		{`INSERT INTO foo VALUES (1)`, `INSERT INTO bar SELECT k FROM foo`, false},
		{`INSERT INTO foo VALUES (1)`, `INSERT INTO bar SELECT f FROM fks`, true},
		{`INSERT INTO foo VALUES (1)`, `INSERT INTO fks VALUES (1)`, false},
		{`INSERT INTO bar VALUES (1)`, `INSERT INTO fks VALUES (1)`, true},
		{`INSERT INTO foo VALUES (1)`, `SELECT * FROM foo`, false},
		{`INSERT INTO foo VALUES (1)`, `SELECT * FROM bar`, true},
		{`INSERT INTO bar VALUES (1)`, `SELECT * FROM bar`, false},
		{`INSERT INTO bar VALUES (1)`, `SELECT * FROM bar@idx`, false},
		{`INSERT INTO foo VALUES (1)`, `DELETE FROM foo`, false},
		{`INSERT INTO foo VALUES (1)`, `DELETE FROM bar`, true},

		{`UPDATE foo SET k = 1`, `UPDATE bar SET k = 1`, true},
		{`UPDATE foo SET k = 1`, `UPDATE foo SET k = 1`, false},
		{`UPDATE foo SET k = 1`, `UPDATE bar SET k = (SELECT k FROM foo)`, false},
		{`UPDATE foo SET k = 1`, `UPDATE bar SET k = (SELECT f FROM fks)`, true},
		{`UPDATE foo SET k = 1`, `INSERT INTO fks VALUES (1)`, false},
		{`UPDATE bar SET k = 1`, `INSERT INTO fks VALUES (1)`, true},
		{`UPDATE foo SET k = 1`, `SELECT * FROM foo`, false},
		{`UPDATE foo SET k = 1`, `SELECT * FROM bar`, true},
		{`UPDATE bar SET k = 1`, `SELECT * FROM bar`, false},
		{`UPDATE bar SET k = 1`, `SELECT * FROM bar@idx`, false},
		{`UPDATE foo SET k = 1`, `DELETE FROM foo`, false},
		{`UPDATE foo SET k = 1`, `DELETE FROM bar`, true},

		// Statements like statement_timestamp enforce a strict ordering on
		// statements, restricting reordering and thus independence.
		{`SELECT * FROM foo`, `SELECT *, statement_timestamp() FROM bar`, false},
		{`DELETE FROM foo`, `DELETE FROM bar WHERE '2015-10-01'::TIMESTAMP = statement_timestamp()`, true},
	} {
		for _, reverse := range []bool{false, true} {
			q1, q2 := test.query1, test.query2
			if reverse {
				// Verify commutativity.
				q1, q2 = q2, q1
			}

			name := fmt.Sprintf("%s | %s", q1, q2)
			t.Run(name, func(t *testing.T) {
				da := NewSpanBasedDependencyAnalyzer()

				plan1, finish1 := planNodeForQuery(t, s, q1)
				defer finish1()
				plan2, finish2 := planNodeForQuery(t, s, q2)
				defer finish2()

				indep := da.Independent(context.TODO(), plan1, plan2)
				if exp := test.independent; indep != exp {
					t.Errorf("expected da.Independent(%q, %q) = %t, but found %t",
						q1, q2, exp, indep)
				}
			})
		}
	}
}
