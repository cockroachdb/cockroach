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
	"fmt"
	"reflect"
	"testing"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func newPlanNode() planNode {
	return newZeroNode(nil /* columns */)
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

// waitAndAssertEmptyWithErrs waits for the ParallelizeQueue to drain, then asserts
// that the queue is empty. It returns the errors produced by ParallelizeQueue.Wait.
func waitAndAssertEmptyWithErrs(t *testing.T, pq *ParallelizeQueue) []error {
	errs := pq.Wait()
	if l := pq.Len(); l != 0 {
		t.Errorf("expected empty ParallelizeQueue, found %d plans remaining", l)
	}
	return errs
}

func waitAndAssertEmpty(t *testing.T, pq *ParallelizeQueue) {
	if errs := waitAndAssertEmptyWithErrs(t, pq); len(errs) > 0 {
		t.Fatalf("unexpected errors waiting for ParallelizeQueue to drain: %v", errs)
	}
}

func (pq *ParallelizeQueue) MustAdd(t *testing.T, plan planNode, exec func() error) {
	p := makeTestPlanner()
	params := runParams{
		ctx:             context.TODO(),
		p:               p,
		extendedEvalCtx: &p.extendedEvalCtx,
	}
	params.p.curPlan.plan = plan
	if err := pq.Add(params, exec); err != nil {
		t.Fatalf("ParallelizeQueue.Add failed: %v", err)
	}
}

// TestParallelizeQueueNoDependencies tests three plans run through a ParallelizeQueue
// when none of the plans are dependent on each other. Because of their independence,
// we use channels to guarantee deterministic execution.
func TestParallelizeQueueNoDependencies(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var res []int
	run1, run2, run3 := make(chan struct{}), make(chan struct{}), make(chan struct{})

	// Executes: plan3 -> plan1 -> plan2.
	pq := MakeParallelizeQueue(NoDependenciesAnalyzer)
	pq.MustAdd(t, newPlanNode(), func() error {
		<-run1
		res = append(res, 1)
		assertLen(t, &pq, 3)
		close(run3)
		return nil
	})
	pq.MustAdd(t, newPlanNode(), func() error {
		<-run2
		res = append(res, 2)
		assertLenEventually(t, &pq, 1)
		return nil
	})
	pq.MustAdd(t, newPlanNode(), func() error {
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

	var res []int
	run := make(chan struct{})
	analyzer := dependencyAnalyzerFunc(func(p1 planNode, p2 planNode) bool {
		return false
	})

	// Executes: plan1 -> plan2 -> plan3.
	pq := MakeParallelizeQueue(analyzer)
	pq.MustAdd(t, newPlanNode(), func() error {
		<-run
		res = append(res, 1)
		assertLen(t, &pq, 3)
		return nil
	})
	pq.MustAdd(t, newPlanNode(), func() error {
		res = append(res, 2)
		assertLen(t, &pq, 2)
		return nil
	})
	pq.MustAdd(t, newPlanNode(), func() error {
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
	pq.MustAdd(t, plan1, func() error {
		<-run1
		res = append(res, 1)
		assertLenEventually(t, &pq, 2)
		return nil
	})
	pq.MustAdd(t, plan2, func() error {
		res = append(res, 2)
		assertLen(t, &pq, 1)
		return nil
	})
	pq.MustAdd(t, plan3, func() error {
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
	pq.MustAdd(t, plan1, func() error {
		<-run1
		res = append(res, 1)
		assertLenEventually(t, &pq, 2)
		return planErr
	})
	pq.MustAdd(t, plan2, func() error {
		// Should never be called. We assert this using the res slice, because
		// we can't call t.Fatalf in a different goroutine.
		res = append(res, 2)
		return nil
	})
	pq.MustAdd(t, plan3, func() error {
		<-run3
		res = append(res, 3)
		assertLen(t, &pq, 3)
		close(run1)
		return nil
	})
	close(run3)

	resErrs := waitAndAssertEmptyWithErrs(t, &pq)
	if len(resErrs) != 1 || resErrs[0] != planErr {
		t.Fatalf("expected plan1 to throw error %v, found %v", planErr, resErrs)
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

	var res []int
	plan1, plan2, plan3 := newPlanNode(), newPlanNode(), newPlanNode()
	planErr := errors.Errorf("plan1 will throw this error")

	// Executes: plan1 (error!) -> plan2 (dropped) -> plan3.
	pq := MakeParallelizeQueue(NoDependenciesAnalyzer)
	pq.MustAdd(t, plan1, func() error {
		res = append(res, 1)
		assertLen(t, &pq, 1)
		return planErr
	})
	testutils.SucceedsSoon(t, func() error {
		// We need this, because any signal from within plan1's execution could
		// race with the beginning of plan2.
		if pqErrs := pq.Errs(); len(pqErrs) == 0 {
			return errors.Errorf("plan1 not yet run")
		}
		return nil
	})

	pq.MustAdd(t, plan2, func() error {
		// Should never be called. We assert this using the res slice, because
		// we can't call t.Fatalf in a different goroutine.
		res = append(res, 2)
		return nil
	})

	// Wait for the ParallelizeQueue to clear and assert that we see the
	// correct error.
	resErrs := waitAndAssertEmptyWithErrs(t, &pq)
	if len(resErrs) != 1 || resErrs[0] != planErr {
		t.Fatalf("expected plan1 to throw error %v, found %v", planErr, resErrs)
	}

	pq.MustAdd(t, plan3, func() error {
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

func planQuery(t *testing.T, s serverutils.TestServerInterface, sql string) (*planner, func()) {
	kvDB := s.DB()
	txn := client.NewTxn(kvDB, s.NodeID(), client.RootTxn)
	p, cleanup := newInternalPlanner(
		"plan", txn, security.RootUser, &MemoryMetrics{}, &s.Executor().(*Executor).cfg)
	p.extendedEvalCtx.Tables.leaseMgr = s.LeaseManager().(*LeaseManager)
	// HACK: we're mutating the SessionData directly, without going through the
	// SessionDataMutator. We're not really supposed to do that, but were we to go
	// through the mutator, we'd need a session and then we'd have to reset the
	// planner with the session so that its copy of the session data gets updated.
	p.extendedEvalCtx.SessionData.Database = "test"

	stmts, err := p.parser.Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if len(stmts) != 1 {
		t.Fatalf("expected to parse 1 statement, got: %d", len(stmts))
	}
	stmt := stmts[0]
	if err := p.makePlan(context.TODO(), Statement{AST: stmt}); err != nil {
		t.Fatal(err)
	}
	return p, func() {
		p.curPlan.close(context.TODO())
		cleanup()
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
			k INT PRIMARY KEY DEFAULT 0,
			v INT DEFAULT 1,
			a INT,
			UNIQUE INDEX idx(v)
		)
	`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE fks (f INT REFERENCES foo)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE baz (a INT DEFAULT 1)`); err != nil {
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

		{`DELETE FROM bar`, `WITH a AS (DELETE FROM bar RETURNING v) SELECT EXISTS(SELECT * FROM a) AS e`, false},
		{`DELETE FROM bar`, `SELECT EXISTS(SELECT * FROM [DELETE FROM bar RETURNING v]) AS e`, false},
		{`SELECT EXISTS(SELECT * FROM [DELETE FROM foo WHERE k = 1 RETURNING k]) AS e`,
			`SELECT EXISTS(SELECT * FROM [DELETE FROM bar WHERE k = 1 RETURNING v]) AS e`, true},

		{`INSERT INTO foo VALUES (1)`, `INSERT INTO bar VALUES (1)`, true},
		{`INSERT INTO foo VALUES (1)`, `INSERT INTO bar SELECT k FROM foo`, false},
		{`INSERT INTO foo VALUES (1)`, `INSERT INTO bar SELECT f FROM fks`, true},
		{`INSERT INTO bar VALUES (1)`, `INSERT INTO fks VALUES (1)`, true},
		{`INSERT INTO foo VALUES (1)`, `SELECT * FROM foo`, false},
		{`INSERT INTO foo VALUES (1)`, `SELECT * FROM bar`, true},
		{`INSERT INTO bar VALUES (1)`, `SELECT * FROM bar`, false},
		{`INSERT INTO bar VALUES (1)`, `SELECT * FROM bar@idx`, false},
		{`INSERT INTO foo VALUES (1)`, `DELETE FROM foo`, false},
		{`INSERT INTO foo VALUES (1)`, `DELETE FROM bar`, true},

		// INSERT ... VALUES statements are special-cased with tighter span
		// analysis to allow inserts into the same table to be independent.
		{`INSERT INTO foo VALUES (1)`, `INSERT INTO foo VALUES (1)`, false},
		{`INSERT INTO foo VALUES (1)`, `INSERT INTO foo VALUES (2)`, true},
		// Subqueries can be arbitrarily-complex, so they don't work.
		{`INSERT INTO foo VALUES (1)`, `INSERT INTO foo SELECT 2`, false},
		{`INSERT INTO foo VALUES (1)`, `INSERT INTO foo SELECT 2 FROM foo`, false},
		{`INSERT INTO foo VALUES (1)`, `INSERT INTO foo VALUES ((SELECT 2))`, false},
		// Secondary indexes need to be independent too.
		{`INSERT INTO bar VALUES (1)`, `INSERT INTO bar VALUES (1)`, false},
		{`INSERT INTO bar VALUES (1)`, `INSERT INTO bar VALUES (2)`, false},
		{`INSERT INTO bar VALUES (1, 5)`, `INSERT INTO bar VALUES (2, 5)`, false},
		{`INSERT INTO bar VALUES (1, 5)`, `INSERT INTO bar VALUES (2, 6)`, true},
		{`INSERT INTO bar (v, k) VALUES (1, 5)`, `INSERT INTO bar (v, k) VALUES (2, 5)`, false},
		{`INSERT INTO bar (k, v) VALUES (1, 5)`, `INSERT INTO bar (k, v) VALUES (2, 5)`, false},
		{`INSERT INTO bar (v, k) VALUES (NULL, 5)`, `INSERT INTO bar (v, k) VALUES (NULL, 5)`, false},
		{`INSERT INTO bar (k, v) VALUES (1, NULL)`, `INSERT INTO bar (k, v) VALUES (2, NULL)`, true},
		// DEFAULT VALUES clauses are handled by this special-case as well.
		{`INSERT INTO bar DEFAULT VALUES`, `INSERT INTO bar DEFAULT VALUES`, false},
		{`INSERT INTO baz DEFAULT VALUES`, `INSERT INTO baz DEFAULT VALUES`, true},
		// This also tightens FK span analysis for INSERT ... VALUES statements.
		{`INSERT INTO foo VALUES (1)`, `INSERT INTO fks VALUES (1)`, false},
		{`INSERT INTO foo VALUES (1)`, `INSERT INTO fks VALUES (2)`, true},

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

				planAndAnalyze := func(q string) (planNode, func()) {
					p, finish := planQuery(t, s, q)
					params := runParams{
						ctx:             context.TODO(),
						extendedEvalCtx: &p.extendedEvalCtx,
						p:               p,
					}

					if err := da.Analyze(params); err != nil {
						t.Fatalf("plan analysis failed: %v", err)
					}

					return p.curPlan.plan, func() {
						finish()
						da.Clear(p.curPlan.plan)
					}
				}

				plan1, finish1 := planAndAnalyze(q1)
				plan2, finish2 := planAndAnalyze(q2)
				defer finish1()
				defer finish2()

				indep := da.Independent(plan1, plan2)
				if exp := test.independent; indep != exp {
					t.Errorf("expected da.Independent(%q, %q) = %t, but found %t",
						q1, q2, exp, indep)
				}
			})
		}
	}
}
