// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Regression tests for #22304.
// Checks that a mutation with RETURNING checks low-level constraints
// before returning anything -- or that at least no constraint-violating
// values are visible to the client.
func TestConstraintValidationBeforeBuffering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	params, _ := createTestServerParamsAllowTenants()
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	if _, err := db.Exec(`
CREATE DATABASE d;
CREATE TABLE d.a(a INT PRIMARY KEY);
INSERT INTO d.a(a) VALUES (1);
	`); err != nil {
		t.Fatal(err)
	}

	step1 := func() (*gosql.Rows, error) {
		return db.Query("INSERT INTO d.a(a) TABLE generate_series(1,3000) RETURNING a")
	}
	step2 := func() (*gosql.Rows, error) {
		if _, err := db.Exec(`INSERT INTO d.a(a) TABLE generate_series(2, 3000)`); err != nil {
			return nil, err
		}
		return db.Query("UPDATE d.a SET a = a - 1 WHERE a > 1 RETURNING a")
	}
	for i, step := range []func() (*gosql.Rows, error){step1, step2} {
		rows, err := step()
		if err != nil {
			if !testutils.IsError(err, `duplicate key value`) {
				t.Errorf("%d: %v", i, err)
			}
		} else {
			defer rows.Close()

			hasNext := rows.Next()
			if !hasNext {
				t.Errorf("%d: returning claims to return no error, yet returns no rows either", i)
			} else {
				var val int
				err := rows.Scan(&val)

				if err != nil {
					if !testutils.IsError(err, `duplicate key value`) {
						t.Errorf("%d: %v", i, err)
					}
				} else {
					// No error. Maybe it'll come later.
					if val == 1 {
						t.Errorf("%d: returning returns rows, including an invalid duplicate", i)
					}

					for rows.Next() {
						err := rows.Scan(&val)
						if err != nil {
							if !testutils.IsError(err, `duplicate key value`) {
								t.Errorf("%d: %v", i, err)
							}
						}
						if val == 1 {
							t.Errorf("%d returning returns rows, including an invalid duplicate", i)
						}
					}
				}
			}
		}
	}
}

func TestReadCommittedImplicitPartitionUpsert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test state machine. We use a state cond variable to force a specific interleaving
	// of conflicting writes to test that we detect and retry those writes properly.
	type State int
	const (
		Init         State = iota // Test is initializing.
		Ready                     // Test ready to run.
		ReadDone                  // Arbiter index has been read, but writes haven't started.
		ConflictDone              // Conflicting write has committed.
		Errored                   // Error has occurred in one of the goroutines, so bail out.
	)
	mu := struct {
		l     syncutil.Mutex // Protecting state.
		state State          // Test state.
		c     *sync.Cond
	}{}
	mu.c = sync.NewCond(&mu.l)

	// Wait for a test to reach this state or error.
	waitForState := func(s State) bool {
		mu.l.Lock()
		defer mu.l.Unlock()
		for mu.state != s && mu.state != Errored {
			mu.c.Wait()
		}
		return mu.state == s
	}
	// Set test to the specified state.
	setState := func(s State) {
		mu.l.Lock()
		mu.state = s
		mu.c.Broadcast()
		mu.l.Unlock()
	}

	ctx := context.Background()
	params, _ := createTestServerParamsAllowTenants()
	// If test is in Ready state, transition to ReadDone and wait for conflict.
	params.Knobs = base.TestingKnobs{
		SQLExecutor: &sql.ExecutorTestingKnobs{
			AfterArbiterRead: func() {
				if mu.state != Ready {
					return
				}
				setState(ReadDone)
				_ = waitForState(ConflictDone)
			},
		},
	}
	s, db, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	// Initialize data.
	if _, err := db.Exec(`
SET experimental_enable_implicit_column_partitioning = true;
CREATE DATABASE d;
CREATE TYPE d.reg AS ENUM ('east', 'west', 'north', 'south');
CREATE TABLE d.upsert (
  id INT PRIMARY KEY,
  k INT NOT NULL,
  r d.reg,
  a INT,
  UNIQUE INDEX (k))
PARTITION ALL BY LIST (r) (
  PARTITION e VALUES IN ('east'),
  PARTITION w VALUES IN ('west'),
  PARTITION n VALUES IN ('north'),
  PARTITION s VALUES IN ('south')
);
`); err != nil {
		t.Fatal(err)
	}

	// Create two connections and make them READ COMMITTED.
	var connections [2]*gosql.Conn
	for i := range connections {
		var err error
		connections[i], err = db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := connections[i].Close(); err != nil {
				t.Fatal(err)
			}
		}()
		if _, err = connections[i].ExecContext(ctx, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED"); err != nil {
			t.Fatal(err)
		}
	}

	// Test the three users of arbiter index reads.
	testCases := []struct {
		stmt             string
		conflictingWrite string
		expectedOutput   []string
	}{
		{
			stmt:             "UPSERT INTO d.upsert VALUES (1, 10, 'east', 100)",
			conflictingWrite: "INSERT INTO d.upsert VALUES (1, 10, 'west', 101)",
			expectedOutput:   []string{"1", "10", "east", "100"},
		},
		{
			stmt:             "INSERT INTO d.upsert VALUES (1, 10, 'west', 101) ON CONFLICT DO NOTHING",
			conflictingWrite: "INSERT INTO d.upsert VALUES (1, 10, 'south', 100)",
			expectedOutput:   []string{"1", "10", "south", "100"},
		},
		{
			stmt:             "INSERT INTO d.upsert VALUES (1, 11, 'east', 100) ON CONFLICT (id) DO UPDATE SET a = upsert.a + 1",
			conflictingWrite: "INSERT INTO d.upsert VALUES (1, 10, 'north', 100)",
			expectedOutput:   []string{"1", "10", "north", "101"},
		},
	}

	// Execute the test statement.
	runTestStmt := func(stmt string, wg *sync.WaitGroup, ch chan error) {
		defer wg.Done()
		_, err := connections[0].ExecContext(ctx, stmt)
		if err != nil {
			ch <- err
			setState(Errored)
		}
	}
	// Wait for the arbiter read to be done, then execute the conflicting write.
	runConflictingWrite := func(stmt string, wg *sync.WaitGroup, ch chan error) {
		defer wg.Done()
		if !waitForState(ReadDone) {
			return
		}
		_, err := connections[1].ExecContext(ctx, stmt)
		if err != nil {
			ch <- err
			setState(Errored)
		} else {
			setState(ConflictDone)
		}
	}

	for idx, tc := range testCases {
		fmt.Printf("Starting test %d -- %q\n", idx+1, tc.stmt)
		setState(Ready)

		ch := make(chan error, len(connections))
		wg := sync.WaitGroup{}
		wg.Add(len(connections))

		go runTestStmt(tc.stmt, &wg, ch)
		go runConflictingWrite(tc.conflictingWrite, &wg, ch)

		// Wait for test to complete and read any errors.
		wg.Wait()
		select {
		case err := <-ch:
			t.Fatal(err)
		default:
		}

		// Verifty that the write completed correctly.
		rows, err := db.QueryContext(ctx, "SELECT * FROM d.upsert")
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; rows.Next(); i++ {
			var id, k, r, a string
			if err := rows.Scan(&id, &k, &r, &a); err != nil {
				t.Fatal(err)
			}
			res := []string{id, k, r, a}
			if !reflect.DeepEqual(tc.expectedOutput, res) {
				t.Fatalf("%d: expected %v, got %v", idx, tc.expectedOutput, res)
			}
		}
		rows.Close()

		if _, err := db.Exec(`TRUNCATE TABLE d.upsert`); err != nil {
			t.Fatal(err)
		}
	}
}
