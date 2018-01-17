// Copyright 2016 The Cockroach Authors.
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

package sql_test

import (
	"context"
	gosql "database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestCancelSelectQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const queryToCancel = "SELECT * FROM generate_series(1,20000000)"

	var conn1 *gosql.DB
	var conn2 *gosql.DB

	tc := serverutils.StartTestCluster(t, 2, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
		})
	defer tc.Stopper().Stop(context.TODO())

	conn1 = tc.ServerConn(0)
	conn2 = tc.ServerConn(1)

	sem := make(chan struct{})
	errChan := make(chan error)

	go func() {
		sem <- struct{}{}
		rows, err := conn2.Query(queryToCancel)
		if err != nil {
			errChan <- err
			return
		}
		for rows.Next() {
		}
		if err = rows.Err(); err != nil {
			errChan <- err
		}
	}()

	<-sem
	time.Sleep(time.Second * 2)

	const cancelQuery = "CANCEL QUERY (SELECT query_id FROM [SHOW CLUSTER QUERIES] WHERE node_id = 2)"

	if _, err := conn1.Exec(cancelQuery); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errChan:
		if !sqlbase.IsQueryCanceledError(err) {
			t.Fatal(err)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("no error received from query supposed to be canceled")
	}

}

func TestCancelParallelQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const queryToBlock = "INSERT INTO nums VALUES (1) RETURNING NOTHING;"
	const queryToCancel = "INSERT INTO nums2 VALUES (2) RETURNING NOTHING;"
	const sqlToRun = "BEGIN TRANSACTION; " + queryToBlock + queryToCancel + " COMMIT;"

	// conn1 is used for the txn above. conn2 is solely for the CANCEL statement.
	var conn1 *gosql.DB
	var conn2 *gosql.DB

	// Up to two goroutines could generate errors (one for each query).
	errChan := make(chan error, 1)
	errChan2 := make(chan error, 1)

	sem := make(chan struct{})

	tc := serverutils.StartTestCluster(t, 2, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
				Knobs: base.TestingKnobs{
					SQLExecutor: &sql.ExecutorTestingKnobs{
						BeforeExecute: func(ctx context.Context, stmt string, _ /* isParallel */ bool) {
							// if queryToBlock
							if strings.Contains(stmt, "(1)") {
								// Block start of execution until queryToCancel has been canceled
								<-sem
							}
						},
						AfterExecute: func(ctx context.Context, stmt string, resultWriter sql.StatementResult, err error) {
							// if queryToBlock
							if strings.Contains(stmt, "(1)") {
								// Ensure queryToBlock errored out with the cancellation error.
								if err == nil {
									errChan <- errors.New("didn't get an error from query that should have been indirectly canceled")
								} else if !sqlbase.IsQueryCanceledError(err) {
									errChan <- err
								}
								close(errChan)
							} else if strings.Contains(stmt, "(2)") { // if queryToCancel
								// This query should have finished successfully; if not,
								// report that error.
								if err != nil {
									errChan2 <- err
								}

								// Cancel this query, even though it has already completed execution.
								// The other query (queryToBlock) should return a cancellation error.
								const cancelQuery = "CANCEL QUERY (SELECT query_id FROM [SHOW CLUSTER QUERIES] WHERE node_id = 1 AND query LIKE '%INSERT INTO nums2 VALUES (2%')"
								if _, err := conn2.Exec(cancelQuery); err != nil {
									errChan2 <- err
								}
								close(errChan2)

								// Unblock queryToBlock
								sem <- struct{}{}
								close(sem)
							}
						},
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.TODO())

	conn1 = tc.ServerConn(0)
	conn2 = tc.ServerConn(1)

	sqlutils.CreateTable(t, conn1, "nums", "num INT", 0, nil)
	sqlutils.CreateTable(t, conn1, "nums2", "num INT", 0, nil)

	// Start the txn. Both queries should run in parallel - and queryToBlock
	// should error out.
	_, err := conn1.Exec(sqlToRun)
	if err != nil && !sqlbase.IsQueryCanceledError(err) {
		t.Fatal(err)
	} else if err == nil {
		t.Fatal("didn't get an error from txn that should have been canceled")
	}

	// Ensure both channels are closed.
	if err := <-errChan2; err != nil {
		t.Fatal(err)
	}
	if err := <-errChan; err != nil {
		t.Fatal(err)
	}
}

// TestCancelDistSQLQuery runs a distsql query and cancels it randomly at
// various points of execution.
func TestCancelDistSQLQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const queryToCancel = "SELECT * FROM nums ORDER BY num"
	cancelQuery := fmt.Sprintf("CANCEL QUERY (SELECT query_id FROM [SHOW CLUSTER QUERIES] WHERE query = '%s')", queryToCancel)

	// conn1 is used for the query above. conn2 is solely for the CANCEL statement.
	var conn1 *gosql.DB
	var conn2 *gosql.DB

	var queryLatency *time.Duration
	sem := make(chan struct{}, 1)
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	tc := serverutils.StartTestCluster(t, 2, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
				Knobs: base.TestingKnobs{
					SQLExecutor: &sql.ExecutorTestingKnobs{
						BeforeExecute: func(_ context.Context, stmt string, _ /* isParallel */ bool) {
							if strings.HasPrefix(stmt, queryToCancel) {
								// Wait for the race to start.
								<-sem
							} else if strings.HasPrefix(stmt, cancelQuery) {
								// Signal to start the race.
								sleepTime := time.Duration(rng.Int63n(int64(*queryLatency)))
								sem <- struct{}{}
								time.Sleep(sleepTime)
							}
						},
					},
				},
			},
		})
	defer tc.Stopper().Stop(context.TODO())

	conn1 = tc.ServerConn(0)
	conn2 = tc.ServerConn(1)

	sqlutils.CreateTable(t, conn1, "nums", "num INT", 0, nil)
	if _, err := conn1.Exec("INSERT INTO nums SELECT generate_series(1,100)"); err != nil {
		t.Fatal(err)
	}

	if _, err := conn1.Exec("ALTER TABLE nums SPLIT AT VALUES (50)"); err != nil {
		t.Fatal(err)
	}
	// Make the second node the leaseholder for the first range to distribute
	// the query.
	if _, err := conn1.Exec(fmt.Sprintf(
		"ALTER TABLE nums TESTING_RELOCATE VALUES (ARRAY[%d], 1)",
		tc.Server(1).GetFirstStoreID(),
	)); err != nil {
		t.Fatal(err)
	}

	// Run queryToCancel to be able to get an estimate of how long it should
	// take. The goroutine in charge of cancellation will sleep a random
	// amount of time within this bound. Signal sem so that it can run
	// unhindered.
	sem <- struct{}{}
	start := timeutil.Now()
	if _, err := conn1.Exec(queryToCancel); err != nil {
		t.Fatal(err)
	}
	execTime := timeutil.Since(start)
	queryLatency = &execTime

	errChan := make(chan error)
	go func() {
		_, err := conn1.Exec(queryToCancel)
		errChan <- err
	}()
	_, err := conn2.Exec(cancelQuery)
	if err != nil && !testutils.IsError(err, "query ID") {
		t.Fatal(err)
	}
	// Successful cancellation.
	// Note the err != nil check. It exists because a successful cancellation
	// does not imply that the query was canceled.
	if err := <-errChan; err != nil && !sqlbase.IsQueryCanceledError(err) {
		t.Fatal(err)
	}
}
