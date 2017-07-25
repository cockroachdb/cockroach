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
//
// Author: Bilal Akhtar <bilal@cockroachlabs.com>

package sql_test

import (
	gosql "database/sql"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
		_, err := conn2.Query(queryToCancel)
		if err != nil {
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
		if !strings.Contains(err.Error(), "query execution cancelled") {
			t.Fatal(err)
		}
	case <-time.After(time.Second * 5):
		t.Fatal("no error received from query supposed to be cancelled")
	}

}

func TestCancelParallelQuery(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// A slower query and a faster query. Both of these will run in parallel, since they're RETURNING NOTHING.
	const query1 = "INSERT INTO nums (SELECT generate_series AS val FROM generate_series(1, 20000000000) WHERE generate_series = (-1)) RETURNING NOTHING;"
	const query2 = "INSERT INTO nums VALUES (2) RETURNING NOTHING;"
	const sqlToRun = "BEGIN TRANSACTION; " + query1 + query2 + " COMMIT;"

	var conn1 *gosql.DB
	var conn2 *gosql.DB

	sem := make(chan struct{})
	errChan := make(chan error)

	tc := serverutils.StartTestCluster(t, 2, /* numNodes */
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				UseDatabase: "test",
				Knobs: base.TestingKnobs{
					SQLExecutor: &sql.ExecutorTestingKnobs{
						StatementFilter: func(ctx context.Context, stmt string, res *sql.Result) {
							// if query2
							if strings.Contains(stmt, "(2)") {
								// Assumption: query1 is still running. Query2 has finished, but still cancel it.
								// This should close the shared context between the two queries, and error out
								// query1. Note that no cancellation checks happen after the StatementFilter runs.
								// query2 will actually finish successfully even though it was the intended
								// target of the CANCEL QUERY.
								go func() {
									sem <- struct{}{}
									const cancelQuery = "CANCEL QUERY (SELECT query_id FROM [SHOW CLUSTER QUERIES] WHERE node_id = 1 AND query LIKE '%INSERT INTO nums VALUES%')"
									_, err := conn2.Exec(cancelQuery)
									if err != nil {
										errChan <- err
										return
									}
									sem <- struct{}{}
								}()
								<-sem
								time.Sleep(time.Second * 2)
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

	// Start the txn. Both queries should run in parallel - and query1
	// should error out.
	_, err := conn1.Exec(sqlToRun)
	if err != nil && !strings.Contains(err.Error(), "query execution cancelled") {
		t.Fatal(err)
	} else if err == nil {
		t.Fatal("didn't get an error from txn that should have been cancelled")
	}

	select {
	case err := <-errChan:
		t.Fatal(err)
	case <-sem:
		// Do nothing - CANCEL QUERY finished normally.
	case <-time.After(time.Second * 5):
		t.Fatal("CANCEL QUERY was never run")
	}
}
