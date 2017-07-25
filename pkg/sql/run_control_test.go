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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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

func TestCancelSelectTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const transactionToCancel = "CREATE DATABASE txn; CREATE TABLE txn.cancel AS SELECT * FROM generate_series(1,20000000)"

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
		_, err := conn2.Query(transactionToCancel)
		if err != nil {
			errChan <- err
		}
	}()

	<-sem
	time.Sleep(time.Second * 2)

	const cancelTransaction = "CANCEL TRANSACTION (SELECT left(kv_txn, 8) FROM [SHOW CLUSTER SESSIONS] WHERE node_id = 2)"

	if _, err := conn1.Exec(cancelTransaction); err != nil {
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
