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
// Author: Cuong Do (cdo@cockroachlabs.com)

package sql_test

import (
	"bytes"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type queryCounter struct {
	query              string
	txnBeginCount      int64
	selectCount        int64
	distSQLSelectCount int64
	updateCount        int64
	insertCount        int64
	deleteCount        int64
	ddlCount           int64
	miscCount          int64
	txnCommitCount     int64
	txnRollbackCount   int64
}

func TestQueryCounts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	var testcases = []queryCounter{
		// The counts are deltas for each query.
		{query: "SET DISTSQL = 'off'", miscCount: 1},
		{query: "BEGIN; END", txnBeginCount: 1, txnCommitCount: 1},
		{query: "SELECT 1", selectCount: 1, txnCommitCount: 1},
		{query: "CREATE DATABASE mt", ddlCount: 1},
		{query: "CREATE TABLE mt.n (num INTEGER)", ddlCount: 1},
		{query: "INSERT INTO mt.n VALUES (3)", insertCount: 1},
		{query: "UPDATE mt.n SET num = num + 1", updateCount: 1},
		{query: "DELETE FROM mt.n", deleteCount: 1},
		{query: "ALTER TABLE mt.n ADD COLUMN num2 INTEGER", ddlCount: 1},
		{query: "EXPLAIN SELECT * FROM mt.n", miscCount: 1},
		{
			query:         "BEGIN; UPDATE mt.n SET num = num + 1; END",
			txnBeginCount: 1, updateCount: 1, txnCommitCount: 1,
		},
		{query: "SELECT * FROM mt.n; SELECT * FROM mt.n; SELECT * FROM mt.n", selectCount: 3},
		{query: "SET DISTSQL = 'on'", miscCount: 1},
		{query: "SELECT * FROM mt.n", selectCount: 1, distSQLSelectCount: 1},
		{query: "SET DISTSQL = 'off'", miscCount: 1},
		{query: "DROP TABLE mt.n", ddlCount: 1},
		{query: "SET database = system", miscCount: 1},
	}

	// Initialize accum while accounting for system migrations that may have run
	// DDL statements.
	accum := queryCounter{
		ddlCount:  s.MustGetSQLCounter(sql.MetaDdl.Name),
		miscCount: s.MustGetSQLCounter(sql.MetaMisc.Name),
	}

	for _, tc := range testcases {
		t.Run(tc.query, func(t *testing.T) {
			if _, err := sqlDB.Exec(tc.query); err != nil {
				t.Fatalf("unexpected error executing '%s': %s'", tc.query, err)
			}

			// Force metric snapshot refresh.
			if err := s.WriteSummaries(); err != nil {
				t.Fatal(err)
			}

			var err error
			if accum.txnBeginCount, err = checkCounterDelta(s, sql.MetaTxnBegin, accum.txnBeginCount, tc.txnBeginCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.distSQLSelectCount, err = checkCounterDelta(s, sql.MetaDistSQLSelect, accum.distSQLSelectCount, tc.distSQLSelectCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.txnRollbackCount, err = checkCounterDelta(s, sql.MetaTxnRollback, accum.txnRollbackCount, tc.txnRollbackCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if err := checkCounterEQ(s, sql.MetaTxnAbort, 0); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.selectCount, err = checkCounterDelta(s, sql.MetaSelect, accum.selectCount, tc.selectCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.updateCount, err = checkCounterDelta(s, sql.MetaUpdate, accum.updateCount, tc.updateCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.insertCount, err = checkCounterDelta(s, sql.MetaInsert, accum.insertCount, tc.insertCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.deleteCount, err = checkCounterDelta(s, sql.MetaDelete, accum.deleteCount, tc.deleteCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.ddlCount, err = checkCounterDelta(s, sql.MetaDdl, accum.ddlCount, tc.ddlCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
			if accum.miscCount, err = checkCounterDelta(s, sql.MetaMisc, accum.miscCount, tc.miscCount); err != nil {
				t.Errorf("%q: %s", tc.query, err)
			}
		})
	}
}

func TestAbortCountConflictingWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, cmdFilters := createTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec("CREATE DATABASE db"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec("CREATE TABLE db.t (k TEXT PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}

	// Inject errors on the INSERT below.
	restarted := false
	cmdFilters.AppendFilter(func(args storagebase.FilterArgs) *roachpb.Error {
		switch req := args.Req.(type) {
		// SQL INSERT generates ConditionalPuts for unique indexes (such as the PK).
		case *roachpb.ConditionalPutRequest:
			if bytes.Contains(req.Value.RawBytes, []byte("marker")) && !restarted {
				restarted = true
				return roachpb.NewErrorWithTxn(
					roachpb.NewTransactionAbortedError(), args.Hdr.Txn)
			}
		}
		return nil
	}, false)

	txn, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	_, err = txn.Exec("INSERT INTO db.t VALUES ('key', 'marker')")
	if !testutils.IsError(err, "aborted") {
		t.Fatal(err)
	}

	if err = txn.Rollback(); err != nil {
		t.Fatal(err)
	}

	if err := checkCounterEQ(s, sql.MetaTxnAbort, 1); err != nil {
		t.Error(err)
	}
	if err := checkCounterEQ(s, sql.MetaTxnBegin, 1); err != nil {
		t.Error(err)
	}
	if err := checkCounterEQ(s, sql.MetaTxnRollback, 0); err != nil {
		t.Error(err)
	}
	if err := checkCounterEQ(s, sql.MetaTxnCommit, 0); err != nil {
		t.Error(err)
	}
	if err := checkCounterEQ(s, sql.MetaInsert, 1); err != nil {
		t.Error(err)
	}
}

// TestErrorDuringTransaction tests that the transaction abort count goes up when a query
// results in an error during a txn.
func TestAbortCountErrorDuringTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := createTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	txn, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := txn.Query("SELECT * FROM i_do.not_exist"); err == nil {
		t.Fatal("Expected an error but didn't get one")
	}

	if err := checkCounterEQ(s, sql.MetaTxnAbort, 1); err != nil {
		t.Error(err)
	}
	if err := checkCounterEQ(s, sql.MetaTxnBegin, 1); err != nil {
		t.Error(err)
	}
	if err := checkCounterEQ(s, sql.MetaSelect, 1); err != nil {
		t.Error(err)
	}

	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
}
