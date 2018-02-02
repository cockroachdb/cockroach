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
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
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
	txnAbortCount      int64
}

func TestQueryCounts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	var testcases = []queryCounter{
		// The counts are deltas for each query.
		{query: "SET DISTSQL = 'off'", miscCount: 1},
		{query: "BEGIN; END", txnBeginCount: 1, txnCommitCount: 1},
		{query: "SELECT 1", selectCount: 1, txnCommitCount: 1},
		{query: "CREATE DATABASE mt", ddlCount: 1},
		{query: "CREATE TABLE mt.public.n (num INTEGER)", ddlCount: 1},
		{query: "INSERT INTO mt.public.n VALUES (3)", insertCount: 1},
		{query: "UPDATE mt.public.n SET num = num + 1", updateCount: 1},
		{query: "DELETE FROM mt.public.n", deleteCount: 1},
		{query: "ALTER TABLE mt.public.n ADD COLUMN num2 INTEGER", ddlCount: 1},
		{query: "EXPLAIN SELECT * FROM mt.public.n", miscCount: 1},
		{
			query:         "BEGIN; UPDATE mt.public.n SET num = num + 1; END",
			txnBeginCount: 1, updateCount: 1, txnCommitCount: 1,
		},
		{query: "SELECT * FROM mt.public.n; SELECT * FROM mt.public.n; SELECT * FROM mt.public.n", selectCount: 3},
		{query: "SET DISTSQL = 'on'", miscCount: 1},
		{query: "SELECT * FROM mt.public.n", selectCount: 1, distSQLSelectCount: 1},
		{query: "SET DISTSQL = 'off'", miscCount: 1},
		{query: "DROP TABLE mt.public.n", ddlCount: 1},
		{query: "SET database = system", miscCount: 1},
	}

	accum := initializeQueryCounter(s)

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
			if accum.txnAbortCount, err = checkCounterDelta(s, sql.MetaTxnAbort, accum.txnAbortCount, 0); err != nil {
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

	params, cmdFilters := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	accum := initializeQueryCounter(s)

	if _, err := sqlDB.Exec("CREATE DATABASE db"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec("CREATE TABLE db.public.t (k TEXT PRIMARY KEY, v TEXT)"); err != nil {
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
	// Run a batch of statements to move the txn out of the AutoRetry state,
	// otherwise the INSERT below would be automatically retried.
	if _, err := txn.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}

	_, err = txn.Exec("INSERT INTO db.public.t VALUES ('key', 'marker')")
	if !testutils.IsError(err, "aborted") {
		t.Fatalf("expected aborted error, got: %v", err)
	}

	if err = txn.Rollback(); err != nil {
		t.Fatal(err)
	}

	if _, err := checkCounterDelta(s, sql.MetaTxnAbort, accum.txnAbortCount, 1); err != nil {
		t.Error(err)
	}
	if _, err := checkCounterDelta(s, sql.MetaTxnBegin, accum.txnBeginCount, 1); err != nil {
		t.Error(err)
	}
	if _, err := checkCounterDelta(s, sql.MetaTxnRollback, accum.txnRollbackCount, 0); err != nil {
		t.Error(err)
	}
	if _, err := checkCounterDelta(s, sql.MetaTxnCommit, accum.txnCommitCount, 0); err != nil {
		t.Error(err)
	}
	if _, err := checkCounterDelta(s, sql.MetaInsert, accum.insertCount, 1); err != nil {
		t.Error(err)
	}
}

// TestErrorDuringTransaction tests that the transaction abort count goes up when a query
// results in an error during a txn.
func TestAbortCountErrorDuringTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	accum := initializeQueryCounter(s)

	txn, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := txn.Query("SELECT * FROM i_do.public.not_exist"); err == nil {
		t.Fatal("Expected an error but didn't get one")
	}

	if _, err := checkCounterDelta(s, sql.MetaTxnAbort, accum.txnAbortCount, 1); err != nil {
		t.Error(err)
	}
	if _, err := checkCounterDelta(s, sql.MetaTxnBegin, accum.txnBeginCount, 1); err != nil {
		t.Error(err)
	}
	if _, err := checkCounterDelta(s, sql.MetaSelect, accum.selectCount, 1); err != nil {
		t.Error(err)
	}

	if err := txn.Rollback(); err != nil {
		t.Fatal(err)
	}
}
