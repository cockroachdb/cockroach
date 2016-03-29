package sql_test

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/storageutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestQueryCounts(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, _ := setup(t)
	defer cleanup(s, sqlDB)

	var testcases = []struct {
		query            string
		txnBeginCount    int64
		selectCount      int64
		updateCount      int64
		insertCount      int64
		deleteCount      int64
		ddlCount         int64
		miscCount        int64
		txnCommitCount   int64
		txnRollbackCount int64
	}{
		{"", 0, 0, 0, 0, 0, 0, 0, 0, 0},
		{"BEGIN; END", 1, 0, 0, 0, 0, 0, 0, 1, 0},
		{"SELECT 1", 1, 1, 0, 0, 0, 0, 0, 1, 0},
		{"CREATE DATABASE mt", 1, 1, 0, 0, 0, 1, 0, 1, 0},
		{"CREATE TABLE mt.n (num INTEGER)", 1, 1, 0, 0, 0, 2, 0, 1, 0},
		{"INSERT INTO mt.n VALUES (3)", 1, 1, 0, 1, 0, 2, 0, 1, 0},
		{"UPDATE mt.n SET num = num + 1", 1, 1, 1, 1, 0, 2, 0, 1, 0},
		{"DELETE FROM mt.n", 1, 1, 1, 1, 1, 2, 0, 1, 0},
		{"ALTER TABLE mt.n ADD COLUMN num2 INTEGER", 1, 1, 1, 1, 1, 3, 0, 1, 0},
		{"EXPLAIN SELECT * FROM mt.n", 1, 1, 1, 1, 1, 3, 1, 1, 0},
		{"BEGIN; UPDATE mt.n SET num = num + 1; END", 2, 1, 2, 1, 1, 3, 1, 2, 0},
		{"SELECT * FROM mt.n; SELECT * FROM mt.n; SELECT * FROM mt.n", 2, 4, 2, 1, 1, 3, 1, 2, 0},
		{"DROP TABLE mt.n", 2, 4, 2, 1, 1, 4, 1, 2, 0},
		{"SET database = system", 2, 4, 2, 1, 1, 4, 2, 2, 0},
	}

	for _, tc := range testcases {
		if tc.query != "" {
			if _, err := sqlDB.Exec(tc.query); err != nil {
				t.Fatalf("unexpected error executing '%s': %s'", tc.query, err)
			}
		}

		// Force metric snapshot refresh.
		if err := s.WriteSummaries(); err != nil {
			t.Fatal(err)
		}

		checkCounterEQ(t, s, "txn.begin.count", tc.txnBeginCount)
		checkCounterEQ(t, s, "select.count", tc.selectCount)
		checkCounterEQ(t, s, "update.count", tc.updateCount)
		checkCounterEQ(t, s, "insert.count", tc.insertCount)
		checkCounterEQ(t, s, "delete.count", tc.deleteCount)
		checkCounterEQ(t, s, "ddl.count", tc.ddlCount)
		checkCounterEQ(t, s, "misc.count", tc.miscCount)
		checkCounterEQ(t, s, "txn.commit.count", tc.txnCommitCount)
		checkCounterEQ(t, s, "txn.rollback.count", tc.txnRollbackCount)
		checkCounterEQ(t, s, "txn.abort.count", 0)

		// Everything after this query will also fail, so quit now to avoid deluge of errors.
		if t.Failed() {
			t.FailNow()
		}
	}
}

func TestAbortCountConflictingWrites(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cmdFilters := createTestServerContext()
	s, sqlDB, _ := setupWithContext(t, ctx)
	defer cleanup(s, sqlDB)

	if _, err := sqlDB.Exec("CREATE DATABASE db"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec("CREATE TABLE db.t (k TEXT PRIMARY KEY, v TEXT)"); err != nil {
		t.Fatal(err)
	}

	// Inject errors on the INSERT below.
	restarted := false
	cmdFilters.AppendFilter(func(args storageutils.FilterArgs) *roachpb.Error {
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

	checkCounterEQ(t, s, "txn.abort.count", 1)
	checkCounterEQ(t, s, "txn.begin.count", 1)
	checkCounterEQ(t, s, "txn.rollback.count", 0)
	checkCounterEQ(t, s, "txn.commit.count", 0)
	checkCounterEQ(t, s, "insert.count", 1)
}

// TestErrorDuringTransaction tests that the transaction abort count goes up when a query
// results in an error during a txn.
func TestAbortCountErrorDuringTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, sqlDB, _ := setup(t)
	defer cleanup(s, sqlDB)

	txn, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := txn.Query("SELECT * FROM i_do.not_exist"); err == nil {
		t.Fatal("Expected an error but didn't get one")
	}

	checkCounterEQ(t, s, "txn.abort.count", 1)
	checkCounterEQ(t, s, "txn.begin.count", 1)
	checkCounterEQ(t, s, "select.count", 1)
}
