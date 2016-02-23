package sql_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestQueryCounts(t *testing.T) {
	defer leaktest.AfterTest(t)
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

		checkCounter := func(key string, e int64) {
			if a := s.MustGetSQLCounter(key); a != e {
				t.Errorf("%s for '%s': actual %d != expected %d", key, tc.query, a, e)
			}
		}
		checkCounter("txn.begin.count", tc.txnBeginCount)
		checkCounter("select.count", tc.selectCount)
		checkCounter("update.count", tc.updateCount)
		checkCounter("insert.count", tc.insertCount)
		checkCounter("delete.count", tc.deleteCount)
		checkCounter("ddl.count", tc.ddlCount)
		checkCounter("misc.count", tc.miscCount)
		checkCounter("txn.commit.count", tc.txnCommitCount)
		checkCounter("txn.rollback.count", tc.txnRollbackCount)
		checkCounter("txn.abort.count", 0)

		// Everything after this query will also fail, so quit now to avoid deluge of errors.
		if t.Failed() {
			t.FailNow()
		}
	}
}

func TestAbortCountConflictingWrites(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, sqlDB, _ := setup(t)
	defer cleanup(s, sqlDB)

	if _, err := sqlDB.Exec("CREATE DATABASE db"); err != nil {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec("CREATE TABLE db.t (n INTEGER PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}

	// Within a transaction, start an INSERT but don't COMMIT it.
	txn, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := txn.Exec("INSERT INTO db.t VALUES (1)"); err != nil {
		t.Error(err)
		return
	}

	// Outside the transaction, do a conflicting INSERT.
	if _, err := sqlDB.Exec("INSERT INTO db.t VALUES (1)"); err != nil {
		t.Fatal(err)
	}

	// The earlier transaction loses.
	// TODO(cdo): This is not exactly right and could take a while. Fix this when there's
	// a better test to model this after.
	if err := txn.Commit(); !testutils.IsError(err, "aborted") {
		t.Fatalf("unexpected error: %s", err)
	}

	checkCounter := func(key string, e int64) {
		if a := s.MustGetSQLCounter(key); a != e {
			t.Errorf("stat %s: actual %d != expected %d", key, a, e)
		}
	}
	checkCounter("txn.abort.count", 1)
	checkCounter("txn.begin.count", 1)
	checkCounter("txn.rollback.count", 0)
	checkCounter("txn.commit.count", 1)
	checkCounter("insert.count", 2)
}

// TestErrorDuringTransaction tests that the transaction abort count goes up when a query
// results in an error during a txn.
func TestAbortCountErrorDuringTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, sqlDB, _ := setup(t)
	defer cleanup(s, sqlDB)

	txn, err := sqlDB.Begin()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := txn.Query("SELECT * FROM i_do.not_exist"); err == nil {
		t.Fatal("Expected an error but didn't get one")
	}

	checkCounter := func(key string, e int64) {
		if a := s.MustGetSQLCounter(key); a != e {
			t.Errorf("stat %s: actual %d != expected %d", key, a, e)
		}
	}
	checkCounter("txn.abort.count", 1)
	checkCounter("txn.begin.count", 1)
	checkCounter("select.count", 1)
}
