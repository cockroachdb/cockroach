package sql_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestQueryCounts(t *testing.T) {
	defer leaktest.AfterTest(t)
	s, sqlDB, _ := setup(t)
	defer cleanup(s, sqlDB)
	nid := s.Gossip().GetNodeID().String()

	var testcases = []struct {
		query       string
		txnCount    int64
		selectCount int64
		updateCount int64
		insertCount int64
		deleteCount int64
		ddlCount    int64
		miscCount   int64
	}{
		{"", 0, 0, 0, 0, 0, 0, 0},
		{"BEGIN; END", 1, 0, 0, 0, 0, 0, 2},
		{"SELECT 1", 1, 1, 0, 0, 0, 0, 2},
		{"CREATE DATABASE mt", 1, 1, 0, 0, 0, 1, 2},
		{"CREATE TABLE mt.n (num INTEGER)", 1, 1, 0, 0, 0, 2, 2},
		{"INSERT INTO mt.n VALUES (3)", 1, 1, 0, 1, 0, 2, 2},
		{"UPDATE mt.n SET num = num + 1", 1, 1, 1, 1, 0, 2, 2},
		{"DELETE FROM mt.n", 1, 1, 1, 1, 1, 2, 2},
		{"ALTER TABLE mt.n ADD COLUMN num2 INTEGER", 1, 1, 1, 1, 1, 3, 2},
		{"EXPLAIN SELECT * FROM mt.n", 1, 1, 1, 1, 1, 3, 3},
		{"BEGIN; UPDATE mt.n SET num = num + 1; END", 2, 1, 2, 1, 1, 3, 5},
		{"SELECT * FROM mt.n; SELECT * FROM mt.n; SELECT * FROM mt.n", 2, 4, 2, 1, 1, 3, 5},
		{"DROP TABLE mt.n", 2, 4, 2, 1, 1, 4, 5},
		{"SET database = system", 2, 4, 2, 1, 1, 4, 6},
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
		if a, e := s.MustGetCounter("cr.node.sql.transaction.begincount."+nid), tc.txnCount; a != e {
			t.Errorf("transaction count for '%s': actual %d != expected %d", tc.query, a, e)
		}
		if a, e := s.MustGetCounter("cr.node.sql.select.count."+nid), tc.selectCount; a != e {
			t.Errorf("select count for '%s': actual %d != expected %d", tc.query, a, e)
		}
		if a, e := s.MustGetCounter("cr.node.sql.update.count."+nid), tc.updateCount; a != e {
			t.Errorf("update count for '%s': actual %d != expected %d", tc.query, a, e)
		}
		if a, e := s.MustGetCounter("cr.node.sql.insert.count."+nid), tc.insertCount; a != e {
			t.Errorf("insert count for '%s': actual %d != expected %d", tc.query, a, e)
		}
		if a, e := s.MustGetCounter("cr.node.sql.delete.count."+nid), tc.deleteCount; a != e {
			t.Errorf("delete count for '%s': actual %d != expected %d", tc.query, a, e)
		}
		if a, e := s.MustGetCounter("cr.node.sql.ddl.count."+nid), tc.ddlCount; a != e {
			t.Errorf("DDL count for '%s': actual %d != expected %d", tc.query, a, e)
		}
		if a, e := s.MustGetCounter("cr.node.sql.misc.count."+nid), tc.miscCount; a != e {
			t.Errorf("misc count for '%s': actual %d != expected %d", tc.query, a, e)
		}

		// Everything after this query will also fail, so quit now to avoid deluge of errors.
		if t.Failed() {
			t.FailNow()
		}
	}
}
