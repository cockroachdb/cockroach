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
	}{
		{"", 0, 0, 0, 0, 0, 0},
		{"BEGIN; END", 1, 0, 0, 0, 0, 0},
		{"SELECT 1", 1, 1, 0, 0, 0, 0},
		{"CREATE DATABASE mt", 1, 1, 0, 0, 0, 1},
		{"CREATE TABLE mt.n (num INTEGER)", 1, 1, 0, 0, 0, 2},
		{"INSERT INTO mt.n VALUES (3)", 1, 1, 0, 1, 0, 2},
		{"UPDATE mt.n SET num = num + 1", 1, 1, 1, 1, 0, 2},
		{"DELETE FROM mt.n", 1, 1, 1, 1, 1, 2},
		{"ALTER TABLE mt.n ADD COLUMN num2 INTEGER", 1, 1, 1, 1, 1, 3},
		{"EXPLAIN SELECT * FROM mt.n", 1, 1, 1, 1, 1, 3},
		{"BEGIN; UPDATE mt.n SET num = num + 1; END", 2, 1, 2, 1, 1, 3},
		{"SELECT * FROM mt.n; SELECT * FROM mt.n; SELECT * FROM mt.n", 2, 4, 2, 1, 1, 3},
		{"DROP TABLE mt.n", 2, 4, 2, 1, 1, 4},
	}

	for _, tc := range testcases {
		if tc.query != "" {
			if _, err := sqlDB.Exec(tc.query); err != nil {
				t.Fatalf("Unexpected error executing '%s': %s'", tc.query, err)
			}
		}

		// Force metric snapshot refresh.
		if err := s.WriteSummaries(); err != nil {
			t.Fatal(err)
		}
		m, err := s.GetMetaRegistryMap()
		if err != nil {
			t.Fatalf("Couldn't get metrics after query '%s': %s", tc.query, err)
		}

		if a, e := int64(m["cr.node.sql.transaction.count."+nid].(float64)), tc.txnCount; a != e {
			t.Errorf("Transaction count for '%s': actual %d != expected %d", tc.query, a, e)
		}
		if a, e := int64(m["cr.node.sql.select.count."+nid].(float64)), tc.selectCount; a != e {
			t.Errorf("Select count for '%s': actual %d != expected %d", tc.query, a, e)
		}
		if a, e := int64(m["cr.node.sql.update.count."+nid].(float64)), tc.updateCount; a != e {
			t.Errorf("Update count for '%s': actual %d != expected %d", tc.query, a, e)
		}
		if a, e := int64(m["cr.node.sql.insert.count."+nid].(float64)), tc.insertCount; a != e {
			t.Errorf("Insert count for '%s': actual %d != expected %d", tc.query, a, e)
		}
		if a, e := int64(m["cr.node.sql.delete.count."+nid].(float64)), tc.deleteCount; a != e {
			t.Errorf("Delete count for '%s': actual %d != expected %d", tc.query, a, e)
		}
		if a, e := int64(m["cr.node.sql.ddl.count."+nid].(float64)), tc.ddlCount; a != e {
			t.Errorf("DDL count for '%s': actual %d != expected %d", tc.query, a, e)
		}

		// Everything after this query will also fail, so quit now to avoid deluge of errors.
		if t.Failed() {
			t.FailNow()
		}
	}
}
