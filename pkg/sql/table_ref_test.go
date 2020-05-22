// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestTableRefs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.Background())

	// Populate the test database.
	stmt := `
CREATE DATABASE test;
CREATE TABLE test.t(a INT PRIMARY KEY, xx INT, b INT, c INT);
CREATE TABLE test.hidden(a INT, b INT);
CREATE INDEX bc ON test.t(b, c);
`
	_, err := db.Exec(stmt)
	if err != nil {
		t.Fatal(err)
	}

	// Retrieve the numeric descriptors.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "t")
	tID := tableDesc.ID
	var aID, bID, cID sqlbase.ColumnID
	for i := range tableDesc.Columns {
		c := &tableDesc.Columns[i]
		switch c.Name {
		case "a":
			aID = c.ID
		case "b":
			bID = c.ID
		case "c":
			cID = c.ID
		}
	}
	pkID := tableDesc.PrimaryIndex.ID
	secID := tableDesc.Indexes[0].ID

	// Retrieve the numeric descriptors.
	tableDesc = sqlbase.GetTableDescriptor(kvDB, keys.SystemSQLCodec, "test", "hidden")
	tIDHidden := tableDesc.ID
	var rowIDHidden sqlbase.ColumnID
	for i := range tableDesc.Columns {
		c := &tableDesc.Columns[i]
		switch c.Name {
		case "rowid":
			rowIDHidden = c.ID
		}
	}

	// Make some schema changes meant to shuffle the ID/name mapping.
	stmt = `
ALTER TABLE test.t RENAME COLUMN b TO d;
ALTER TABLE test.t RENAME COLUMN a TO p;
ALTER TABLE test.t DROP COLUMN xx;
`
	_, err = db.Exec(stmt)
	if err != nil {
		t.Fatal(err)
	}

	// Check the table references.
	testData := []struct {
		tableExpr       string
		expectedColumns string
		expectedError   string
	}{
		{fmt.Sprintf("[%d as t]", tID), `(p, d, c)`, ``},
		{fmt.Sprintf("[%d(%d) as t]", tID, aID), `(p)`, ``},
		{fmt.Sprintf("[%d(%d) as t]", tID, bID), `(d)`, ``},
		{fmt.Sprintf("[%d(%d) as t]", tID, cID), `(c)`, ``},
		{fmt.Sprintf("[%d as t]@bc", tID), `(p, d, c)`, ``},
		{fmt.Sprintf("[%d(%d) as t]@bc", tID, aID), `(p)`, ``},
		{fmt.Sprintf("[%d(%d) as t]@bc", tID, bID), `(d)`, ``},
		{fmt.Sprintf("[%d(%d) as t]@bc", tID, cID), `(c)`, ``},
		{fmt.Sprintf("[%d(%d, %d, %d) as t]", tID, cID, bID, aID), `(c, d, p)`, ``},
		{fmt.Sprintf("[%d(%d, %d, %d) as t(c, b, a)]", tID, cID, bID, aID), `(c, b, a)`, ``},
		{fmt.Sprintf("[%d() as t]", tID), ``, `pq: an explicit list of column IDs must include at least one column`},
		{`[666() as t]`, ``, `pq: [666() AS t]: relation "[666]" does not exist`},
		{fmt.Sprintf("[%d(666) as t]", tID), ``, `pq: column [666] does not exist`},
		{fmt.Sprintf("test.t@[%d]", pkID), `(p, d, c)`, ``},
		{fmt.Sprintf("test.t@[%d]", secID), `(p, d, c)`, ``},
		{`test.t@[666]`, ``, `pq: index [666] not found`},
		{fmt.Sprintf("[%d as t]@[%d]", tID, pkID), `(p, d, c)`, ``},
		{fmt.Sprintf("[%d(%d) as t]@[%d]", tID, aID, pkID), `(p)`, ``},
		{fmt.Sprintf("[%d(%d) as t]@[%d]", tID, bID, pkID), `(d)`, ``},
		{fmt.Sprintf("[%d(%d) as t]@[%d]", tID, cID, pkID), `(c)`, ``},
		{fmt.Sprintf("[%d(%d) as t]@[%d]", tID, aID, secID), `(p)`, ``},
		{fmt.Sprintf("[%d(%d) as t]@[%d]", tID, bID, secID), `(d)`, ``},
		{fmt.Sprintf("[%d(%d) as t]@[%d]", tID, cID, secID), `(c)`, ``},
		{fmt.Sprintf("[%d(%d) as t]", tIDHidden, rowIDHidden), `()`, ``},
	}

	for i, d := range testData {
		t.Run(d.tableExpr, func(t *testing.T) {
			sql := `SELECT columns FROM [EXPLAIN(VERBOSE) SELECT * FROM ` + d.tableExpr + "] WHERE columns != ''"
			var columns string
			if err := db.QueryRow(sql).Scan(&columns); err != nil {
				if d.expectedError != "" {
					if err.Error() != d.expectedError {
						t.Fatalf("%d: %s: expected error: %s, got: %v", i, d.tableExpr, d.expectedError, err)
					}
				} else {
					t.Fatalf("%d: %s: query failed: %v", i, d.tableExpr, err)
				}
			}

			if columns != d.expectedColumns {
				t.Fatalf("%d: %s: expected: %s, got: %s", i, d.tableExpr, d.expectedColumns, columns)
			}
		})
	}
}
