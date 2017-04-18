// Copyright 2017 The Cockroach Authors.
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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package sql_test

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestTableRefs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := createTestServerParams()
	s, db, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	// Populate the test database.
	stmt := `
CREATE DATABASE test;
CREATE TABLE test.t(a INT PRIMARY KEY, xx INT, b INT, c INT);
CREATE INDEX bc ON test.t(b, c);
`
	_, err := db.Exec(stmt)
	if err != nil {
		t.Fatal(err)
	}

	// Retrieve the numeric descriptors.
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "test", "t")
	tID := tableDesc.ID
	var aID, bID, cID sqlbase.ColumnID
	for _, c := range tableDesc.Columns {
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
		{fmt.Sprintf("[%d] as t", tID), `(p, d, c)`, ``},
		{fmt.Sprintf("[%d(%d)] as t", tID, aID), `(p)`, ``},
		{fmt.Sprintf("[%d(%d)] as t", tID, bID), `(d)`, ``},
		{fmt.Sprintf("[%d(%d)] as t", tID, cID), `(c)`, ``},
		{fmt.Sprintf("[%d]@bc as t", tID), `(p, d, c)`, ``},
		{fmt.Sprintf("[%d(%d)]@bc as t", tID, aID), `(p)`, ``},
		{fmt.Sprintf("[%d(%d)]@bc as t", tID, bID), `(d)`, ``},
		{fmt.Sprintf("[%d(%d)]@bc as t", tID, cID), `(c)`, ``},
		{fmt.Sprintf("[%d(%d, %d, %d)] as t", tID, cID, bID, aID), `(c, d, p)`, ``},
		{fmt.Sprintf("[%d(%d, %d, %d)] as t(c, b, a)", tID, cID, bID, aID), `(c, b, a)`, ``},
		{fmt.Sprintf("[%d()] as t", tID), `()`, ``},
		{`[666()] as t`, ``, `pq: table "<id=666>" does not exist`},
		{fmt.Sprintf("[%d(666)] as t", tID), ``, `pq: column 666 does not exist`},
		{fmt.Sprintf("test.t@[%d]", pkID), `(p, d, c)`, ``},
		{fmt.Sprintf("test.t@[%d]", secID), `(p, d, c)`, ``},
		{`test.t@[666]`, ``, `pq: index 666 not found`},
		{fmt.Sprintf("[%d]@[%d] as t", tID, pkID), `(p, d, c)`, ``},
		{fmt.Sprintf("[%d(%d)]@[%d] as t", tID, aID, pkID), `(p)`, ``},
		{fmt.Sprintf("[%d(%d)]@[%d] as t", tID, bID, pkID), `(d)`, ``},
		{fmt.Sprintf("[%d(%d)]@[%d] as t", tID, cID, pkID), `(c)`, ``},
		{fmt.Sprintf("[%d(%d)]@[%d] as t", tID, aID, secID), `(p)`, ``},
		{fmt.Sprintf("[%d(%d)]@[%d] as t", tID, bID, secID), `(d)`, ``},
		{fmt.Sprintf("[%d(%d)]@[%d] as t", tID, cID, secID), `(c)`, ``},
	}

	for i, d := range testData {
		sql := "SELECT Columns FROM [EXPLAIN(METADATA) SELECT * FROM " + d.tableExpr + "]"
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
	}
}
