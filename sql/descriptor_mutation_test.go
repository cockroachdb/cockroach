// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package sql_test

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	csql "github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// checkTableData compares the data in the table with the expected rows e.
// The single column mutation if present is made live so that sql can be
// used to read all the data in the table including the mutation column.
// Before returning the last column in the table can be placed in the
// mutation state.
func checkTableData(t *testing.T, kvDB *client.DB, sqlDB *sql.DB, descKey roachpb.Key, desc *csql.Descriptor, state csql.DescriptorMutation_State, e [][]string, returnWithMutation bool) {
	// Remove mutation to check real values in DB using SQL
	tableDesc := desc.GetTable()
	if tableDesc.ColumnMutations != nil {
		if len(tableDesc.ColumnMutations) != 1 {
			t.Fatalf("%d mutations != 1", len(tableDesc.ColumnMutations))
		}
		tableDesc.Columns = append(tableDesc.Columns, tableDesc.ColumnMutations[0].Column)
		tableDesc.ColumnMutations = nil
		if err := tableDesc.Validate(); err != nil {
			t.Fatal(err)
		}
		if err := kvDB.Put(descKey, desc); err != nil {
			t.Fatal(err)
		}
	}
	// Read from DB.
	rows, err := sqlDB.Query(`SELECT * FROM t.kv`)
	if err != nil {
		t.Fatal(err)
	}
	cols, err := rows.Columns()
	if err != nil {
		t.Fatal(err)
	}
	if len(cols) != len(e[0]) {
		t.Fatalf("wrong number of columns %d", len(cols))
	}
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(interface{})
	}
	i := 0
	// Number of values seen by SQL.
	numVals := 0
	for ; rows.Next(); i++ {
		if i >= len(e) {
			t.Fatalf("more rows than expected:%d, %v", len(e), e)
		}
		if err := rows.Scan(vals...); err != nil {
			t.Fatal(err)
		}
		for j, v := range vals {
			if val := *v.(*interface{}); val != nil {
				s := fmt.Sprint(val)
				if e[i][j] != s {
					t.Fatalf("e:%v, v:%v", e[i][j], s)
				}
				numVals++
			} else if e[i][j] != "NULL" {
				t.Fatalf("e:%v, v:%v", e[i][j], "NULL")
			}
		}
	}
	if i != len(e) {
		t.Fatalf("fewer rows read than expected: %d, e=%v", i, e)
	}

	// Check that there are no hidden values
	var tablePrefix []byte
	tablePrefix = append(tablePrefix, keys.TableDataPrefix...)
	tablePrefix = encoding.EncodeUvarint(tablePrefix, uint64(tableDesc.ID))

	tableStartKey := roachpb.Key(tablePrefix)
	tableEndKey := tableStartKey.PrefixEnd()
	if kvs, err := kvDB.Scan(tableStartKey, tableEndKey, 0); err != nil {
		t.Fatal(err)
	} else if len(kvs) != numVals {
		t.Fatalf("expected %d key value pairs, but got %d", numVals, len(kvs))
	}

	if returnWithMutation {
		tableDesc.ColumnMutations = []csql.ColumnMutation{{Column: tableDesc.Columns[len(tableDesc.Columns)-1], Mutation: csql.DescriptorMutation{State: state}}}
		tableDesc.Columns = tableDesc.Columns[:len(tableDesc.Columns)-1]
		if err := tableDesc.Validate(); err != nil {
			t.Fatal(err)
		}
		if err := kvDB.Put(descKey, desc); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOperationsWithColumnMutation(t *testing.T) {
	defer leaktest.AfterTest(t)
	// The descriptor changes made must have an immediate effect.
	csql.DisableTableLeases = true
	defer func() {
		csql.DisableTableLeases = false
	}()
	server, sqlDB, kvDB := setup(t)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR, i CHAR DEFAULT 'i');
`); err != nil {
		t.Fatal(err)
	}

	// read table descriptor
	nameKey := csql.MakeNameMetadataKey(keys.MaxReservedDescID+1, "kv")
	gr, err := kvDB.Get(nameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}
	descKey := csql.MakeDescMetadataKey(csql.ID(gr.ValueInt()))
	desc := &csql.Descriptor{}
	if err := kvDB.GetProto(descKey, desc); err != nil {
		t.Fatal(err)
	}

	// Test how the different operations are affected by mutation states.
	testData := []struct {
		state csql.DescriptorMutation_State
		// Rows in the database.
		// Initial rset of ows.
		start [][]string
		// After failed insert.
		afterInsert [][]string
		// After insert without column under mutation specified.
		afterInsertWithout [][]string
		// After failed update.
		afterUpdate [][]string
		// After update without column under mutation specified.
		afterUpdateWithout [][]string
		// After deletion of a row.
		afterDelete [][]string
	}{
		{state: csql.DescriptorMutation_DELETE_ONLY,
			start:              [][]string{{"a", "z", "q"}},
			afterInsert:        [][]string{{"a", "z", "q"}},
			afterInsertWithout: [][]string{{"a", "z", "q"}, {"c", "x", "NULL"}},
			afterUpdate:        [][]string{{"a", "z", "q"}, {"c", "x", "NULL"}},
			afterUpdateWithout: [][]string{{"a", "u", "q"}, {"c", "x", "NULL"}},
			afterDelete:        [][]string{{"c", "x", "NULL"}},
		},
		{state: csql.DescriptorMutation_WRITE_ONLY,
			start:              [][]string{{"a", "z", "q"}},
			afterInsert:        [][]string{{"a", "z", "q"}},
			afterInsertWithout: [][]string{{"a", "z", "q"}, {"c", "x", "i"}},
			afterUpdate:        [][]string{{"a", "z", "q"}, {"c", "x", "i"}},
			afterUpdateWithout: [][]string{{"a", "u", "q"}, {"c", "x", "i"}},
			afterDelete:        [][]string{{"c", "x", "i"}},
		},
	}

	for _, test := range testData {
		// Refresh table to start state.
		if _, err := sqlDB.Exec(`TRUNCATE TABLE t.kv`); err != nil {
			t.Fatal(err)
		}
		for _, row := range test.start {
			if _, err := sqlDB.Exec(`INSERT INTO t.kv VALUES ($1, $2, $3)`, row[0], row[1], row[2]); err != nil {
				t.Fatal(err)
			}
		}
		checkTableData(t, kvDB, sqlDB, descKey, desc, test.state, test.start, true /* returnWithMutation */)

		// The read of column i fails.
		if _, err := sqlDB.Query(`SELECT i FROM t.kv`); err == nil {
			t.Fatalf("Read succeeded despite table being in %v state", test.state)
		}
		// Column i is hidden.
		if rows, err := sqlDB.Query(`SELECT * FROM t.kv`); err != nil {
			t.Fatal(err)
		} else {
			if cols, err := rows.Columns(); err != nil {
				t.Fatal(err)
			} else {
				if len(cols) != 2 || cols[0] != "k" || cols[1] != "v" {
					t.Fatalf("wrong set of columns; len=%d, columns=%v", len(cols), cols)
				}
			}
			i := 0
			for ; rows.Next(); i++ {
				row := []string{"k", "v"}
				if err := rows.Scan(&row[0], &row[1]); err != nil {
					t.Fatal(err)
				}
				for j := range row {
					if row[j] != test.start[i][j] {
						t.Fatalf("e:%v, v:%s", test.start[i][j], row[j])
					}
				}
			}
			if i < len(test.start) {
				t.Fatalf("fewer rows read than expected: %d, e=%v", i, test.start)
			}
		}

		// Insert into table using the column undergoing a mutation
		if _, err := sqlDB.Exec(`INSERT INTO t.kv (k, v, i) VALUES ('b', 'y', 'i')`); !testutils.IsError(err, "column \"i\" does not exist") {
			t.Fatal(err)
		}
		// Repeat the same without specifying the columns
		if _, err := sqlDB.Exec(`INSERT INTO t.kv VALUES ('b', 'y', 'i')`); !testutils.IsError(err, "INSERT has more expressions than target columns: 3/2") {
			t.Fatal(err)
		}
		checkTableData(t, kvDB, sqlDB, descKey, desc, test.state, test.afterInsert, true /* returnWithMutation */)

		// Insert into table.
		if _, err := sqlDB.Exec(`INSERT INTO t.kv VALUES ('c', 'x')`); err != nil {
			t.Fatal(err)
		}
		checkTableData(t, kvDB, sqlDB, descKey, desc, test.state, test.afterInsertWithout, true /* returnWithMutation */)

		// Update table with mutation column.
		if _, err := sqlDB.Exec(`UPDATE t.kv SET (v, i) = ('u', 'u') WHERE k = 'a'`); !testutils.IsError(err, "column \"i\" does not exist") {
			t.Fatal(err)
		}
		checkTableData(t, kvDB, sqlDB, descKey, desc, test.state, test.afterUpdate, true /* returnWithMutation */)

		// Update without a mutation column.
		if _, err := sqlDB.Exec(`UPDATE t.kv SET v = 'u' WHERE k = 'a'`); err != nil {
			t.Fatal(err)
		}
		checkTableData(t, kvDB, sqlDB, descKey, desc, test.state, test.afterUpdateWithout, true /* returnWithMutation */)

		// Delete row.
		if _, err := sqlDB.Exec(`DELETE FROM t.kv WHERE k = 'a'`); err != nil {
			t.Fatal(err)
		}
		checkTableData(t, kvDB, sqlDB, descKey, desc, test.state, test.afterDelete, false /* returnWithMutation */)
	}

	// Check that a mutation can only be inserted with an explicit mutation state.
	tableDesc := desc.GetTable()
	tableDesc.ColumnMutations = []csql.ColumnMutation{{Column: tableDesc.Columns[len(tableDesc.Columns)-1]}}
	tableDesc.Columns = tableDesc.Columns[:len(tableDesc.Columns)-1]
	if err := tableDesc.Validate(); !testutils.IsError(err, "mutation in UNKNOWN state: col i, col-id 3") {
		t.Fatal(err)
	}
}
