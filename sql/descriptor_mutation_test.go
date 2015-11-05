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
	gosql "database/sql"
	"fmt"
	"log"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func checkTableData(t *testing.T, kvDB *client.DB, sqlDB *gosql.DB, descKey roachpb.Key, desc *sql.Descriptor, state sql.DescriptorMutation_State, e [][]string, revert bool) {
	// Remove mutation to check real values in DB using SQL
	tableDesc := desc.GetTable()
	id, err := tableDesc.FindColumnByName("v")
	if err != nil {
		t.Fatal(err)
	}
	tableDesc.Columns[id].Mutation = nil
	if err := kvDB.Put(descKey, desc); err != nil {
		t.Fatal(err)
	}
	// Read from DB.
	rows, err := sqlDB.Query(`SELECT * FROM t.kv`)
	if err != nil {
		t.Fatal(err)
	}
	cols, err := rows.Columns()
	if err != nil {
		log.Fatal(err)
	}
	if len(cols) != 2 {
		log.Fatal(fmt.Sprintf("wrong number of columns %d", len(cols)))
	}
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(interface{})
	}
	i := 0
	for ; rows.Next(); i++ {
		if i >= len(e) {
			log.Fatal(fmt.Sprintf("more rows than expected:%d, %v", len(e), e))
		}
		if err := rows.Scan(vals...); err != nil {
			log.Fatal(err)
		}
		for j, v := range vals {
			if val := *v.(*interface{}); val != nil {
				s := fmt.Sprint(val)
				if e[i][j] != s {
					log.Fatal(fmt.Sprintf("e:%v, v:%v", e[i][j], s))
				}
			} else {
				if e[i][j] != "NULL" {
					log.Fatal(fmt.Sprintf("e:%v, v:%v", e[i][j], "NULL"))
				}
			}
		}
	}
	if i != len(e) {
		log.Fatal(fmt.Sprintf("fewer rows read than expected: %d, e=%v", i, e))
	}

	// Revert table to prior state
	if revert {
		tableDesc.Columns[id].Mutation = &sql.DescriptorMutation{State: state}
		if err := kvDB.Put(descKey, desc); err != nil {
			t.Fatal(err)
		}
	}
}

func TestOperationsWithColumnMutation(t *testing.T) {
	defer leaktest.AfterTest(t)
	// The descriptor changes made must have an immediate effect.
	sql.DisableTableLeases = true
	server, sqlDB, kvDB := setup(t)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.kv (k CHAR PRIMARY KEY, v CHAR DEFAULT 'h');
`); err != nil {
		t.Fatal(err)
	}

	// read table descriptor
	nameKey := sql.MakeNameMetadataKey(keys.MaxReservedDescID+1, "kv")
	gr, err := kvDB.Get(nameKey)
	if err != nil {
		t.Fatal(err)
	}
	if !gr.Exists() {
		t.Fatalf("Name entry %q does not exist", nameKey)
	}
	descKey := sql.MakeDescMetadataKey(sql.ID(gr.ValueInt()))
	desc := &sql.Descriptor{}
	if err := kvDB.GetProto(descKey, desc); err != nil {
		t.Fatal(err)
	}

	testData := []struct {
		state       sql.DescriptorMutation_State
		start       [][]string
		afterInsert [][]string
		afterUpdate [][]string
		afterDelete [][]string
	}{
		{state: sql.DescriptorMutation_DELETE_ONLY,
			start:       [][]string{{"a", "b"}, {"c", "d"}, {"e", "f"}},
			afterInsert: [][]string{{"a", "b"}, {"c", "d"}, {"e", "f"}, {"g", "NULL"}, {"i", "NULL"}},
			afterUpdate: [][]string{{"a", "NULL"}, {"c", "d"}, {"e", "f"}, {"g", "NULL"}, {"i", "NULL"}},
			afterDelete: [][]string{{"a", "NULL"}, {"e", "f"}, {"g", "NULL"}, {"i", "NULL"}},
		},
		{state: sql.DescriptorMutation_WRITE_ONLY,
			start:       [][]string{{"a", "b"}, {"c", "d"}, {"e", "f"}},
			afterInsert: [][]string{{"a", "b"}, {"c", "d"}, {"e", "f"}, {"g", "h"}, {"i", "j"}},
			afterUpdate: [][]string{{"a", "h"}, {"c", "d"}, {"e", "f"}, {"g", "h"}, {"i", "j"}},
			afterDelete: [][]string{{"a", "h"}, {"e", "f"}, {"g", "h"}, {"i", "j"}},
		},
	}

	for _, test := range testData {
		// Refresh table to start state.
		if _, err := sqlDB.Exec(`TRUNCATE TABLE t.kv`); err != nil {
			t.Fatal(err)
		}
		for _, row := range test.start {
			if _, err := sqlDB.Exec(`INSERT INTO t.kv VALUES ($1, $2)`, row[0], row[1]); err != nil {
				t.Fatal(err)
			}
		}
		checkTableData(t, kvDB, sqlDB, descKey, desc, test.state, test.start, true)

		// The read fails.
		if _, err := sqlDB.Query(`SELECT v FROM t.kv`); err == nil {
			t.Fatal(fmt.Sprintf("Read succeeded despite table being in %v state", test.state))
		}
		// Column v is hidden.
		if rows, err := sqlDB.Query(`SELECT * FROM t.kv`); err != nil {
			t.Fatal(err)
		} else {
			if cols, err := rows.Columns(); err != nil {
				log.Fatal(err)
			} else {
				if len(cols) != 1 || cols[0] != "k" {
					log.Fatal(fmt.Sprintf("wrong set of columns; len=%d, columns=%v", len(cols), cols))
				}
			}
			i := 0
			for ; rows.Next(); i++ {
				v := "foo"
				if err := rows.Scan(&v); err != nil {
					log.Fatal(err)
				}
				if v != test.start[i][0] {
					log.Fatal(fmt.Sprintf("e:%v, v:%s", test.start[i][0], v))
				}
			}
			if i < len(test.start) {
				log.Fatal(fmt.Sprintf("fewer rows read than expected: %d, e=%v", i, test.start))
			}
		}

		// Insert into table using the default value for column v.
		if _, err := sqlDB.Exec(`INSERT INTO t.kv VALUES ('g')`); err != nil {
			t.Fatal(err)
		}
		// Insert into table.
		if _, err := sqlDB.Exec(`INSERT INTO t.kv VALUES ('i', 'j')`); err != nil {
			t.Fatal(err)
		}
		checkTableData(t, kvDB, sqlDB, descKey, desc, test.state, test.afterInsert, true)

		// Update table.
		if _, err := sqlDB.Exec(`UPDATE t.kv SET v = 'h' WHERE k = 'a'`); err != nil {
			t.Fatal(err)
		}
		checkTableData(t, kvDB, sqlDB, descKey, desc, test.state, test.afterUpdate, true)

		// Delete row.
		if _, err := sqlDB.Exec(`DELETE FROM t.kv WHERE k = 'c'`); err != nil {
			t.Fatal(err)
		}
		checkTableData(t, kvDB, sqlDB, descKey, desc, test.state, test.afterDelete, false)
	}
}
