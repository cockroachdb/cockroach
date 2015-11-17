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
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	csql "github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

type mutationTest struct {
	kvDB    *client.DB
	sqlDB   *sql.DB
	descKey roachpb.Key
	desc    *csql.Descriptor
}

// checkQueryResponse runs the sql query q, and checks that it matches
// the expected response. It returns the total number of non-null values
// returned in the response (num-rows*num-columns - total-num-null-values),
// as a measure of the number of key:value pairs visible to it.
func (mt mutationTest) checkQueryResponse(q string, e [][]string) (int, error) {
	// Read from DB.
	rows, err := mt.sqlDB.Query(q)
	if err != nil {
		return -1, err
	}
	cols, err := rows.Columns()
	if err != nil {
		return -1, err
	}
	if len(e) > 0 && len(cols) != len(e[0]) {
		return -1, fmt.Errorf("wrong number of columns %d", len(cols))
	}
	vals := make([]interface{}, len(cols))
	for i := range vals {
		vals[i] = new(interface{})
	}
	i := 0
	// Number of non-NULL values.
	numVals := 0
	for ; rows.Next(); i++ {
		if i >= len(e) {
			return -1, fmt.Errorf("expected less than %d rows, got %d rows:, %v", len(e), i, e)
		}
		if err := rows.Scan(vals...); err != nil {
			return -1, err
		}
		for j, v := range vals {
			if val := *v.(*interface{}); val != nil {
				s := fmt.Sprint(val)
				if e[i][j] != s {
					return -1, fmt.Errorf("e:%v, v:%v", e[i][j], s)
				}
				numVals++
			} else if e[i][j] != "NULL" {
				return -1, fmt.Errorf("e:%v, v:%v", e[i][j], "NULL")
			}
		}
	}
	if i != len(e) {
		return -1, fmt.Errorf("fewer rows read than expected: %d, e=%v", i, e)
	}
	return numVals, nil
}

// checkTableSize checks that the number of key:value pairs stored
// in the table equals e.
func (mt *mutationTest) checkTableSize(e int) error {
	// Check that there are no hidden values
	var tablePrefix []byte
	tablePrefix = append(tablePrefix, keys.TableDataPrefix...)
	tableDesc := mt.desc.GetTable()
	tablePrefix = encoding.EncodeUvarint(tablePrefix, uint64(tableDesc.ID))
	tableStartKey := roachpb.Key(tablePrefix)
	tableEndKey := tableStartKey.PrefixEnd()
	if kvs, err := mt.kvDB.Scan(tableStartKey, tableEndKey, 0); err != nil {
		return err
	} else if len(kvs) != e {
		return fmt.Errorf("expected %d key value pairs, but got %d", e, len(kvs))
	}
	return nil
}

// Convert all the mutations into live descriptors for the table
// and write the updated table descriptor to the DB.
func (mt mutationTest) makeMutationsActive() error {
	// Remove mutation to check real values in DB using SQL
	tableDesc := mt.desc.GetTable()
	if tableDesc.Mutations == nil || len(tableDesc.Mutations) == 0 {
		return nil
	}
	if len(tableDesc.Mutations) != 1 {
		return fmt.Errorf("%d mutations != 1", len(tableDesc.Mutations))
	}
	m := tableDesc.Mutations[0]
	if col := m.GetColumn(); col != nil {
		tableDesc.Columns = append(tableDesc.Columns, *col)
	} else if index := m.GetIndex(); index != nil {
		tableDesc.Indexes = append(tableDesc.Indexes, *index)
	} else {
		return fmt.Errorf("no descriptor in mutation: %v", m)
	}
	tableDesc.Mutations = tableDesc.Mutations[1:]
	if err := tableDesc.Validate(); err != nil {
		return err
	}
	return mt.kvDB.Put(mt.descKey, mt.desc)
}

// writeColumnMutation adds column as a mutation and writes the
// descriptor to the DB.
func (mt mutationTest) writeColumnMutation(column string, state csql.DescriptorMutation_State) error {
	tableDesc := mt.desc.GetTable()
	i, err := tableDesc.FindColumnByName(column)
	if err != nil {
		return err
	}
	col := &tableDesc.Columns[i]
	m := csql.DescriptorMutation{Descriptor_: &csql.DescriptorMutation_Column{Column: col}, State: state}
	// randomly pick add/drop mutation.
	r := rand.Intn(2)
	if r == 0 {
		m.Direction = csql.DescriptorMutation_ADD
	} else {
		m.Direction = csql.DescriptorMutation_DROP
	}
	tableDesc.Mutations = append(tableDesc.Mutations, m)
	tableDesc.Columns = append(tableDesc.Columns[:i], tableDesc.Columns[i+1:]...)
	if err := tableDesc.Validate(); err != nil {
		return err
	}
	return mt.kvDB.Put(mt.descKey, mt.desc)
}

func TestOperationsWithColumnMutation(t *testing.T) {
	defer leaktest.AfterTest(t)
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer csql.TestDisableTableLeases()()
	server, sqlDB, kvDB := setup(t)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR, i CHAR DEFAULT 'i');
`); err != nil {
		t.Fatal(err)
	}

	// read table descriptor
	nameKey := csql.MakeNameMetadataKey(keys.MaxReservedDescID+1, "test")
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

	mTest := mutationTest{
		kvDB:    kvDB,
		sqlDB:   sqlDB,
		descKey: descKey,
		desc:    desc,
	}

	starQuery := `SELECT * FROM t.test`
	// Run the tests for both states.
	for _, state := range []csql.DescriptorMutation_State{csql.DescriptorMutation_DELETE_ONLY, csql.DescriptorMutation_WRITE_ONLY} {
		// Init table to start state.
		if _, err := sqlDB.Exec(`TRUNCATE TABLE t.test`); err != nil {
			t.Fatal(err)
		}
		initRows := [][]string{{"a", "z", "q"}}
		for _, row := range initRows {
			if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ($1, $2, $3)`, row[0], row[1], row[2]); err != nil {
				t.Fatal(err)
			}
		}
		// Check that the table only contains the initRows.
		if _, err := mTest.checkQueryResponse(starQuery, initRows); err != nil {
			t.Fatal(err)
		}

		// Add column "i" as a mutation.
		if err := mTest.writeColumnMutation("i", state); err != nil {
			t.Fatal(err)
		}
		// A direct read of column "i" fails.
		if _, err := sqlDB.Query(`SELECT i FROM t.test`); err == nil {
			t.Fatalf("Read succeeded despite column being in %v state", state)
		}
		// The table only contains columns "k" and "v".
		if _, err := mTest.checkQueryResponse(starQuery, [][]string{{"a", "z"}}); err != nil {
			t.Fatal(err)
		}

		// Inserting a row into the table while specifying column "i" results in an error.
		if _, err := sqlDB.Exec(`INSERT INTO t.test (k, v, i) VALUES ('b', 'y', 'i')`); !testutils.IsError(err, "column \"i\" does not exist") {
			t.Fatal(err)
		}
		// Repeating the same without specifying the columns results in a different error.
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ('b', 'y', 'i')`); !testutils.IsError(err, "INSERT has more expressions than target columns: 3/2") {
			t.Fatal(err)
		}
		// Make column "i" live so that it is read.
		if err := mTest.makeMutationsActive(); err != nil {
			t.Fatal(err)
		}
		// Check that we can read all the rows and columns.
		if _, err := mTest.checkQueryResponse(starQuery, initRows); err != nil {
			t.Fatal(err)
		}

		var afterInsert, afterUpdate, afterDelete [][]string
		if state == csql.DescriptorMutation_DELETE_ONLY {
			// The default value of "i" for column "i" is not written.
			afterInsert = [][]string{{"a", "z", "q"}, {"c", "x", "NULL"}}
			// Update is a noop for column "i".
			afterUpdate = [][]string{{"a", "u", "q"}, {"c", "x", "NULL"}}
			// Delete also deletes column "i".
			afterDelete = [][]string{{"c", "x", "NULL"}}
		} else {
			// The default value of "i" for column "i" is written.
			afterInsert = [][]string{{"a", "z", "q"}, {"c", "x", "i"}}
			// Update is a noop for column "i".
			afterUpdate = [][]string{{"a", "u", "q"}, {"c", "x", "i"}}
			// Delete also deletes column "i".
			afterDelete = [][]string{{"c", "x", "i"}}
		}

		// Make column "i" a mutation.
		if err := mTest.writeColumnMutation("i", state); err != nil {
			t.Fatal(err)
		}
		// Insert a row into the table.
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ('c', 'x')`); err != nil {
			t.Fatal(err)
		}
		// Make column "i" live so that it is read.
		if err := mTest.makeMutationsActive(); err != nil {
			t.Fatal(err)
		}
		// Notice that the default value of "i" is only written when the
		// descriptor is in the WRITE_ONLY state.
		if _, err := mTest.checkQueryResponse(starQuery, afterInsert); err != nil {
			t.Fatal(err)
		}

		// Make column "i" a mutation.
		if err := mTest.writeColumnMutation("i", state); err != nil {
			t.Fatal(err)
		}
		// Updating column "i" for a row fails.
		if _, err := sqlDB.Exec(`UPDATE t.test SET (v, i) = ('u', 'u') WHERE k = 'a'`); !testutils.IsError(err, "column \"i\" does not exist") {
			t.Fatal(err)
		}
		// Make column "i" live so that it is read.
		if err := mTest.makeMutationsActive(); err != nil {
			t.Fatal(err)
		}
		// The above failed update was a noop.
		if _, err := mTest.checkQueryResponse(starQuery, afterInsert); err != nil {
			t.Fatal(err)
		}

		// Make column "i" a mutation.
		if err := mTest.writeColumnMutation("i", state); err != nil {
			t.Fatal(err)
		}
		// Update a row without specifying  mutation column "i".
		if _, err := sqlDB.Exec(`UPDATE t.test SET v = 'u' WHERE k = 'a'`); err != nil {
			t.Fatal(err)
		}
		// Make column "i" live so that it is read.
		if err := mTest.makeMutationsActive(); err != nil {
			t.Fatal(err)
		}
		// The update to column "v" is seen; there is no effect on column "i".
		if _, err := mTest.checkQueryResponse(starQuery, afterUpdate); err != nil {
			t.Fatal(err)
		}

		// Make column "i" a mutation.
		if err := mTest.writeColumnMutation("i", state); err != nil {
			t.Fatal(err)
		}
		// Delete row "a".
		if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = 'a'`); err != nil {
			t.Fatal(err)
		}
		// Make column "i" live so that it is read.
		if err := mTest.makeMutationsActive(); err != nil {
			t.Fatal(err)
		}
		// Row "a" is deleted. numVals is the number of non-NULL values seen,
		// or the number of KV values belonging to all the rows in the table
		// excluding row "a" since it's deleted.
		numVals, err := mTest.checkQueryResponse(starQuery, afterDelete)
		if err != nil {
			t.Fatal(err)
		}

		// Check that there are no hidden KV values for row "a",
		// and column "i" for row "a" was deleted.
		if err := mTest.checkTableSize(numVals); err != nil {
			t.Fatal(err)
		}
	}

	// Check that a mutation can only be inserted with an explicit mutation state.
	tableDesc := desc.GetTable()
	tableDesc.Mutations = []csql.DescriptorMutation{{}}
	if err := tableDesc.Validate(); !testutils.IsError(err, "mutation in state UNKNOWN, direction NONE, and no column/index descriptor") {
		t.Fatal(err)
	}
	tableDesc.Mutations = []csql.DescriptorMutation{{Descriptor_: &csql.DescriptorMutation_Column{Column: &tableDesc.Columns[len(tableDesc.Columns)-1]}}}
	tableDesc.Columns = tableDesc.Columns[:len(tableDesc.Columns)-1]
	if err := tableDesc.Validate(); !testutils.IsError(err, "mutation in state UNKNOWN, direction NONE, col i, id 3") {
		t.Fatal(err)
	}
}

// writeIndexMutation adds index as a mutation and writes the
// descriptor to the DB.
func (mt mutationTest) writeIndexMutation(index string, state csql.DescriptorMutation_State) error {
	tableDesc := mt.desc.GetTable()
	i, err := tableDesc.FindIndexByName(index)
	if err != nil {
		return err
	}
	idx := &tableDesc.Indexes[i]
	m := csql.DescriptorMutation{Descriptor_: &csql.DescriptorMutation_Index{Index: idx}, State: state}
	// randomly pick add/drop mutation.
	r := rand.Intn(2)
	if r == 0 {
		m.Direction = csql.DescriptorMutation_ADD
	} else {
		m.Direction = csql.DescriptorMutation_DROP
	}
	tableDesc.Mutations = append(tableDesc.Mutations, m)
	tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)
	if err := tableDesc.Validate(); err != nil {
		return err
	}
	return mt.kvDB.Put(mt.descKey, mt.desc)
}

func TestOperationsWithIndexMutation(t *testing.T) {
	defer leaktest.AfterTest(t)
	// The descriptor changes made must have an immediate effect.
	defer csql.TestDisableTableLeases()()
	server, sqlDB, kvDB := setup(t)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR, INDEX foo (v));
`); err != nil {
		t.Fatal(err)
	}

	// read table descriptor
	nameKey := csql.MakeNameMetadataKey(keys.MaxReservedDescID+1, "test")
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

	mTest := mutationTest{
		kvDB:    kvDB,
		sqlDB:   sqlDB,
		descKey: descKey,
		desc:    desc,
	}

	starQuery := `SELECT * FROM t.test`
	indexQuery := `SELECT * FROM t.test@foo`
	// See the effect of the operations depending on the state.
	for _, state := range []csql.DescriptorMutation_State{csql.DescriptorMutation_DELETE_ONLY, csql.DescriptorMutation_WRITE_ONLY} {
		// Init table with some entries.
		if _, err := sqlDB.Exec(`TRUNCATE TABLE t.test`); err != nil {
			t.Fatal(err)
		}
		initRows := [][]string{{"a", "z"}, {"b", "y"}}
		for _, row := range initRows {
			if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ($1, $2)`, row[0], row[1]); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := mTest.checkQueryResponse(starQuery, initRows); err != nil {
			t.Fatal(err)
		}

		// Index foo is visible.
		if _, err := mTest.checkQueryResponse(indexQuery, [][]string{{"y"}, {"z"}}); err != nil {
			t.Fatal(err)
		}

		// Index foo is invisible once it's a mutation.
		if err := mTest.writeIndexMutation("foo", state); err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Query(indexQuery); !testutils.IsError(err, "index \"foo\" not found") {
			t.Fatal(err)
		}

		// Insert a new entry.
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ('c', 'x')`); err != nil {
			t.Fatal(err)
		}
		if _, err := mTest.checkQueryResponse(starQuery, [][]string{{"a", "z"}, {"b", "y"}, {"c", "x"}}); err != nil {
			t.Fatal(err)
		}

		// Make index "foo" live so that we can read it.
		if err := mTest.makeMutationsActive(); err != nil {
			t.Fatal(err)
		}
		if state == csql.DescriptorMutation_DELETE_ONLY {
			// "x" didn't get added to the index.
			if _, err := mTest.checkQueryResponse(indexQuery, [][]string{{"y"}, {"z"}}); err != nil {
				t.Fatal(err)
			}

		} else {
			// "x" got added to the index.
			if _, err := mTest.checkQueryResponse(indexQuery, [][]string{{"x"}, {"y"}, {"z"}}); err != nil {
				t.Fatal(err)
			}

		}

		// Make "foo" a mutation.
		if err := mTest.writeIndexMutation("foo", state); err != nil {
			t.Fatal(err)
		}
		// Update.
		if _, err := sqlDB.Exec(`UPDATE t.test SET v = 'w' WHERE k = 'c'`); err != nil {
			t.Fatal(err)
		}
		// Update "v" to its current value "z" in row "a".
		if _, err := sqlDB.Exec(`UPDATE t.test SET v = 'z' WHERE k = 'a'`); err != nil {
			t.Fatal(err)
		}
		if _, err := mTest.checkQueryResponse(starQuery, [][]string{{"a", "z"}, {"b", "y"}, {"c", "w"}}); err != nil {
			t.Fatal(err)
		}

		// Make index "foo" live so that we can read it.
		if err := mTest.makeMutationsActive(); err != nil {
			t.Fatal(err)
		}
		if state == csql.DescriptorMutation_DELETE_ONLY {
			// updating "x" -> "w" is a noop on the index,
			// updating "z" -> "z" results in "z" being deleted from the index.
			if _, err := mTest.checkQueryResponse(indexQuery, [][]string{{"y"}, {"z"}}); err != nil {
				t.Fatal(err)
			}

		} else {
			// updating "x" -> "w" results in the index updating from "x" -> "w",
			// updating "z" -> "z" is a noop on the index.
			if _, err := mTest.checkQueryResponse(indexQuery, [][]string{{"w"}, {"y"}, {"z"}}); err != nil {
				t.Fatal(err)
			}

		}

		// Make "foo" a mutation.
		if err := mTest.writeIndexMutation("foo", state); err != nil {
			t.Fatal(err)
		}
		// Delete row "b".
		if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = 'b'`); err != nil {
			t.Fatal(err)
		}
		if _, err := mTest.checkQueryResponse(starQuery, [][]string{{"a", "z"}, {"c", "w"}}); err != nil {
			t.Fatal(err)
		}

		// Make index "foo" live so that we can read it.
		if err := mTest.makeMutationsActive(); err != nil {
			t.Fatal(err)
		}
		// deleting row "b" deletes "y" from the index.
		if state == csql.DescriptorMutation_DELETE_ONLY {
			if _, err := mTest.checkQueryResponse(indexQuery, [][]string{{"z"}}); err != nil {
				t.Fatal(err)
			}

		} else {
			if _, err := mTest.checkQueryResponse(indexQuery, [][]string{{"w"}, {"z"}}); err != nil {
				t.Fatal(err)
			}

		}
	}

	// Check that a mutation can only be inserted with an explicit mutation state.
	tableDesc := desc.GetTable()
	tableDesc.Mutations = []csql.DescriptorMutation{{Descriptor_: &csql.DescriptorMutation_Index{Index: &tableDesc.Indexes[len(tableDesc.Indexes)-1]}}}
	tableDesc.Indexes = tableDesc.Indexes[:len(tableDesc.Indexes)-1]
	if err := tableDesc.Validate(); !testutils.IsError(err, "mutation in state UNKNOWN, direction NONE, index foo, id 2") {
		t.Fatal(err)
	}
}
