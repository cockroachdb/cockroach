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
// permissions and limitations under the License.
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
	"github.com/cockroachdb/cockroach/util/leaktest"
)

type mutationTest struct {
	*testing.T
	kvDB    *client.DB
	sqlDB   *sql.DB
	descKey roachpb.Key
	desc    *csql.Descriptor
}

// checkQueryResponse runs the sql query q, and checks that it matches
// the expected response. It returns the total number of non-null values
// returned in the response (num-rows*num-columns - total-num-null-values),
// as a measure of the number of key:value pairs visible to it.
func (mt mutationTest) checkQueryResponse(q string, e [][]string) int {
	// Read from DB.
	rows, err := mt.sqlDB.Query(q)
	if err != nil {
		mt.Fatal(err)
	}
	cols, err := rows.Columns()
	if err != nil {
		mt.Fatal(err)
	}
	if len(e) > 0 && len(cols) != len(e[0]) {
		mt.Fatalf("wrong number of columns %d in response to query %s", len(cols), q)
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
			mt.Fatalf("expected less than %d rows, got %d rows:, %v", len(e), i, e)
		}
		if err := rows.Scan(vals...); err != nil {
			mt.Fatal(err)
		}
		for j, v := range vals {
			if val := *v.(*interface{}); val != nil {
				var s string
				switch t := val.(type) {
				case []byte:
					s = string(t)
				default:
					s = fmt.Sprint(val)
				}
				if e[i][j] != s {
					mt.Fatalf("e:%v, v:%v", e[i][j], s)
				}
				numVals++
			} else if e[i][j] != "NULL" {
				mt.Fatalf("e:%v, v:%v", e[i][j], "NULL")
			}
		}
	}
	if i != len(e) {
		mt.Fatalf("fewer rows read than expected: %d, e=%v", i, e)
	}
	return numVals
}

// checkTableSize checks that the number of key:value pairs stored
// in the table equals e.
func (mt mutationTest) checkTableSize(e int) {
	// Check that there are no hidden values
	tableDesc := mt.desc.GetTable()
	tablePrefix := keys.MakeTablePrefix(uint32(tableDesc.ID))
	tableStartKey := roachpb.Key(tablePrefix)
	tableEndKey := tableStartKey.PrefixEnd()
	if kvs, err := mt.kvDB.Scan(tableStartKey, tableEndKey, 0); err != nil {
		mt.Fatal(err)
	} else if len(kvs) != e {
		mt.Fatalf("expected %d key value pairs, but got %d", e, len(kvs))
	}
}

// Convert all the mutations into live descriptors for the table
// and write the updated table descriptor to the DB.
func (mt mutationTest) makeMutationsActive() {
	// Remove mutation to check real values in DB using SQL
	tableDesc := mt.desc.GetTable()
	if tableDesc.Mutations == nil || len(tableDesc.Mutations) == 0 {
		mt.Fatal("No mutations to make active")
	}
	for _, m := range tableDesc.Mutations {
		if col := m.GetColumn(); col != nil {
			tableDesc.Columns = append(tableDesc.Columns, *col)
		} else if index := m.GetIndex(); index != nil {
			tableDesc.Indexes = append(tableDesc.Indexes, *index)
		} else {
			mt.Fatalf("no descriptor in mutation: %v", m)
		}
	}
	tableDesc.Mutations = nil
	if err := tableDesc.Validate(); err != nil {
		mt.Fatal(err)
	}
	if err := mt.kvDB.Put(mt.descKey, mt.desc); err != nil {
		mt.Fatal(err)
	}
}

// writeColumnMutation adds column as a mutation and writes the
// descriptor to the DB.
func (mt mutationTest) writeColumnMutation(column string, m csql.DescriptorMutation) {
	tableDesc := mt.desc.GetTable()
	_, i, err := tableDesc.FindColumnByName(column)
	if err != nil {
		mt.Fatal(err)
	}
	col := tableDesc.Columns[i]
	tableDesc.Columns = append(tableDesc.Columns[:i], tableDesc.Columns[i+1:]...)
	m.Descriptor_ = &csql.DescriptorMutation_Column{Column: &col}
	mt.writeMutation(m)
}

// writeMutation writes the mutation to the table descriptor. If the
// State or the Direction is undefined, these values are populated via
// picking random values before the mutation is written.
func (mt mutationTest) writeMutation(m csql.DescriptorMutation) {
	if m.Direction == csql.DescriptorMutation_NONE {
		// randomly pick ADD/DROP mutation.
		r := rand.Intn(2)
		if r == 0 {
			m.Direction = csql.DescriptorMutation_ADD
		} else {
			m.Direction = csql.DescriptorMutation_DROP
		}
	}
	if m.State == csql.DescriptorMutation_UNKNOWN {
		// randomly pick DELETE_ONLY/WRITE_ONLY state.
		r := rand.Intn(2)
		if r == 0 {
			m.State = csql.DescriptorMutation_DELETE_ONLY
		} else {
			m.State = csql.DescriptorMutation_WRITE_ONLY
		}
	}
	tableDesc := mt.desc.GetTable()
	tableDesc.Mutations = append(tableDesc.Mutations, m)
	if err := tableDesc.Validate(); err != nil {
		mt.Fatal(err)
	}
	if err := mt.kvDB.Put(mt.descKey, mt.desc); err != nil {
		mt.Fatal(err)
	}
}

func TestOperationsWithColumnMutation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer csql.TestDisableTableLeases()()
	// Disable external processing of mutations.
	defer csql.TestDisableAsyncSchemaChangeExec()()
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
		T:       t,
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
		_ = mTest.checkQueryResponse(starQuery, initRows)

		// Add column "i" as a mutation.
		mTest.writeColumnMutation("i", csql.DescriptorMutation{State: state})
		// A direct read of column "i" fails.
		if _, err := sqlDB.Query(`SELECT i FROM t.test`); err == nil {
			t.Fatalf("Read succeeded despite column being in %v state", csql.DescriptorMutation{State: state})
		}
		// The table only contains columns "k" and "v".
		_ = mTest.checkQueryResponse(starQuery, [][]string{{"a", "z"}})

		// The column backfill uses Put instead of CPut because it depends on
		// an INSERT of a column in the WRITE_ONLY state failing. These two
		// tests guarantee that.

		// Inserting a row into the table while specifying column "i" results in an error.
		if _, err := sqlDB.Exec(`INSERT INTO t.test (k, v, i) VALUES ('b', 'y', 'i')`); !testutils.IsError(err, `column "i" does not exist`) {
			t.Fatal(err)
		}
		// Repeating the same without specifying the columns results in a different error.
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ('b', 'y', 'i')`); !testutils.IsError(err, "INSERT has more expressions than target columns: 3/2") {
			t.Fatal(err)
		}
		// Make column "i" live so that it is read.
		mTest.makeMutationsActive()
		// Check that we can read all the rows and columns.
		_ = mTest.checkQueryResponse(starQuery, initRows)

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
		mTest.writeColumnMutation("i", csql.DescriptorMutation{State: state})
		// Insert a row into the table.
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ('c', 'x')`); err != nil {
			t.Fatal(err)
		}
		// Make column "i" live so that it is read.
		mTest.makeMutationsActive()
		// Notice that the default value of "i" is only written when the
		// descriptor is in the WRITE_ONLY state.
		_ = mTest.checkQueryResponse(starQuery, afterInsert)

		// The column backfill uses Put instead of CPut because it depends on
		// an UPDATE of a column in the WRITE_ONLY state failing. This test
		// guarantees that.

		// Make column "i" a mutation.
		mTest.writeColumnMutation("i", csql.DescriptorMutation{State: state})
		// Updating column "i" for a row fails.
		if _, err := sqlDB.Exec(`UPDATE t.test SET (v, i) = ('u', 'u') WHERE k = 'a'`); !testutils.IsError(err, `column "i" does not exist`) {
			t.Fatal(err)
		}
		// Make column "i" live so that it is read.
		mTest.makeMutationsActive()
		// The above failed update was a noop.
		_ = mTest.checkQueryResponse(starQuery, afterInsert)

		// Make column "i" a mutation.
		mTest.writeColumnMutation("i", csql.DescriptorMutation{State: state})
		// Update a row without specifying  mutation column "i".
		if _, err := sqlDB.Exec(`UPDATE t.test SET v = 'u' WHERE k = 'a'`); err != nil {
			t.Fatal(err)
		}
		// Make column "i" live so that it is read.
		mTest.makeMutationsActive()
		// The update to column "v" is seen; there is no effect on column "i".
		_ = mTest.checkQueryResponse(starQuery, afterUpdate)

		// Make column "i" a mutation.
		mTest.writeColumnMutation("i", csql.DescriptorMutation{State: state})
		// Delete row "a".
		if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = 'a'`); err != nil {
			t.Fatal(err)
		}
		// Make column "i" live so that it is read.
		mTest.makeMutationsActive()
		// Row "a" is deleted. numVals is the number of non-NULL values seen,
		// or the number of KV values belonging to all the rows in the table
		// excluding row "a" since it's deleted.
		numVals := mTest.checkQueryResponse(starQuery, afterDelete)
		// Check that there are no hidden KV values for row "a",
		// and column "i" for row "a" was deleted.
		mTest.checkTableSize(numVals)
	}

	// Check that a mutation can only be inserted with an explicit mutation state, and direction.
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
	tableDesc.Mutations[0].State = csql.DescriptorMutation_DELETE_ONLY
	if err := tableDesc.Validate(); !testutils.IsError(err, "mutation in state DELETE_ONLY, direction NONE, col i, id 3") {
		t.Fatal(err)
	}
	tableDesc.Mutations[0].State = csql.DescriptorMutation_UNKNOWN
	tableDesc.Mutations[0].Direction = csql.DescriptorMutation_DROP
	if err := tableDesc.Validate(); !testutils.IsError(err, "mutation in state UNKNOWN, direction DROP, col i, id 3") {
		t.Fatal(err)
	}
}

// writeIndexMutation adds index as a mutation and writes the
// descriptor to the DB.
func (mt mutationTest) writeIndexMutation(index string, m csql.DescriptorMutation) {
	tableDesc := mt.desc.GetTable()
	_, i, err := tableDesc.FindIndexByName(index)
	if err != nil {
		mt.Fatal(err)
	}
	idx := tableDesc.Indexes[i]
	tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)
	m.Descriptor_ = &csql.DescriptorMutation_Index{Index: &idx}
	mt.writeMutation(m)
}

func TestOperationsWithIndexMutation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect.
	defer csql.TestDisableTableLeases()()
	// Disable external processing of mutations.
	defer csql.TestDisableAsyncSchemaChangeExec()()
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
		T:       t,
		kvDB:    kvDB,
		sqlDB:   sqlDB,
		descKey: descKey,
		desc:    desc,
	}

	starQuery := `SELECT * FROM t.test`
	indexQuery := `SELECT v FROM t.test@foo`
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
		_ = mTest.checkQueryResponse(starQuery, initRows)
		// Index foo is visible.
		_ = mTest.checkQueryResponse(indexQuery, [][]string{{"y"}, {"z"}})

		// Index foo is invisible once it's a mutation.
		mTest.writeIndexMutation("foo", csql.DescriptorMutation{State: state})
		if _, err := sqlDB.Query(indexQuery); !testutils.IsError(err, `index "foo" not found`) {
			t.Fatal(err)
		}

		// Insert a new entry.
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ('c', 'x')`); err != nil {
			t.Fatal(err)
		}
		_ = mTest.checkQueryResponse(starQuery, [][]string{{"a", "z"}, {"b", "y"}, {"c", "x"}})

		// Make index "foo" live so that we can read it.
		mTest.makeMutationsActive()
		if state == csql.DescriptorMutation_DELETE_ONLY {
			// "x" didn't get added to the index.
			_ = mTest.checkQueryResponse(indexQuery, [][]string{{"y"}, {"z"}})
		} else {
			// "x" got added to the index.
			_ = mTest.checkQueryResponse(indexQuery, [][]string{{"x"}, {"y"}, {"z"}})
		}

		// Make "foo" a mutation.
		mTest.writeIndexMutation("foo", csql.DescriptorMutation{State: state})
		// Update.
		if _, err := sqlDB.Exec(`UPDATE t.test SET v = 'w' WHERE k = 'c'`); err != nil {
			t.Fatal(err)
		}
		// Update "v" to its current value "z" in row "a".
		if _, err := sqlDB.Exec(`UPDATE t.test SET v = 'z' WHERE k = 'a'`); err != nil {
			t.Fatal(err)
		}
		_ = mTest.checkQueryResponse(starQuery, [][]string{{"a", "z"}, {"b", "y"}, {"c", "w"}})

		// Make index "foo" live so that we can read it.
		mTest.makeMutationsActive()
		if state == csql.DescriptorMutation_DELETE_ONLY {
			// updating "x" -> "w" is a noop on the index,
			// updating "z" -> "z" results in "z" being deleted from the index.
			_ = mTest.checkQueryResponse(indexQuery, [][]string{{"y"}, {"z"}})
		} else {
			// updating "x" -> "w" results in the index updating from "x" -> "w",
			// updating "z" -> "z" is a noop on the index.
			_ = mTest.checkQueryResponse(indexQuery, [][]string{{"w"}, {"y"}, {"z"}})
		}

		// Make "foo" a mutation.
		mTest.writeIndexMutation("foo", csql.DescriptorMutation{State: state})
		// Delete row "b".
		if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = 'b'`); err != nil {
			t.Fatal(err)
		}
		_ = mTest.checkQueryResponse(starQuery, [][]string{{"a", "z"}, {"c", "w"}})

		// Make index "foo" live so that we can read it.
		mTest.makeMutationsActive()
		// Deleting row "b" deletes "y" from the index.
		if state == csql.DescriptorMutation_DELETE_ONLY {
			mTest.checkQueryResponse(indexQuery, [][]string{{"z"}})
		} else {
			mTest.checkQueryResponse(indexQuery, [][]string{{"w"}, {"z"}})
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

func TestCommandsWithPendingMutations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer csql.TestDisableTableLeases()()
	// Disable external processing of mutations.
	defer csql.TestDisableAsyncSchemaChangeExec()()
	server, sqlDB, kvDB := setup(t)
	defer cleanup(server, sqlDB)

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (a CHAR PRIMARY KEY, b CHAR, c CHAR, INDEX foo (c));
`); err != nil {
		t.Fatal(err)
	}

	// Read table descriptor
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

	mt := mutationTest{
		T:       t,
		kvDB:    kvDB,
		sqlDB:   sqlDB,
		descKey: descKey,
		desc:    desc,
	}

	// Test CREATE INDEX in the presence of mutations.

	// Add index DROP mutation "foo""
	mt.writeIndexMutation("foo", csql.DescriptorMutation{Direction: csql.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`CREATE INDEX foo ON t.test (c)`); !testutils.IsError(err, `index "foo" being dropped, try again later`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()

	// "foo" is being added.
	mt.writeIndexMutation("foo", csql.DescriptorMutation{Direction: csql.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`CREATE INDEX foo ON t.test (c)`); !testutils.IsError(err, `duplicate index name: "foo"`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// Add column DROP mutation "b"
	mt.writeColumnMutation("b", csql.DescriptorMutation{Direction: csql.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`CREATE INDEX bar ON t.test (b)`); !testutils.IsError(err, `index "bar" contains unknown column "b"`) {
		t.Fatal(err)
	}
	// Make "b" live.
	mt.makeMutationsActive()
	// "b" is being added.
	mt.writeColumnMutation("b", csql.DescriptorMutation{Direction: csql.DescriptorMutation_ADD})
	// An index referencing a column mutation that is being added
	// is allowed to be added.
	if _, err := sqlDB.Exec(`CREATE INDEX bar ON t.test (b)`); err != nil {
		t.Fatal(err)
	}
	// Make "b" live.
	mt.makeMutationsActive()

	// Test DROP INDEX in the presence of mutations.

	// Add index DROP mutation "foo""
	mt.writeIndexMutation("foo", csql.DescriptorMutation{Direction: csql.DescriptorMutation_DROP})
	// Noop.
	if _, err := sqlDB.Exec(`DROP INDEX t.test@foo`); err != nil {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// "foo" is being added.
	mt.writeIndexMutation("foo", csql.DescriptorMutation{Direction: csql.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`DROP INDEX t.test@foo`); !testutils.IsError(err, `index "foo" in the middle of being added, try again later`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// Test ALTER TABLE ADD/DROP column in the presence of mutations.

	// Add column DROP mutation "b"
	mt.writeColumnMutation("b", csql.DescriptorMutation{Direction: csql.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD b CHAR`); !testutils.IsError(err, `column "b" being dropped, try again later`) {
		t.Fatal(err)
	}
	// Noop.
	if _, err := sqlDB.Exec(`ALTER TABLE t.test DROP b`); err != nil {
		t.Fatal(err)
	}
	// Make "b" live.
	mt.makeMutationsActive()
	// "b" is being added.
	mt.writeColumnMutation("b", csql.DescriptorMutation{Direction: csql.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD b CHAR`); !testutils.IsError(err, `duplicate column name: "b"`) {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.test DROP b`); !testutils.IsError(err, `column "b" in the middle of being added, try again later`) {
		t.Fatal(err)
	}
	// Make "b" live.
	mt.makeMutationsActive()

	// Test ALTER TABLE ADD CONSTRAINT in the presence of mutations.

	// Add index DROP mutation "foo""
	mt.writeIndexMutation("foo", csql.DescriptorMutation{Direction: csql.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD CONSTRAINT foo UNIQUE (c)`); !testutils.IsError(err, `index "foo" being dropped, try again later`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// "foo" is being added.
	mt.writeIndexMutation("foo", csql.DescriptorMutation{Direction: csql.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD CONSTRAINT foo UNIQUE (c)`); !testutils.IsError(err, `duplicate index name: "foo"`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// Add column mutation "b"
	mt.writeColumnMutation("b", csql.DescriptorMutation{Direction: csql.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD CONSTRAINT bar UNIQUE (b)`); !testutils.IsError(err, `index "bar" contains unknown column "b"`) {
		t.Fatal(err)
	}
	// Make "b" live.
	mt.makeMutationsActive()
	// "b" is being added.
	mt.writeColumnMutation("b", csql.DescriptorMutation{Direction: csql.DescriptorMutation_ADD})
	// Noop.
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD CONSTRAINT bar UNIQUE (b)`); err != nil {
		t.Fatal(err)
	}
	// Make "b" live.
	mt.makeMutationsActive()

	// Test DROP CONSTRAINT in the presence of mutations.

	// Add index mutation "foo""
	mt.writeIndexMutation("foo", csql.DescriptorMutation{Direction: csql.DescriptorMutation_DROP})
	// Noop.
	if _, err := sqlDB.Exec(`ALTER TABLE t.test DROP CONSTRAINT foo`); err != nil {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// "foo" is being added.
	mt.writeIndexMutation("foo", csql.DescriptorMutation{Direction: csql.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test DROP CONSTRAINT foo`); !testutils.IsError(err, `constraint "foo" in the middle of being added, try again later`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()

	// Rename column/index, while index is under mutation.

	// Add index mutation "foo""
	mt.writeIndexMutation("foo", csql.DescriptorMutation{})
	if _, err := sqlDB.Exec(`ALTER INDEX t.test@foo RENAME to ufo`); err != nil {
		mt.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.test RENAME COLUMN c TO d`); err != nil {
		mt.Fatal(err)
	}
	// The mutation in the table descriptor has changed and we would like
	// to update our copy to make it live.
	if err := mt.kvDB.GetProto(mt.descKey, mt.desc); err != nil {
		mt.Fatal(err)
	}
	// Make "ufo" live.
	mt.makeMutationsActive()
	// The index has been renamed to ufo, and the column to d.
	_ = mt.checkQueryResponse("SHOW INDEXES FROM t.test", [][]string{{"test", "primary", "true", "1", "a", "ASC", "false"}, {"test", "ufo", "false", "1", "d", "ASC", "false"}})

	// Rename column under mutation works properly.

	// Add column mutation "b".
	mt.writeColumnMutation("b", csql.DescriptorMutation{})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test RENAME COLUMN b TO e`); err != nil {
		mt.Fatal(err)
	}
	// The mutation in the table descriptor has changed and we would like
	// to update our copy to make it live.
	if err := mt.kvDB.GetProto(mt.descKey, mt.desc); err != nil {
		mt.Fatal(err)
	}
	// Make column "e" live.
	mt.makeMutationsActive()
	// Column b changed to d.
	_ = mt.checkQueryResponse("SHOW COLUMNS FROM t.test", [][]string{{"a", "STRING", "false", "NULL"}, {"d", "STRING", "true", "NULL"}, {"e", "STRING", "true", "NULL"}})

	// Try to change column defaults while column is under mutation.
	mt.writeColumnMutation("e", csql.DescriptorMutation{Direction: csql.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ALTER COLUMN e SET DEFAULT 'a'`); !testutils.IsError(
		err, `column "e" in the middle of being added`) {
		t.Fatal(err)
	}
	mt.makeMutationsActive()
	mt.writeColumnMutation("e", csql.DescriptorMutation{Direction: csql.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ALTER COLUMN e SET DEFAULT 'a'`); !testutils.IsError(
		err, `column "e" in the middle of being dropped`) {
		t.Fatal(err)
	}
	mt.makeMutationsActive()
}
