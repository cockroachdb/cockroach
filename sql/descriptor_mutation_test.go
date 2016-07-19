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
	gosql "database/sql"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	csql "github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/testutils/serverutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

type mutationTest struct {
	*testing.T
	kvDB      *client.DB
	sqlDB     *gosql.DB
	tableDesc *sqlbase.TableDescriptor
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
			mt.Errorf("expected less than %d rows, got %d rows:, %v", len(e), i, e)
			return numVals
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
					mt.Errorf("expected %v, found %v", e[i][j], s)
				}
				numVals++
			} else if e[i][j] != "NULL" {
				mt.Errorf("expected %v, found %v", e[i][j], "NULL")
			}
		}
	}
	if i != len(e) {
		mt.Errorf("fewer rows read than expected: found %d, expected %v", i, e)
	}
	return numVals
}

// checkTableSize checks that the number of key:value pairs stored
// in the table equals e.
func (mt mutationTest) checkTableSize(e int) {
	// Check that there are no hidden values
	tablePrefix := keys.MakeTablePrefix(uint32(mt.tableDesc.ID))
	tableStartKey := roachpb.Key(tablePrefix)
	tableEndKey := tableStartKey.PrefixEnd()
	if kvs, err := mt.kvDB.Scan(tableStartKey, tableEndKey, 0); err != nil {
		mt.Error(err)
	} else if len(kvs) != e {
		mt.Errorf("expected %d key value pairs, but got %d", e, len(kvs))
	}
}

// Convert all the mutations into live descriptors for the table
// and write the updated table descriptor to the DB.
func (mt mutationTest) makeMutationsActive() {
	// Remove mutation to check real values in DB using SQL
	if mt.tableDesc.Mutations == nil || len(mt.tableDesc.Mutations) == 0 {
		mt.Fatal("No mutations to make active")
	}
	for _, m := range mt.tableDesc.Mutations {
		if col := m.GetColumn(); col != nil {
			mt.tableDesc.Columns = append(mt.tableDesc.Columns, *col)
		} else if index := m.GetIndex(); index != nil {
			mt.tableDesc.Indexes = append(mt.tableDesc.Indexes, *index)
		} else {
			mt.Fatalf("no descriptor in mutation: %v", m)
		}
	}
	mt.tableDesc.Mutations = nil
	if err := mt.tableDesc.ValidateTable(); err != nil {
		mt.Fatal(err)
	}
	if err := mt.kvDB.Put(
		sqlbase.MakeDescMetadataKey(mt.tableDesc.ID),
		sqlbase.WrapDescriptor(mt.tableDesc),
	); err != nil {
		mt.Fatal(err)
	}
}

// writeColumnMutation adds column as a mutation and writes the
// descriptor to the DB.
func (mt mutationTest) writeColumnMutation(column string, m sqlbase.DescriptorMutation) {
	_, i, err := mt.tableDesc.FindColumnByName(column)
	if err != nil {
		mt.Fatal(err)
	}
	col := mt.tableDesc.Columns[i]
	mt.tableDesc.Columns = append(mt.tableDesc.Columns[:i], mt.tableDesc.Columns[i+1:]...)
	m.Descriptor_ = &sqlbase.DescriptorMutation_Column{Column: &col}
	mt.writeMutation(m)
}

// writeMutation writes the mutation to the table descriptor. If the
// State or the Direction is undefined, these values are populated via
// picking random values before the mutation is written.
func (mt mutationTest) writeMutation(m sqlbase.DescriptorMutation) {
	if m.Direction == sqlbase.DescriptorMutation_NONE {
		// randomly pick ADD/DROP mutation if this is the first mutation, or
		// pick the direction already chosen for the first mutation.
		if len(mt.tableDesc.Mutations) > 0 {
			m.Direction = mt.tableDesc.Mutations[0].Direction
		} else {
			m.Direction = sqlbase.DescriptorMutation_DROP
			if rand.Intn(2) == 0 {
				m.Direction = sqlbase.DescriptorMutation_ADD
			}
		}
	}
	if m.State == sqlbase.DescriptorMutation_UNKNOWN {
		// randomly pick DELETE_ONLY/WRITE_ONLY state.
		r := rand.Intn(2)
		if r == 0 {
			m.State = sqlbase.DescriptorMutation_DELETE_ONLY
		} else {
			m.State = sqlbase.DescriptorMutation_WRITE_ONLY
		}
	}
	mt.tableDesc.Mutations = append(mt.tableDesc.Mutations, m)
	if err := mt.tableDesc.ValidateTable(); err != nil {
		mt.Fatal(err)
	}
	if err := mt.kvDB.Put(
		sqlbase.MakeDescMetadataKey(mt.tableDesc.ID),
		sqlbase.WrapDescriptor(mt.tableDesc),
	); err != nil {
		mt.Fatal(err)
	}
}

func TestOperationsWithColumnMutation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer csql.TestDisableTableLeases()()
	// Disable external processing of mutations.
	params, _ := createTestServerParams()
	params.Knobs.SQLSchemaChangeManager = &csql.SchemaChangeManagerTestingKnobs{
		AsyncSchemaChangerExecNotification: schemaChangeManagerDisabled,
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop()

	// Fix the column families so the key counts below don't change if the
	// family heuristics are updated.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR, i CHAR DEFAULT 'i', FAMILY (k), FAMILY (v), FAMILY (i));
`); err != nil {
		t.Fatal(err)
	}

	// read table descriptor
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	mTest := mutationTest{
		T:         t,
		kvDB:      kvDB,
		sqlDB:     sqlDB,
		tableDesc: tableDesc,
	}

	starQuery := `SELECT * FROM t.test`
	// Run the tests for both states.
	for _, state := range []sqlbase.DescriptorMutation_State{sqlbase.DescriptorMutation_DELETE_ONLY, sqlbase.DescriptorMutation_WRITE_ONLY} {
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
		mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})
		// A direct read of column "i" fails.
		if _, err := sqlDB.Query(`SELECT i FROM t.test`); err == nil {
			t.Fatalf("Read succeeded despite column being in %v state", sqlbase.DescriptorMutation{State: state})
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
		if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ('b', 'y', 'i')`); !testutils.IsError(err, "INSERT error: table t.test has 2 columns but 3 values were supplied") {
			t.Fatal(err)
		}
		// Make column "i" live so that it is read.
		mTest.makeMutationsActive()
		// Check that we can read all the rows and columns.
		_ = mTest.checkQueryResponse(starQuery, initRows)

		var afterInsert, afterUpdate, afterDelete [][]string
		if state == sqlbase.DescriptorMutation_DELETE_ONLY {
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
		mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})
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
		mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})
		// Updating column "i" for a row fails.
		if _, err := sqlDB.Exec(`UPDATE t.test SET (v, i) = ('u', 'u') WHERE k = 'a'`); !testutils.IsError(err, `column "i" does not exist`) {
			t.Fatal(err)
		}
		// Make column "i" live so that it is read.
		mTest.makeMutationsActive()
		// The above failed update was a noop.
		_ = mTest.checkQueryResponse(starQuery, afterInsert)

		// Make column "i" a mutation.
		mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})
		// Update a row without specifying  mutation column "i".
		if _, err := sqlDB.Exec(`UPDATE t.test SET v = 'u' WHERE k = 'a'`); err != nil {
			t.Fatal(err)
		}
		// Make column "i" live so that it is read.
		mTest.makeMutationsActive()
		// The update to column "v" is seen; there is no effect on column "i".
		_ = mTest.checkQueryResponse(starQuery, afterUpdate)

		// Make column "i" a mutation.
		mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})
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
	tableDesc = mTest.tableDesc
	tableDesc.Mutations = []sqlbase.DescriptorMutation{{}}
	if err := tableDesc.ValidateTable(); !testutils.IsError(err, "mutation in state UNKNOWN, direction NONE, and no column/index descriptor") {
		t.Fatal(err)
	}
	tableDesc.Mutations = []sqlbase.DescriptorMutation{{Descriptor_: &sqlbase.DescriptorMutation_Column{Column: &tableDesc.Columns[len(tableDesc.Columns)-1]}}}
	tableDesc.Columns = tableDesc.Columns[:len(tableDesc.Columns)-1]
	if err := tableDesc.ValidateTable(); !testutils.IsError(err, "mutation in state UNKNOWN, direction NONE, col i, id 3") {
		t.Fatal(err)
	}
	tableDesc.Mutations[0].State = sqlbase.DescriptorMutation_DELETE_ONLY
	if err := tableDesc.ValidateTable(); !testutils.IsError(err, "mutation in state DELETE_ONLY, direction NONE, col i, id 3") {
		t.Fatal(err)
	}
	tableDesc.Mutations[0].State = sqlbase.DescriptorMutation_UNKNOWN
	tableDesc.Mutations[0].Direction = sqlbase.DescriptorMutation_DROP
	if err := tableDesc.ValidateTable(); !testutils.IsError(err, "mutation in state UNKNOWN, direction DROP, col i, id 3") {
		t.Fatal(err)
	}
}

// writeIndexMutation adds index as a mutation and writes the
// descriptor to the DB.
func (mt mutationTest) writeIndexMutation(index string, m sqlbase.DescriptorMutation) {
	tableDesc := mt.tableDesc
	_, i, err := tableDesc.FindIndexByName(index)
	if err != nil {
		mt.Fatal(err)
	}
	idx := tableDesc.Indexes[i]
	tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)
	m.Descriptor_ = &sqlbase.DescriptorMutation_Index{Index: &idx}
	mt.writeMutation(m)
}

func TestOperationsWithIndexMutation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect.
	defer csql.TestDisableTableLeases()()
	// Disable external processing of mutations.
	params, _ := createTestServerParams()
	params.Knobs.SQLSchemaChangeManager = &csql.SchemaChangeManagerTestingKnobs{
		AsyncSchemaChangerExecNotification: schemaChangeManagerDisabled,
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR, INDEX foo (v));
`); err != nil {
		t.Fatal(err)
	}

	// read table descriptor
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	mTest := mutationTest{
		T:         t,
		kvDB:      kvDB,
		sqlDB:     sqlDB,
		tableDesc: tableDesc,
	}

	starQuery := `SELECT * FROM t.test`
	indexQuery := `SELECT v FROM t.test@foo`
	// See the effect of the operations depending on the state.
	for _, state := range []sqlbase.DescriptorMutation_State{sqlbase.DescriptorMutation_DELETE_ONLY, sqlbase.DescriptorMutation_WRITE_ONLY} {
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
		mTest.writeIndexMutation("foo", sqlbase.DescriptorMutation{State: state})
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
		if state == sqlbase.DescriptorMutation_DELETE_ONLY {
			// "x" didn't get added to the index.
			_ = mTest.checkQueryResponse(indexQuery, [][]string{{"y"}, {"z"}})
		} else {
			// "x" got added to the index.
			_ = mTest.checkQueryResponse(indexQuery, [][]string{{"x"}, {"y"}, {"z"}})
		}

		// Make "foo" a mutation.
		mTest.writeIndexMutation("foo", sqlbase.DescriptorMutation{State: state})
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
		if state == sqlbase.DescriptorMutation_DELETE_ONLY {
			// updating "x" -> "w" is a noop on the index,
			// updating "z" -> "z" results in "z" being deleted from the index.
			_ = mTest.checkQueryResponse(indexQuery, [][]string{{"y"}, {"z"}})
		} else {
			// updating "x" -> "w" results in the index updating from "x" -> "w",
			// updating "z" -> "z" is a noop on the index.
			_ = mTest.checkQueryResponse(indexQuery, [][]string{{"w"}, {"y"}, {"z"}})
		}

		// Make "foo" a mutation.
		mTest.writeIndexMutation("foo", sqlbase.DescriptorMutation{State: state})
		// Delete row "b".
		if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = 'b'`); err != nil {
			t.Fatal(err)
		}
		_ = mTest.checkQueryResponse(starQuery, [][]string{{"a", "z"}, {"c", "w"}})

		// Make index "foo" live so that we can read it.
		mTest.makeMutationsActive()
		// Deleting row "b" deletes "y" from the index.
		if state == sqlbase.DescriptorMutation_DELETE_ONLY {
			mTest.checkQueryResponse(indexQuery, [][]string{{"z"}})
		} else {
			mTest.checkQueryResponse(indexQuery, [][]string{{"w"}, {"z"}})
		}
	}

	// Check that a mutation can only be inserted with an explicit mutation state.
	tableDesc = mTest.tableDesc
	tableDesc.Mutations = []sqlbase.DescriptorMutation{{Descriptor_: &sqlbase.DescriptorMutation_Index{Index: &tableDesc.Indexes[len(tableDesc.Indexes)-1]}}}
	tableDesc.Indexes = tableDesc.Indexes[:len(tableDesc.Indexes)-1]
	if err := tableDesc.ValidateTable(); !testutils.IsError(err, "mutation in state UNKNOWN, direction NONE, index foo, id 2") {
		t.Fatal(err)
	}
}

// TestOperationsWithUniqueColumnMutation tests all the operations while an
// index mutation refers to a column mutation.
func TestOperationsWithUniqueColumnMutation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer csql.TestDisableTableLeases()()
	// Disable external processing of mutations.
	params, _ := createTestServerParams()
	params.Knobs.SQLSchemaChangeManager = &csql.SchemaChangeManagerTestingKnobs{
		AsyncSchemaChangerExecNotification: schemaChangeManagerDisabled,
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop()

	// Create a table with column i and an index on v and i. Fix the column
	// families so the key counts below don't change if the family heuristics
	// are updated.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR, i CHAR, INDEX foo (i, v), FAMILY (k), FAMILY (v), FAMILY (i));
`); err != nil {
		t.Fatal(err)
	}

	// read table descriptor
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	mTest := mutationTest{
		T:         t,
		kvDB:      kvDB,
		sqlDB:     sqlDB,
		tableDesc: tableDesc,
	}

	starQuery := `SELECT * FROM t.test`
	indexQuery := `SELECT i FROM t.test@foo`
	// Run the tests for both states for a column and an index.
	for _, state := range []sqlbase.DescriptorMutation_State{
		sqlbase.DescriptorMutation_DELETE_ONLY,
		sqlbase.DescriptorMutation_WRITE_ONLY,
	} {
		for _, idxState := range []sqlbase.DescriptorMutation_State{
			sqlbase.DescriptorMutation_DELETE_ONLY,
			sqlbase.DescriptorMutation_WRITE_ONLY,
		} {
			// Ignore the impossible column in DELETE_ONLY state while index
			// is in the WRITE_ONLY state.
			if state == sqlbase.DescriptorMutation_DELETE_ONLY &&
				idxState == sqlbase.DescriptorMutation_WRITE_ONLY {
				continue
			}
			// Init table to start state.
			if _, err := sqlDB.Exec(`TRUNCATE TABLE t.test`); err != nil {
				t.Fatal(err)
			}
			initRows := [][]string{{"a", "z", "q"}, {"b", "y", "r"}}
			for _, row := range initRows {
				if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ($1, $2, $3)`, row[0], row[1], row[2]); err != nil {
					t.Fatal(err)
				}
			}
			// Check that the table only contains the initRows.
			_ = mTest.checkQueryResponse(starQuery, initRows)

			// Add index "foo" as a mutation.
			mTest.writeIndexMutation("foo", sqlbase.DescriptorMutation{State: idxState})
			// Make column "i" a mutation.
			mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})

			// Insert a row into the table.
			if _, err := sqlDB.Exec(`INSERT INTO t.test VALUES ('c', 'x')`); err != nil {
				t.Error(err)
			}

			// Make column "i" and index "foo" live.
			mTest.makeMutationsActive()
			// column "i" has no entry.
			_ = mTest.checkQueryResponse(starQuery, [][]string{{"a", "z", "q"}, {"b", "y", "r"}, {"c", "x", "NULL"}})
			if idxState == sqlbase.DescriptorMutation_DELETE_ONLY {
				// No index entry for row "c"
				_ = mTest.checkQueryResponse(indexQuery, [][]string{{"q"}, {"r"}})
			} else {
				// Index entry for row "c"
				_ = mTest.checkQueryResponse(indexQuery, [][]string{{"NULL"}, {"q"}, {"r"}})
			}

			// Add index "foo" as a mutation.
			mTest.writeIndexMutation("foo", sqlbase.DescriptorMutation{State: idxState})
			// Make column "i" a mutation.
			mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})

			// Updating column "i" for a row fails.
			if _, err := sqlDB.Exec(`UPDATE t.test SET (v, i) = ('u', 'u') WHERE k = 'a'`); !testutils.IsError(err, `column "i" does not exist`) {
				t.Error(err)
			}

			// Update a row without specifying  mutation column "i".
			if _, err := sqlDB.Exec(`UPDATE t.test SET v = 'u' WHERE k = 'a'`); err != nil {
				t.Error(err)
			}
			// Make column "i" and index "foo" live.
			mTest.makeMutationsActive()

			// The update to column "v" is seen; there is no effect on column "i".
			_ = mTest.checkQueryResponse(starQuery, [][]string{{"a", "u", "q"}, {"b", "y", "r"}, {"c", "x", "NULL"}})
			if idxState == sqlbase.DescriptorMutation_DELETE_ONLY {
				// Index entry for row "a" is deleted.
				_ = mTest.checkQueryResponse(indexQuery, [][]string{{"r"}})
			} else {
				// No change in index "foo"
				_ = mTest.checkQueryResponse(indexQuery, [][]string{{"NULL"}, {"q"}, {"r"}})
			}

			// Add index "foo" as a mutation.
			mTest.writeIndexMutation("foo", sqlbase.DescriptorMutation{State: idxState})
			// Make column "i" a mutation.
			mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})

			// Delete row "b".
			if _, err := sqlDB.Exec(`DELETE FROM t.test WHERE k = 'b'`); err != nil {
				t.Error(err)
			}
			// Make column "i" and index "foo" live.
			mTest.makeMutationsActive()
			// Row "b" is deleted. numVals is the number of non-NULL values seen,
			// or the number of KV values belonging to all the rows in the table
			// excluding row "b" since it's deleted.
			numVals := mTest.checkQueryResponse(starQuery, [][]string{{"a", "u", "q"}, {"c", "x", "NULL"}})
			// idxVals is the number of index values seen.
			var idxVals int
			if idxState == sqlbase.DescriptorMutation_DELETE_ONLY {
				// Index entry for row "b" is deleted.
				idxVals = mTest.checkQueryResponse(indexQuery, [][]string{})
			} else {
				// Index entry for row "b" is deleted. idxVals doesn't account for
				// the NULL value seen.
				idxVals = mTest.checkQueryResponse(indexQuery, [][]string{{"NULL"}, {"q"}})
				// Increment idxVals to account for the NULL value seen above.
				idxVals++
			}
			// Check that there are no hidden KV values for row "b", and column
			// "i" for row "b" was deleted. Also check that the index values are
			// all accounted for.
			mTest.checkTableSize(numVals + idxVals)
		}
	}
}

// TestSchemaChangeCommandsWithPendingMutations tests how schema change
// commands behave when they are referencing schema elements that are
// mutations that are not yet live.
func TestSchemaChangeCommandsWithPendingMutations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer csql.TestDisableTableLeases()()
	// Disable external processing of mutations.
	params, _ := createTestServerParams()
	params.Knobs.SQLSchemaChangeManager = &csql.SchemaChangeManagerTestingKnobs{
		AsyncSchemaChangerExecNotification: schemaChangeManagerDisabled,
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop()

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (a CHAR PRIMARY KEY, b CHAR, c CHAR, INDEX foo (c));
`); err != nil {
		t.Fatal(err)
	}

	// Read table descriptor
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	mt := mutationTest{
		T:         t,
		kvDB:      kvDB,
		sqlDB:     sqlDB,
		tableDesc: tableDesc,
	}

	// Test CREATE INDEX in the presence of mutations.

	// Add index DROP mutation "foo""
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`CREATE INDEX foo ON t.test (c)`); !testutils.IsError(err, `index "foo" being dropped, try again later`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()

	// "foo" is being added.
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`CREATE INDEX foo ON t.test (c)`); !testutils.IsError(err, `duplicate index name: "foo"`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// Add column DROP mutation "b"
	mt.writeColumnMutation("b", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`CREATE INDEX bar ON t.test (b)`); !testutils.IsError(err, `index "bar" contains unknown column "b"`) {
		t.Fatal(err)
	}
	// Make "b" live.
	mt.makeMutationsActive()
	// "b" is being added.
	mt.writeColumnMutation("b", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	// An index referencing a column mutation that is being added
	// is allowed to be added.
	if _, err := sqlDB.Exec(`CREATE INDEX bar ON t.test (b)`); err != nil {
		t.Fatal(err)
	}
	// Make "b" live.
	mt.makeMutationsActive()

	// Test DROP INDEX in the presence of mutations.

	// Add index DROP mutation "foo""
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	// Noop.
	if _, err := sqlDB.Exec(`DROP INDEX t.test@foo`); err != nil {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// "foo" is being added.
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`DROP INDEX t.test@foo`); !testutils.IsError(err, `index "foo" in the middle of being added, try again later`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// Test ALTER TABLE ADD/DROP column in the presence of mutations.

	// Add column DROP mutation "b"
	mt.writeColumnMutation("b", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
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
	mt.writeColumnMutation("b", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
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
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD CONSTRAINT foo UNIQUE (c)`); !testutils.IsError(err, `index "foo" being dropped, try again later`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// "foo" is being added.
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD CONSTRAINT foo UNIQUE (c)`); !testutils.IsError(err, `duplicate index name: "foo"`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// Add column mutation "b"
	mt.writeColumnMutation("b", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD CONSTRAINT bar UNIQUE (b)`); !testutils.IsError(err, `index "bar" contains unknown column "b"`) {
		t.Fatal(err)
	}
	// Make "b" live.
	mt.makeMutationsActive()
	// "b" is being added.
	mt.writeColumnMutation("b", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	// Noop.
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ADD CONSTRAINT bar UNIQUE (b)`); err != nil {
		t.Fatal(err)
	}
	// Make "b" live.
	mt.makeMutationsActive()

	// Test DROP CONSTRAINT in the presence of mutations.

	// Add index mutation "foo""
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	// Noop.
	if _, err := sqlDB.Exec(`ALTER TABLE t.test DROP CONSTRAINT foo`); err != nil {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// "foo" is being added.
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test DROP CONSTRAINT foo`); !testutils.IsError(err, `constraint "foo" in the middle of being added, try again later`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()

	// Rename column/index, while index is under mutation.

	// Add index mutation "foo""
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{})
	if _, err := sqlDB.Exec(`ALTER INDEX t.test@foo RENAME to ufo`); err != nil {
		mt.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.test RENAME COLUMN c TO d`); err != nil {
		mt.Fatal(err)
	}
	// The mutation in the table descriptor has changed and we would like
	// to update our copy to make it live.
	mt.tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Make "ufo" live.
	mt.makeMutationsActive()
	// The index has been renamed to ufo, and the column to d.
	_ = mt.checkQueryResponse("SHOW INDEXES FROM t.test", [][]string{{"test", "primary", "true", "1", "a", "ASC", "false"}, {"test", "ufo", "false", "1", "d", "ASC", "false"}})

	// Rename column under mutation works properly.

	// Add column mutation "b".
	mt.writeColumnMutation("b", sqlbase.DescriptorMutation{})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test RENAME COLUMN b TO e`); err != nil {
		mt.Fatal(err)
	}

	// The mutation in the table descriptor has changed and we would like
	// to update our copy to make it live.
	mt.tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Make column "e" live.
	mt.makeMutationsActive()
	// Column b changed to d.
	_ = mt.checkQueryResponse("SHOW COLUMNS FROM t.test", [][]string{{"a", "STRING", "false", "NULL"}, {"d", "STRING", "true", "NULL"}, {"e", "STRING", "true", "NULL"}})

	// Try to change column defaults while column is under mutation.
	mt.writeColumnMutation("e", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ALTER COLUMN e SET DEFAULT 'a'`); !testutils.IsError(
		err, `column "e" in the middle of being added`) {
		t.Fatal(err)
	}
	mt.makeMutationsActive()
	mt.writeColumnMutation("e", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`ALTER TABLE t.test ALTER COLUMN e SET DEFAULT 'a'`); !testutils.IsError(
		err, `column "e" in the middle of being dropped`) {
		t.Fatal(err)
	}
	mt.makeMutationsActive()
}

// TestTableMutationQueue tests that schema elements when added are
// assigned the correct start state and mutation id.
func TestTableMutationQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Disable synchronous and asynchronous schema change processing so that
	// the mutations get queued up.
	params, _ := createTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLExecutor: &csql.ExecutorTestingKnobs{
			SyncSchemaChangersFilter: func(tscc csql.TestingSchemaChangerCollection) {
				tscc.ClearSchemaChangers()
			},
		},
		SQLSchemaChangeManager: &csql.SchemaChangeManagerTestingKnobs{
			AsyncSchemaChangerExecNotification: schemaChangeManagerDisabled,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop()

	// Create a table with column i and an index on v and i.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k CHAR PRIMARY KEY, v CHAR);
`); err != nil {
		t.Fatal(err)
	}

	// Run some schema changes.

	// This single command creates three columns and two indexes sharing the
	// same mutation ID.
	if _, err := sqlDB.Exec(
		`ALTER TABLE t.test ADD d INT UNIQUE, ADD e INT UNIQUE, ADD f INT`,
	); err != nil {
		t.Fatal(err)
	}

	// This command creates two mutations sharing the same mutation ID.
	if _, err := sqlDB.Exec(
		`ALTER TABLE t.test ADD g INT, ADD CONSTRAINT idx_f UNIQUE (f)`,
	); err != nil {
		t.Fatal(err)
	}

	// This command creates a single mutation.
	if _, err := sqlDB.Exec(`CREATE UNIQUE INDEX idx_g ON t.test (g)`); err != nil {
		t.Fatal(err)
	}

	// This command created a drop mutation.
	if _, err := sqlDB.Exec(`ALTER TABLE t.test DROP v`); err != nil {
		t.Fatal(err)
	}

	// read table descriptor
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	expected := []struct {
		name  string
		id    sqlbase.MutationID
		state sqlbase.DescriptorMutation_State
	}{
		{"d", 1, sqlbase.DescriptorMutation_DELETE_ONLY},
		{"test_d_key", 1, sqlbase.DescriptorMutation_DELETE_ONLY},
		{"e", 1, sqlbase.DescriptorMutation_DELETE_ONLY},
		{"test_e_key", 1, sqlbase.DescriptorMutation_DELETE_ONLY},
		{"f", 1, sqlbase.DescriptorMutation_DELETE_ONLY},
		// Second schema change.
		{"g", 2, sqlbase.DescriptorMutation_DELETE_ONLY},
		{"idx_f", 2, sqlbase.DescriptorMutation_DELETE_ONLY},
		// Third.
		{"idx_g", 3, sqlbase.DescriptorMutation_DELETE_ONLY},
		// Drop mutations start off in the WRITE_ONLY state.
		{"v", 4, sqlbase.DescriptorMutation_WRITE_ONLY},
	}

	if len(tableDesc.Mutations) != len(expected) {
		t.Fatalf("%d mutations, instead of expected %d", len(tableDesc.Mutations), len(expected))
	}

	for i, m := range tableDesc.Mutations {
		name := expected[i].name
		if col := m.GetColumn(); col != nil {
			if col.Name != name {
				t.Errorf("%d entry: name %s, expected %s", i, col.Name, name)
			}
		}
		if idx := m.GetIndex(); idx != nil {
			if idx.Name != name {
				t.Errorf("%d entry: name %s, expected %s", i, idx.Name, name)
			}
		}
		if id := expected[i].id; m.MutationID != id {
			t.Errorf("%d entry: id %d, expected %d", i, m.MutationID, id)
		}
		if state := expected[i].state; m.State != state {
			t.Errorf("%d entry: state %s, expected %s", i, m.State, state)
		}
	}
}
