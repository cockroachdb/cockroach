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

package sql_test

import (
	"context"
	gosql "database/sql"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type mutationTest struct {
	// SQLRunner embeds testing.TB
	testing.TB
	*sqlutils.SQLRunner
	kvDB      *client.DB
	tableDesc *sqlbase.TableDescriptor
}

func makeMutationTest(
	t *testing.T, kvDB *client.DB, db *gosql.DB, tableDesc *sqlbase.TableDescriptor,
) mutationTest {
	return mutationTest{
		TB:        t,
		SQLRunner: sqlutils.MakeSQLRunner(db),
		kvDB:      kvDB,
		tableDesc: tableDesc,
	}
}

// checkTableSize checks that the number of key:value pairs stored
// in the table equals e.
func (mt mutationTest) checkTableSize(e int) {
	// Check that there are no hidden values
	tablePrefix := keys.MakeTablePrefix(uint32(mt.tableDesc.ID))
	tableStartKey := roachpb.Key(tablePrefix)
	tableEndKey := tableStartKey.PrefixEnd()
	if kvs, err := mt.kvDB.Scan(context.TODO(), tableStartKey, tableEndKey, 0); err != nil {
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
		context.TODO(),
		sqlbase.MakeDescMetadataKey(mt.tableDesc.ID),
		sqlbase.WrapDescriptor(mt.tableDesc),
	); err != nil {
		mt.Fatal(err)
	}
}

// writeColumnMutation adds column as a mutation and writes the
// descriptor to the DB.
func (mt mutationTest) writeColumnMutation(column string, m sqlbase.DescriptorMutation) {
	col, _, err := mt.tableDesc.FindColumnByName(tree.Name(column))
	if err != nil {
		mt.Fatal(err)
	}
	for i := range mt.tableDesc.Columns {
		if col.ID == mt.tableDesc.Columns[i].ID {
			mt.tableDesc.Columns = append(mt.tableDesc.Columns[:i], mt.tableDesc.Columns[i+1:]...)
			break
		}
	}
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
		// randomly pick DELETE_ONLY/DELETE_AND_WRITE_ONLY state.
		r := rand.Intn(2)
		if r == 0 {
			m.State = sqlbase.DescriptorMutation_DELETE_ONLY
		} else {
			m.State = sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY
		}
	}
	mt.tableDesc.Mutations = append(mt.tableDesc.Mutations, m)
	if err := mt.tableDesc.ValidateTable(); err != nil {
		mt.Fatal(err)
	}
	if err := mt.kvDB.Put(
		context.TODO(),
		sqlbase.MakeDescMetadataKey(mt.tableDesc.ID),
		sqlbase.WrapDescriptor(mt.tableDesc),
	); err != nil {
		mt.Fatal(err)
	}
}

// Test INSERT, UPDATE, UPSERT, and DELETE operations with a column schema
// change.
func TestOperationsWithColumnMutation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer sql.TestDisableTableLeases()()
	// Disable external processing of mutations.
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
		AsyncExecNotification: asyncSchemaChangerDisabled,
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	// Fix the column families so the key counts below don't change if the
	// family heuristics are updated.
	// Add an index so that we test adding a column when a table has an index.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k CHAR PRIMARY KEY, v CHAR, i CHAR DEFAULT 'i', FAMILY (k), FAMILY (v), FAMILY (i));
CREATE INDEX allidx ON t.public.test (k, v);
`); err != nil {
		t.Fatal(err)
	}

	// read table descriptor
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	mTest := makeMutationTest(t, kvDB, sqlDB, tableDesc)

	starQuery := `SELECT * FROM t.public.test`
	for _, useUpsert := range []bool{true, false} {
		// Run the tests for both states.
		for _, state := range []sqlbase.DescriptorMutation_State{sqlbase.DescriptorMutation_DELETE_ONLY,
			sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY} {
			// Init table to start state.
			mTest.Exec(t, `TRUNCATE TABLE t.public.test`)
			// read table descriptor
			mTest.tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")

			initRows := [][]string{{"a", "z", "q"}}
			for _, row := range initRows {
				if useUpsert {
					mTest.Exec(t, `UPSERT INTO t.public.test VALUES ($1, $2, $3)`, row[0], row[1], row[2])
				} else {
					mTest.Exec(t, `INSERT INTO t.public.test VALUES ($1, $2, $3)`, row[0], row[1], row[2])
				}
			}
			// Check that the table only contains the initRows.
			mTest.CheckQueryResults(t, starQuery, initRows)

			// Add column "i" as a mutation.
			mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})
			// A direct read of column "i" fails.
			if _, err := sqlDB.Query(`SELECT i FROM t.public.test`); err == nil {
				t.Fatalf("Read succeeded despite column being in %v state", sqlbase.DescriptorMutation{State: state})
			}
			// The table only contains columns "k" and "v".
			mTest.CheckQueryResults(t, starQuery, [][]string{{"a", "z"}})

			// The column backfill uses Put instead of CPut because it depends on
			// an INSERT of a column in the DELETE_AND_WRITE_ONLY state failing. These two
			// tests guarantee that.

			var err error
			// Inserting a row into the table while specifying column "i" results in an error.
			if useUpsert {
				_, err = sqlDB.Exec(`UPSERT INTO t.public.test (k, v, i) VALUES ('b', 'y', 'i')`)
			} else {
				_, err = sqlDB.Exec(`INSERT INTO t.public.test (k, v, i) VALUES ('b', 'y', 'i')`)
			}
			if !testutils.IsError(err, `column "i" does not exist`) {
				t.Fatal(err)
			}

			// Repeating the same without specifying the columns results in a different error.
			if useUpsert {
				_, err = sqlDB.Exec(`UPSERT INTO t.public.test VALUES ('b', 'y', 'i')`)
			} else {
				_, err = sqlDB.Exec(`INSERT INTO t.public.test VALUES ('b', 'y', 'i')`)
			}
			if !testutils.IsError(err, "INSERT has more expressions than target columns, 3 expressions for 2 targets") {
				t.Fatal(err)
			}

			// Make column "i" live so that it is read.
			mTest.makeMutationsActive()
			// Check that we can read all the rows and columns.
			mTest.CheckQueryResults(t, starQuery, initRows)

			var afterInsert, afterUpdate, afterPKUpdate, afterDelete [][]string
			var afterDeleteKeys int
			if state == sqlbase.DescriptorMutation_DELETE_ONLY {
				// The default value of "i" for column "i" is not written.
				afterInsert = [][]string{{"a", "z", "q"}, {"c", "x", "NULL"}}
				// Update is a noop for column "i".
				afterUpdate = [][]string{{"a", "u", "q"}, {"c", "x", "NULL"}}
				// Update the pk of the second tuple from c to d
				afterPKUpdate = [][]string{{"a", "u", "q"}, {"d", "x", "NULL"}}
				// Delete also deletes column "i".
				afterDelete = [][]string{{"d", "x", "NULL"}}
				afterDeleteKeys = 3
			} else {
				// The default value of "i" for column "i" is written.
				afterInsert = [][]string{{"a", "z", "q"}, {"c", "x", "i"}}
				if useUpsert {
					// Update is not a noop for column "i". Column "i" gets updated
					// with its default value (#9474).
					afterUpdate = [][]string{{"a", "u", "i"}, {"c", "x", "i"}}
					afterPKUpdate = [][]string{{"a", "u", "i"}, {"d", "x", "i"}}
				} else {
					// Update is a noop for column "i".
					afterUpdate = [][]string{{"a", "u", "q"}, {"c", "x", "i"}}
					afterPKUpdate = [][]string{{"a", "u", "q"}, {"d", "x", "i"}}
				}
				// Delete also deletes column "i".
				afterDelete = [][]string{{"d", "x", "i"}}
				afterDeleteKeys = 4
			}
			// Make column "i" a mutation.
			mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})
			// Insert a row into the table.
			if useUpsert {
				mTest.Exec(t, `UPSERT INTO t.public.test VALUES ('c', 'x')`)
			} else {
				mTest.Exec(t, `INSERT INTO t.public.test VALUES ('c', 'x')`)
			}
			// Make column "i" live so that it is read.
			mTest.makeMutationsActive()
			// Notice that the default value of "i" is only written when the
			// descriptor is in the DELETE_AND_WRITE_ONLY state.
			mTest.CheckQueryResults(t, starQuery, afterInsert)

			// The column backfill uses Put instead of CPut because it depends on
			// an UPDATE of a column in the DELETE_AND_WRITE_ONLY state failing. This test
			// guarantees that.

			// Make column "i" a mutation.
			mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})
			// Updating column "i" for a row fails.
			if useUpsert {
				_, err := sqlDB.Exec(`UPSERT INTO t.public.test VALUES ('a', 'u', 'u')`)
				if !testutils.IsError(err, `INSERT has more expressions than target columns, 3 expressions for 2 targets`) {
					t.Fatal(err)
				}
			} else {
				_, err := sqlDB.Exec(`UPDATE t.public.test SET (v, i) = ('u', 'u') WHERE k = 'a'`)
				if !testutils.IsError(err, `column "i" does not exist`) {
					t.Fatal(err)
				}
			}
			// Make column "i" live so that it is read.
			mTest.makeMutationsActive()
			// The above failed update was a noop.
			mTest.CheckQueryResults(t, starQuery, afterInsert)

			// Make column "i" a mutation.
			mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})
			// Update a row without specifying  mutation column "i".
			if useUpsert {
				mTest.Exec(t, `UPSERT INTO t.public.test VALUES ('a', 'u')`)
			} else {
				mTest.Exec(t, `UPDATE t.public.test SET v = 'u' WHERE k = 'a'`)
			}
			// Make column "i" live so that it is read.
			mTest.makeMutationsActive()
			// The update to column "v" is seen; there is no effect on column "i".
			mTest.CheckQueryResults(t, starQuery, afterUpdate)

			// Make column "i" a mutation.
			mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})
			// Update primary key of row "c" to be "d"
			mTest.Exec(t, `UPDATE t.public.test SET k = 'd' WHERE v = 'x'`)
			// Make column "i" live so that it is read.
			mTest.makeMutationsActive()
			mTest.CheckQueryResults(t, starQuery, afterPKUpdate)

			// Make column "i" a mutation.
			mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})
			// Delete row "a".
			mTest.Exec(t, `DELETE FROM t.public.test WHERE k = 'a'`)
			// Make column "i" live so that it is read.
			mTest.makeMutationsActive()
			// Row "a" is deleted.
			mTest.CheckQueryResults(t, starQuery, afterDelete)
			// Check that there are no hidden KV values for row "a",
			// and column "i" for row "a" was deleted.
			mTest.checkTableSize(afterDeleteKeys)
		}
	}

	// Check that a mutation can only be inserted with an explicit mutation state, and direction.
	tableDesc = mTest.tableDesc
	tableDesc.Mutations = []sqlbase.DescriptorMutation{{}}
	if err := tableDesc.ValidateTable(); !testutils.IsError(err, "mutation in state UNKNOWN, direction NONE, and no column/index descriptor") {
		t.Fatal(err)
	}
	tableDesc.Mutations = []sqlbase.DescriptorMutation{{Descriptor_: &sqlbase.DescriptorMutation_Column{Column: &tableDesc.Columns[len(tableDesc.Columns)-1]}}}
	tableDesc.Columns = tableDesc.Columns[:len(tableDesc.Columns)-1]
	if err := tableDesc.ValidateTable(); !testutils.IsError(err, `mutation in state UNKNOWN, direction NONE, col "i", id 3`) {
		t.Fatal(err)
	}
	tableDesc.Mutations[0].State = sqlbase.DescriptorMutation_DELETE_ONLY
	if err := tableDesc.ValidateTable(); !testutils.IsError(err, `mutation in state DELETE_ONLY, direction NONE, col "i", id 3`) {
		t.Fatal(err)
	}
	tableDesc.Mutations[0].State = sqlbase.DescriptorMutation_UNKNOWN
	tableDesc.Mutations[0].Direction = sqlbase.DescriptorMutation_DROP
	if err := tableDesc.ValidateTable(); !testutils.IsError(err, `mutation in state UNKNOWN, direction DROP, col "i", id 3`) {
		t.Fatal(err)
	}
}

// writeIndexMutation adds index as a mutation and writes the
// descriptor to the DB.
func (mt mutationTest) writeIndexMutation(index string, m sqlbase.DescriptorMutation) {
	tableDesc := mt.tableDesc
	idx, _, err := tableDesc.FindIndexByName(index)
	if err != nil {
		mt.Fatal(err)
	}
	for i := range tableDesc.Indexes {
		if idx.ID == tableDesc.Indexes[i].ID {
			tableDesc.Indexes = append(tableDesc.Indexes[:i], tableDesc.Indexes[i+1:]...)
			break
		}
	}

	m.Descriptor_ = &sqlbase.DescriptorMutation_Index{Index: &idx}
	mt.writeMutation(m)
}

// Test INSERT, UPDATE, UPSERT, and DELETE operations with an index schema
// change.
func TestOperationsWithIndexMutation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect.
	defer sql.TestDisableTableLeases()()
	// Disable external processing of mutations.
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
		AsyncExecNotification: asyncSchemaChangerDisabled,
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k CHAR PRIMARY KEY, v CHAR, INDEX foo (v));
`); err != nil {
		t.Fatal(err)
	}

	// read table descriptor
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	mTest := makeMutationTest(t, kvDB, sqlDB, tableDesc)

	starQuery := `SELECT * FROM t.public.test`
	indexQuery := `SELECT v FROM t.public.test@foo`
	for _, useUpsert := range []bool{true, false} {
		// See the effect of the operations depending on the state.
		for _, state := range []sqlbase.DescriptorMutation_State{sqlbase.DescriptorMutation_DELETE_ONLY,
			sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY} {
			// Init table with some entries.
			if _, err := sqlDB.Exec(`TRUNCATE TABLE t.public.test`); err != nil {
				t.Fatal(err)
			}
			// read table descriptor
			mTest.tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")

			initRows := [][]string{{"a", "z"}, {"b", "y"}}
			for _, row := range initRows {
				if useUpsert {
					mTest.Exec(t, `UPSERT INTO t.public.test VALUES ($1, $2)`, row[0], row[1])
				} else {
					mTest.Exec(t, `INSERT INTO t.public.test VALUES ($1, $2)`, row[0], row[1])
				}
			}
			mTest.CheckQueryResults(t, starQuery, initRows)
			// Index foo is visible.
			mTest.CheckQueryResults(t, indexQuery, [][]string{{"y"}, {"z"}})

			// Index foo is invisible once it's a mutation.
			mTest.writeIndexMutation("foo", sqlbase.DescriptorMutation{State: state})
			if _, err := sqlDB.Query(indexQuery); !testutils.IsError(err, `index "foo" not found`) {
				t.Fatal(err)
			}

			// Insert a new entry.
			if useUpsert {
				mTest.Exec(t, `UPSERT INTO t.public.test VALUES ('c', 'x')`)
			} else {
				mTest.Exec(t, `INSERT INTO t.public.test VALUES ('c', 'x')`)
			}
			mTest.CheckQueryResults(t, starQuery, [][]string{{"a", "z"}, {"b", "y"}, {"c", "x"}})

			// Make index "foo" live so that we can read it.
			mTest.makeMutationsActive()
			if state == sqlbase.DescriptorMutation_DELETE_ONLY {
				// "x" didn't get added to the index.
				mTest.CheckQueryResults(t, indexQuery, [][]string{{"y"}, {"z"}})
			} else {
				// "x" got added to the index.
				mTest.CheckQueryResults(t, indexQuery, [][]string{{"x"}, {"y"}, {"z"}})
			}

			// Make "foo" a mutation.
			mTest.writeIndexMutation("foo", sqlbase.DescriptorMutation{State: state})
			// Update.
			if useUpsert {
				mTest.Exec(t, `UPSERT INTO t.public.test VALUES ('c', 'w')`)
				// Update "v" to its current value "z" in row "a".
				mTest.Exec(t, `UPSERT INTO t.public.test VALUES ('a', 'z')`)
			} else {
				mTest.Exec(t, `UPDATE t.public.test SET v = 'w' WHERE k = 'c'`)
				// Update "v" to its current value "z" in row "a".
				mTest.Exec(t, `UPDATE t.public.test SET v = 'z' WHERE k = 'a'`)
			}
			mTest.CheckQueryResults(t, starQuery, [][]string{{"a", "z"}, {"b", "y"}, {"c", "w"}})

			// Make index "foo" live so that we can read it.
			mTest.makeMutationsActive()
			if state == sqlbase.DescriptorMutation_DELETE_ONLY {
				// updating "x" -> "w" is a noop on the index,
				// updating "z" -> "z" results in "z" being deleted from the index.
				mTest.CheckQueryResults(t, indexQuery, [][]string{{"y"}, {"z"}})
			} else {
				// updating "x" -> "w" results in the index updating from "x" -> "w",
				// updating "z" -> "z" is a noop on the index.
				mTest.CheckQueryResults(t, indexQuery, [][]string{{"w"}, {"y"}, {"z"}})
			}

			// Make "foo" a mutation.
			mTest.writeIndexMutation("foo", sqlbase.DescriptorMutation{State: state})
			// Update the primary key of row "a".
			mTest.Exec(t, `UPDATE t.public.test SET k = 'd' WHERE v = 'z'`)
			mTest.CheckQueryResults(t, starQuery, [][]string{{"b", "y"}, {"c", "w"}, {"d", "z"}})

			// Make index "foo" live so that we can read it.
			mTest.makeMutationsActive()
			// Updating the primary key for a row when we're in delete-only won't
			// create a new index entry, and will delete the old one. Otherwise it'll
			// create a new entry and delete the old one.
			if state == sqlbase.DescriptorMutation_DELETE_ONLY {
				mTest.CheckQueryResults(t, indexQuery, [][]string{{"y"}})
			} else {
				mTest.CheckQueryResults(t, indexQuery, [][]string{{"w"}, {"y"}, {"z"}})
			}

			// Make "foo" a mutation.
			mTest.writeIndexMutation("foo", sqlbase.DescriptorMutation{State: state})
			// Delete row "b".
			mTest.Exec(t, `DELETE FROM t.public.test WHERE k = 'b'`)
			mTest.CheckQueryResults(t, starQuery, [][]string{{"c", "w"}, {"d", "z"}})

			// Make index "foo" live so that we can read it.
			mTest.makeMutationsActive()
			// Deleting row "b" deletes "y" from the index.
			if state == sqlbase.DescriptorMutation_DELETE_ONLY {
				mTest.CheckQueryResults(t, indexQuery, [][]string{})
			} else {
				mTest.CheckQueryResults(t, indexQuery, [][]string{{"w"}, {"z"}})
			}
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

// TestOperationsWithColumnAndIndexMutation tests the INSERT, UPDATE, UPSERT,
// and DELETE operations while an index mutation refers to a column mutation.
func TestOperationsWithColumnAndIndexMutation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// The descriptor changes made must have an immediate effect
	// so disable leases on tables.
	defer sql.TestDisableTableLeases()()
	// Disable external processing of mutations.
	params, _ := tests.CreateTestServerParams()
	params.Knobs.SQLSchemaChanger = &sql.SchemaChangerTestingKnobs{
		AsyncExecNotification: asyncSchemaChangerDisabled,
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	// Create a table with column i and an index on v and i. Fix the column
	// families so the key counts below don't change if the family heuristics
	// are updated.
	// Add an index so that we test adding a column when a table has an index.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k CHAR PRIMARY KEY, v CHAR, i CHAR, INDEX foo (i, v), FAMILY (k), FAMILY (v), FAMILY (i));
CREATE INDEX allidx ON t.public.test (k, v);
`); err != nil {
		t.Fatal(err)
	}

	// read table descriptor
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	mTest := makeMutationTest(t, kvDB, sqlDB, tableDesc)

	starQuery := `SELECT * FROM t.public.test`
	indexQuery := `SELECT i FROM t.public.test@foo`
	for _, useUpsert := range []bool{true, false} {
		// Run the tests for both states for a column and an index.
		for _, state := range []sqlbase.DescriptorMutation_State{
			sqlbase.DescriptorMutation_DELETE_ONLY,
			sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
		} {
			for _, idxState := range []sqlbase.DescriptorMutation_State{
				sqlbase.DescriptorMutation_DELETE_ONLY,
				sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY,
			} {
				// Ignore the impossible column in DELETE_ONLY state while index
				// is in the DELETE_AND_WRITE_ONLY state.
				if state == sqlbase.DescriptorMutation_DELETE_ONLY &&
					idxState == sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY {
					continue
				}
				// Init table to start state.
				if _, err := sqlDB.Exec(`TRUNCATE TABLE t.public.test`); err != nil {
					t.Fatal(err)
				}

				// read table descriptor
				mTest.tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")

				initRows := [][]string{{"a", "z", "q"}, {"b", "y", "r"}}
				for _, row := range initRows {
					if useUpsert {
						mTest.Exec(t, `UPSERT INTO t.public.test VALUES ($1, $2, $3)`, row[0], row[1], row[2])
					} else {
						mTest.Exec(t, `INSERT INTO t.public.test VALUES ($1, $2, $3)`, row[0], row[1], row[2])
					}
				}
				// Check that the table only contains the initRows.
				mTest.CheckQueryResults(t, starQuery, initRows)

				// Add index "foo" as a mutation.
				mTest.writeIndexMutation("foo", sqlbase.DescriptorMutation{State: idxState})
				// Make column "i" a mutation.
				mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})

				// Insert a row into the table.
				if useUpsert {
					mTest.Exec(t, `UPSERT INTO t.public.test VALUES ('c', 'x')`)
				} else {
					mTest.Exec(t, `INSERT INTO t.public.test VALUES ('c', 'x')`)
				}

				// Make column "i" and index "foo" live.
				mTest.makeMutationsActive()
				// column "i" has no entry.
				mTest.CheckQueryResults(t, starQuery, [][]string{{"a", "z", "q"}, {"b", "y", "r"}, {"c", "x", "NULL"}})
				if idxState == sqlbase.DescriptorMutation_DELETE_ONLY {
					// No index entry for row "c"
					mTest.CheckQueryResults(t, indexQuery, [][]string{{"q"}, {"r"}})
				} else {
					// Index entry for row "c"
					mTest.CheckQueryResults(t, indexQuery, [][]string{{"NULL"}, {"q"}, {"r"}})
				}

				// Add index "foo" as a mutation.
				mTest.writeIndexMutation("foo", sqlbase.DescriptorMutation{State: idxState})
				// Make column "i" a mutation.
				mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})

				// Updating column "i" for a row fails.
				if useUpsert {
					_, err := sqlDB.Exec(`UPSERT INTO t.public.test VALUES ('a', 'u', 'u')`)
					if !testutils.IsError(err, `INSERT has more expressions than target columns, 3 expressions for 2 targets`) {
						t.Error(err)
					}
				} else {
					_, err := sqlDB.Exec(`UPDATE t.public.test SET (v, i) = ('u', 'u') WHERE k = 'a'`)
					if !testutils.IsError(err, `column "i" does not exist`) {
						t.Error(err)
					}
				}

				// Using the mutation column as an index expression is disallowed.
				_, err := sqlDB.Exec(`UPDATE t.public.test SET v = 'u' WHERE i < 'a'`)
				if !testutils.IsError(err, `column "i" is being backfilled`) {
					t.Error(err)
				}
				// TODO(vivek): Fix this error to return the same is being
				// backfilled error.
				_, err = sqlDB.Exec(`UPDATE t.public.test SET i = 'u' WHERE k = 'a'`)
				if !testutils.IsError(err, `column "i" does not exist`) {
					t.Error(err)
				}
				_, err = sqlDB.Exec(`DELETE FROM t.public.test WHERE i < 'a'`)
				if !testutils.IsError(err, `column "i" is being backfilled`) {
					t.Error(err)
				}

				// Update a row without specifying  mutation column "i".
				if useUpsert {
					mTest.Exec(t, `UPSERT INTO t.public.test VALUES ('a', 'u')`)
				} else {
					mTest.Exec(t, `UPDATE t.public.test SET v = 'u' WHERE k = 'a'`)
				}
				// Make column "i" and index "foo" live.
				mTest.makeMutationsActive()

				// The update to column "v" is seen; there is no effect on column "i".
				mTest.CheckQueryResults(t, starQuery, [][]string{{"a", "u", "q"}, {"b", "y", "r"}, {"c", "x", "NULL"}})
				if idxState == sqlbase.DescriptorMutation_DELETE_ONLY {
					// Index entry for row "a" is deleted.
					mTest.CheckQueryResults(t, indexQuery, [][]string{{"r"}})
				} else {
					// No change in index "foo"
					mTest.CheckQueryResults(t, indexQuery, [][]string{{"NULL"}, {"q"}, {"r"}})
				}

				// Add index "foo" as a mutation.
				mTest.writeIndexMutation("foo", sqlbase.DescriptorMutation{State: idxState})
				// Make column "i" a mutation.
				mTest.writeColumnMutation("i", sqlbase.DescriptorMutation{State: state})

				// Delete row "b".
				mTest.Exec(t, `DELETE FROM t.public.test WHERE k = 'b'`)
				// Make column "i" and index "foo" live.
				mTest.makeMutationsActive()
				// Row "b" is deleted.
				mTest.CheckQueryResults(t, starQuery, [][]string{{"a", "u", "q"}, {"c", "x", "NULL"}})
				// numKVs is the number of expected key-values. We start with the number
				// of non-NULL values above.
				numKVs := 7
				if idxState == sqlbase.DescriptorMutation_DELETE_ONLY {
					// Index entry for row "b" is deleted.
					mTest.CheckQueryResults(t, indexQuery, [][]string{})
				} else {
					// Index entry for row "b" is deleted.
					mTest.CheckQueryResults(t, indexQuery, [][]string{{"NULL"}, {"q"}})
					// We have two index values.
					numKVs += 2
				}
				// Check that there are no hidden KV values for row "b", and column
				// "i" for row "b" was deleted. Also check that the index values are
				// all accounted for.
				mTest.checkTableSize(numKVs)
			}
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
	defer sql.TestDisableTableLeases()()
	// Disable external processing of mutations.
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SyncFilter:            sql.TestingSchemaChangerCollection.ClearSchemaChangers,
			AsyncExecNotification: asyncSchemaChangerDisabled,
		},
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableMigrations: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (a CHAR PRIMARY KEY, b CHAR, c CHAR, INDEX foo (c));
`); err != nil {
		t.Fatal(err)
	}

	// Read table descriptor
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	mt := makeMutationTest(t, kvDB, sqlDB, tableDesc)

	// Test CREATE INDEX in the presence of mutations.

	// Add index DROP mutation "foo""
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`CREATE INDEX foo ON t.public.test (c)`); !testutils.IsError(err, `index "foo" being dropped, try again later`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()

	// "foo" is being added.
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`CREATE INDEX foo ON t.public.test (c)`); !testutils.IsError(err,
		`duplicate: index "foo" in the middle of being added, not yet public`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// Add column DROP mutation "b"
	mt.writeColumnMutation("b", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`CREATE INDEX bar ON t.public.test (b)`); !testutils.IsError(err, `index "bar" contains unknown column "b"`) {
		t.Fatal(err)
	}
	// Make "b" live.
	mt.makeMutationsActive()
	// "b" is being added.
	mt.writeColumnMutation("b", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	// An index referencing a column mutation that is being added
	// is allowed to be added.
	mt.Exec(t, `CREATE INDEX bar ON t.public.test (b)`)
	// Make "b" live.
	mt.makeMutationsActive()

	// Test DROP INDEX in the presence of mutations.

	// Add index DROP mutation "foo""
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	// Noop.
	mt.Exec(t, `DROP INDEX t.public.test@foo`)
	// Make "foo" live.
	mt.makeMutationsActive()
	// "foo" is being added.
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`DROP INDEX t.public.test@foo`); !testutils.IsError(err, `index "foo" in the middle of being added, try again later`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// Test ALTER TABLE ADD/DROP column in the presence of mutations.

	// Add column DROP mutation "b"
	mt.writeColumnMutation("b", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`ALTER TABLE t.public.test ADD b CHAR`); !testutils.IsError(err, `column "b" being dropped, try again later`) {
		t.Fatal(err)
	}
	// Noop.
	mt.Exec(t, `ALTER TABLE t.public.test DROP b`)
	// Make "b" live.
	mt.makeMutationsActive()
	// "b" is being added.
	mt.writeColumnMutation("b", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`ALTER TABLE t.public.test ADD b CHAR`); !testutils.IsError(err,
		`duplicate: column "b" in the middle of being added, not yet public`) {
		t.Fatal(err)
	}
	if _, err := sqlDB.Exec(`ALTER TABLE t.public.test DROP b`); !testutils.IsError(err, `column "b" in the middle of being added, try again later`) {
		t.Fatal(err)
	}
	// Make "b" live.
	mt.makeMutationsActive()

	// Test ALTER TABLE ADD CONSTRAINT in the presence of mutations.

	// Add index DROP mutation "foo""
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`ALTER TABLE t.public.test ADD CONSTRAINT foo UNIQUE (c)`); !testutils.IsError(err, `index "foo" being dropped, try again later`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// "foo" is being added.
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`ALTER TABLE t.public.test ADD CONSTRAINT foo UNIQUE (c)`); !testutils.IsError(err,
		`duplicate: index "foo" in the middle of being added, not yet public`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()
	// Add column mutation "b"
	mt.writeColumnMutation("b", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`ALTER TABLE t.public.test ADD CONSTRAINT bar UNIQUE (b)`); !testutils.IsError(err, `index "bar" contains unknown column "b"`) {
		t.Fatal(err)
	}
	// Make "b" live.
	mt.makeMutationsActive()
	// "b" is being added.
	mt.writeColumnMutation("b", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	// Noop.
	mt.Exec(t, `ALTER TABLE t.public.test ADD CONSTRAINT bar UNIQUE (b)`)
	// Make "b" live.
	mt.makeMutationsActive()

	// Test DROP CONSTRAINT in the presence of mutations.

	// Add index mutation "foo""
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	// Noop.
	mt.Exec(t, `DROP INDEX t.public.test@foo`)
	// Make "foo" live.
	mt.makeMutationsActive()
	// "foo" is being added.
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`DROP INDEX t.public.test@foo`); !testutils.IsError(err, `index "foo" in the middle of being added, try again later`) {
		t.Fatal(err)
	}
	// Make "foo" live.
	mt.makeMutationsActive()

	// Rename column/index, while index is under mutation.

	// Add index mutation "foo""
	mt.writeIndexMutation("foo", sqlbase.DescriptorMutation{})
	mt.Exec(t, `ALTER INDEX t.public.test@foo RENAME to ufo`)
	mt.Exec(t, `ALTER TABLE t.public.test RENAME COLUMN c TO d`)
	// The mutation in the table descriptor has changed and we would like
	// to update our copy to make it live.
	mt.tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Make "ufo" live.
	mt.makeMutationsActive()
	// The index has been renamed to ufo, and the column to d.
	mt.CheckQueryResults(t,
		"SHOW INDEXES FROM t.public.test",
		[][]string{
			{"test", "primary", "true", "1", "a", "ASC", "false", "false"},
			{"test", "ufo", "false", "1", "d", "ASC", "false", "false"},
			{"test", "ufo", "false", "2", "a", "ASC", "false", "true"},
		},
	)

	// Rename column under mutation works properly.

	// Add column mutation "b".
	mt.writeColumnMutation("b", sqlbase.DescriptorMutation{})
	if _, err := sqlDB.Exec(`ALTER TABLE t.public.test RENAME COLUMN b TO e`); err != nil {
		mt.Fatal(err)
	}

	// The mutation in the table descriptor has changed and we would like
	// to update our copy to make it live.
	mt.tableDesc = sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Make column "e" live.
	mt.makeMutationsActive()
	// Column b changed to d.
	mt.CheckQueryResults(t,
		"SHOW COLUMNS FROM t.public.test",
		[][]string{
			{"a", "STRING", "false", "NULL", "{\"primary\",\"ufo\"}"},
			{"d", "STRING", "true", "NULL", "{\"ufo\"}"},
			{"e", "STRING", "true", "NULL", "{}"},
		},
	)

	// Try to change column defaults while column is under mutation.
	mt.writeColumnMutation("e", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_ADD})
	if _, err := sqlDB.Exec(`ALTER TABLE t.public.test ALTER COLUMN e SET DEFAULT 'a'`); err != nil {
		t.Fatal(err)
	}
	mt.makeMutationsActive()
	mt.writeColumnMutation("e", sqlbase.DescriptorMutation{Direction: sqlbase.DescriptorMutation_DROP})
	if _, err := sqlDB.Exec(`ALTER TABLE t.public.test ALTER COLUMN e SET DEFAULT 'a'`); !testutils.IsError(
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
	params, _ := tests.CreateTestServerParams()
	params.Knobs = base.TestingKnobs{
		SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
			SyncFilter: func(tscc sql.TestingSchemaChangerCollection) {
				tscc.ClearSchemaChangers()
			},
			AsyncExecNotification: asyncSchemaChangerDisabled,
		},
		SQLMigrationManager: &sqlmigrations.MigrationManagerTestingKnobs{
			DisableMigrations: true,
		},
	}
	server, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer server.Stopper().Stop(context.TODO())

	// Create a table with column i and an index on v and i.
	if _, err := sqlDB.Exec(`
CREATE DATABASE t;
CREATE TABLE t.public.test (k CHAR PRIMARY KEY, v CHAR UNIQUE);
`); err != nil {
		t.Fatal(err)
	}

	// Run some schema changes.

	// This single command creates three columns and two indexes sharing the
	// same mutation ID.
	if _, err := sqlDB.Exec(
		`ALTER TABLE t.public.test ADD d INT UNIQUE, ADD e INT UNIQUE, ADD f INT`,
	); err != nil {
		t.Fatal(err)
	}

	// This command creates two mutations sharing the same mutation ID.
	if _, err := sqlDB.Exec(
		`ALTER TABLE t.public.test ADD g INT, ADD CONSTRAINT idx_f UNIQUE (f)`,
	); err != nil {
		t.Fatal(err)
	}

	// This command creates a single mutation.
	if _, err := sqlDB.Exec(`CREATE UNIQUE INDEX idx_g ON t.public.test (g)`); err != nil {
		t.Fatal(err)
	}

	// This command created a drop mutation.
	if _, err := sqlDB.Exec(`ALTER TABLE t.public.test DROP v`); err != nil {
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
		// Drop mutations start off in the DELETE_AND_WRITE_ONLY state.
		// UNIQUE column deletion gets split into two mutation ids.
		{"test_v_key", 4, sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY},
		{"v", 5, sqlbase.DescriptorMutation_DELETE_AND_WRITE_ONLY},
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

// TestAddingFKs checks the behavior of a table in the non-public `ADD` state.
// Being non-public, it should not be visible to clients, and is therefore
// assumed to be empty (e.g. by foreign key checks), since no one could have
// written to it yet.
func TestAddingFKs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(context.TODO())

	if _, err := sqlDB.Exec(`
		CREATE DATABASE t;
		CREATE TABLE t.public.products (id INT PRIMARY KEY);
		INSERT INTO t.public.products VALUES (1), (2);
		CREATE TABLE t.public.orders (id INT PRIMARY KEY, product INT REFERENCES t.public.products, INDEX (product));
	`); err != nil {
		t.Fatal(err)
	}

	// Step the referencing table back to the ADD state.
	ordersDesc := sqlbase.GetTableDescriptor(kvDB, "t", "orders")
	ordersDesc.State = sqlbase.TableDescriptor_ADD
	ordersDesc.Version++
	if err := kvDB.Put(
		context.TODO(),
		sqlbase.MakeDescMetadataKey(ordersDesc.ID),
		sqlbase.WrapDescriptor(ordersDesc),
	); err != nil {
		t.Fatal(err)
	}

	// Generally a referenced table needs to lookup referencing tables to check
	// FKs during delete operations, but referencing tables in the ADD state are
	// given special treatment.
	if _, err := sqlDB.Exec(`DELETE FROM t.public.products`); err != nil {
		t.Fatal(err)
	}

	// Client should not see the orders table.
	if _, err := sqlDB.Exec(
		`SELECT * FROM t.public.orders`,
	); !testutils.IsError(err, "table is being added") {
		t.Fatal(err)
	}
}
