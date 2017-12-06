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

package sql_test

import (
	gosql "database/sql"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type scrubResult struct {
	errorType  string
	database   string
	table      string
	primaryKey string
	timestamp  time.Time
	repaired   bool
	details    string
}

// getResultRows will scan and unmarshal scrubResults from a Rows
// iterator.
func getResultRows(rows *gosql.Rows) (results []scrubResult, err error) {
	var unused *string
	for rows.Next() {
		result := scrubResult{}
		if err := rows.Scan(
			// TODO(joey): In the future, SCRUB will run as a job during execution.
			&unused, /* job_uuid */
			&result.errorType,
			&result.database,
			&result.table,
			&result.primaryKey,
			&result.timestamp,
			&result.repaired,
			&result.details,
		); err != nil {
			return nil, err
		}
		results = append(results, result)
	}

	if rows.Err() != nil {
		return nil, err
	}

	return results, nil
}

// TestScrubIndexMissingIndexEntry tests that
// `SCRUB TABLE ... INDEX ALL`` will find missing index entries. To test
// this, a row's underlying secondary index k/v is deleted using the KV
// client. This causes a missing index entry error as the row is missing
// the expected secondary index k/v.
func TestScrubIndexMissingIndexEntry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
CREATE INDEX secondary ON t.test (v);
INSERT INTO t.test VALUES (10, 20);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Construct datums for our row values (k, v).
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(20)}
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	secondaryIndex := &tableDesc.Indexes[0]

	colIDtoRowIndex := make(map[sqlbase.ColumnID]int)
	colIDtoRowIndex[tableDesc.Columns[0].ID] = 0
	colIDtoRowIndex[tableDesc.Columns[1].ID] = 1

	// Construct the secondary index key that is currently in the
	// database.
	secondaryIndexKey, err := sqlbase.EncodeSecondaryIndex(
		tableDesc, secondaryIndex, colIDtoRowIndex, values)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Delete the entry.
	if err := kvDB.Del(context.TODO(), secondaryIndexKey.Key); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the index errors we created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS INDEX ALL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()
	results, err := getResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}
	if result := results[0]; result.errorType != scrub.MissingIndexEntryError {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.MissingIndexEntryError, result.errorType)
	} else if result.database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.database)
	} else if result.table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.table)
	} else if result.primaryKey != "(10)" {
		t.Fatalf("expected primaryKey %q, got %q", "(10)", result.primaryKey)
	} else if result.repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.repaired)
	} else if !strings.Contains(result.details, `"v":"20"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"v":"20"`, result.details)
	}
}

// TestScrubIndexDanglingIndexReference tests that
// `SCRUB TABLE ... INDEX`` will find dangling index references, which
// are index entries that have no corresponding primary k/v. To test
// this an index entry is generated and inserted. This creates a
// dangling index error as the corresponding primary k/v is not equal.
func TestScrubIndexDanglingIndexReference(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
CREATE INDEX secondary ON t.test (v);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	secondaryIndexDesc := &tableDesc.Indexes[0]

	colIDtoRowIndex := make(map[sqlbase.ColumnID]int)
	colIDtoRowIndex[tableDesc.Columns[0].ID] = 0
	colIDtoRowIndex[tableDesc.Columns[1].ID] = 1

	// Construct datums and secondary k/v for our row values (k, v).
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(314)}
	secondaryIndex, err := sqlbase.EncodeSecondaryIndex(
		tableDesc, secondaryIndexDesc, colIDtoRowIndex, values)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Put the new secondary k/v into the database.
	if err := kvDB.Put(context.TODO(), secondaryIndex.Key, &secondaryIndex.Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the index errors we created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS INDEX ALL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := getResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}
	if result := results[0]; result.errorType != scrub.DanglingIndexReferenceError {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.DanglingIndexReferenceError, result.errorType)
	} else if result.database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.database)
	} else if result.table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.table)
	} else if result.primaryKey != "(10)" {
		t.Fatalf("expected primaryKey %q, got %q", "(10)", result.primaryKey)
	} else if result.repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.repaired)
	} else if !strings.Contains(result.details, `"v":"314"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"v":"314"`, result.details)
	}

	// Run SCRUB DATABASE to make sure it also catches the problem.
	rows, err = db.Query(`EXPERIMENTAL SCRUB DATABASE t`)
	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}
	defer rows.Close()
	scrubDatabaseResults, err := getResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(scrubDatabaseResults) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(scrubDatabaseResults), scrubDatabaseResults)
	} else if !(scrubDatabaseResults[0].errorType == results[0].errorType &&
		scrubDatabaseResults[0].database == results[0].database &&
		scrubDatabaseResults[0].table == results[0].table &&
		scrubDatabaseResults[0].details == results[0].details) {
		t.Fatalf("expected results to be equal, SCRUB TABLE got %v. SCRUB DATABASE got %v",
			results, scrubDatabaseResults)
	}
}

// TestScrubIndexCatchesStoringMismatch tests that
// `SCRUB TABLE ... INDEX ALL` will fail if an index entry only differs
// by its STORING values. To test this, a row's underlying secondary
// index k/v is updated using the KV client to have a different value.
func TestScrubIndexCatchesStoringMismatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT, data INT);
CREATE INDEX secondary ON t.test (v) STORING (data);
INSERT INTO t.test VALUES (10, 20, 1337);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	secondaryIndexDesc := &tableDesc.Indexes[0]

	colIDtoRowIndex := make(map[sqlbase.ColumnID]int)
	colIDtoRowIndex[tableDesc.Columns[0].ID] = 0
	colIDtoRowIndex[tableDesc.Columns[1].ID] = 1
	colIDtoRowIndex[tableDesc.Columns[2].ID] = 2

	// Generate the existing secondary index key.
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(20), tree.NewDInt(1337)}
	secondaryIndex, err := sqlbase.EncodeSecondaryIndex(
		tableDesc, secondaryIndexDesc, colIDtoRowIndex, values)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Delete the existing secondary k/v.
	if err := kvDB.Del(context.TODO(), secondaryIndex.Key); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Generate a secondary index k/v that has a different value.
	values = []tree.Datum{tree.NewDInt(10), tree.NewDInt(20), tree.NewDInt(314)}
	secondaryIndex, err = sqlbase.EncodeSecondaryIndex(
		tableDesc, secondaryIndexDesc, colIDtoRowIndex, values)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Put the incorrect secondary k/v.
	if err := kvDB.Put(context.TODO(), secondaryIndex.Key, &secondaryIndex.Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the index errors we created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS INDEX ALL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := getResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// We will receive both a missing_index_entry and dangling_index_reference.
	if len(results) != 2 {
		t.Fatalf("expected 2 result, got %d. got %#v", len(results), results)
	}

	// Assert the missing index error is correct.
	var missingIndexError *scrubResult
	for _, result := range results {
		if result.errorType == scrub.MissingIndexEntryError {
			missingIndexError = &result
			break
		}
	}
	if result := missingIndexError; result == nil {
		t.Fatalf("expected errors to include %q error, but got errors: %#v",
			scrub.MissingIndexEntryError, results)
	} else if result.database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.database)
	} else if result.table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.table)
	} else if result.primaryKey != "(10)" {
		t.Fatalf("expected primaryKey %q, got %q", "(10)", result.primaryKey)
	} else if result.repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.repaired)
	} else if !strings.Contains(result.details, `"data":"1337"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"data":"1337"`, result.details)
	}

	// Assert the dangling index error is correct.
	var danglingIndexResult *scrubResult
	for _, result := range results {
		if result.errorType == scrub.DanglingIndexReferenceError {
			danglingIndexResult = &result
			break
		}
	}
	if result := danglingIndexResult; result == nil {
		t.Fatalf("expected errors to include %q error, but got errors: %#v",
			scrub.DanglingIndexReferenceError, results)
	} else if result.database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.database)
	} else if result.table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.table)
	} else if result.primaryKey != "(10)" {
		t.Fatalf("expected primaryKey %q, got %q", "(10)", result.primaryKey)
	} else if result.repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.repaired)
	} else if !strings.Contains(result.details, `"data":"314"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"data":"314"`, result.details)
	}
}

// TestScrubCheckConstraint tests that `SCRUB TABLE ... CONSTRAINT ALL`
// will fail if a check constraint is violated. To test this, a row's
// underlying value is updated using the KV client so the row violates
// the constraint..
func TestScrubCheckConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT, CHECK (v > 1));
INSERT INTO t.test VALUES (10, 2);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	colIDtoRowIndex := make(map[sqlbase.ColumnID]int)
	colIDtoRowIndex[tableDesc.Columns[0].ID] = 0
	colIDtoRowIndex[tableDesc.Columns[1].ID] = 1

	// Create the primary index key.
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(2)}
	primaryIndexKeyPrefix := sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)
	primaryIndexKey, _, err := sqlbase.EncodeIndexKey(
		tableDesc, &tableDesc.PrimaryIndex, colIDtoRowIndex, values, primaryIndexKeyPrefix)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Add the family suffix to the key.
	family := tableDesc.Families[0]
	primaryIndexKey = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))

	// Generate a k/v that has a different value that violates the
	// constraint.
	values = []tree.Datum{tree.NewDInt(10), tree.NewDInt(0)}
	// Encode the column value.
	valueBuf, err := sqlbase.EncodeTableValue(
		[]byte(nil), tableDesc.Columns[1].ID, values[1], []byte(nil))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Construct the tuple for the family value.
	var value roachpb.Value
	value.SetTuple(valueBuf)

	// Overwrite the existing value.
	if err := kvDB.Put(context.TODO(), primaryIndexKey, &value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Run SCRUB and find the CHECK violation created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS CONSTRAINT ALL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()
	results, err := getResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}

	if result := results[0]; result.errorType != string(scrub.CheckConstraintViolation) {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.CheckConstraintViolation, result.errorType)
	} else if result.database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.database)
	} else if result.table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.table)
	} else if result.primaryKey != "(10)" {
		t.Fatalf("expected primaryKey %q, got %q", "(10)", result.primaryKey)
	} else if result.repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.repaired)
	} else if !strings.Contains(result.details,
		`{"constraint_name":"check_v","row_data":{"k":"10","v":"0"}}`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s",
			`{"constraint_name":"check_v","row_data":{"k":"10","v":"0"}}`,
			result.details)
	}
}

// TestScrubFKConstraintFKIsNull tests that `SCRUB TABLE ... CONSTRAINT
// ALL` will report an error when a foreign key constraint is violated.
// To test this, the secondary index used for the foreign key lookup is
// modified using the KV client to change the value and cause a
// violation.
func TestScrubFKConstraintFKMissing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.parent (
	id INT PRIMARY KEY
);
CREATE TABLE t.child (
	child_id INT PRIMARY KEY,
	parent_id INT,
	INDEX (parent_id),
	FOREIGN KEY (parent_id) REFERENCES t.parent (id)
);
INSERT INTO t.parent VALUES (314);
INSERT INTO t.child VALUES (10, 314);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "child")

	// Construct datums for the child row values (child_id, parent_id).
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(314)}
	secondaryIndex := &tableDesc.Indexes[0]

	colIDtoRowIndex := make(map[sqlbase.ColumnID]int)
	colIDtoRowIndex[tableDesc.Columns[0].ID] = 0
	colIDtoRowIndex[tableDesc.Columns[1].ID] = 1

	// Construct the secondary index key entry as it exists in the
	// database.
	secondaryIndexKey, err := sqlbase.EncodeSecondaryIndex(
		tableDesc, secondaryIndex, colIDtoRowIndex, values)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Delete the existing secondary key entry, as we will later replace
	// it.
	if err := kvDB.Del(context.TODO(), secondaryIndexKey.Key); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Replace the foreign key value.
	values[1] = tree.NewDInt(0)

	// Construct the new secondary index key that will be inserted.
	secondaryIndexKey, err = sqlbase.EncodeSecondaryIndex(
		tableDesc, secondaryIndex, colIDtoRowIndex, values)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Add the new, replacement secondary index entry.
	if err := kvDB.Put(context.TODO(), secondaryIndexKey.Key, &secondaryIndexKey.Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the FOREIGN KEY violation created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.child WITH OPTIONS CONSTRAINT ALL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := getResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}

	if result := results[0]; result.errorType != string(scrub.ForeignKeyConstraintViolation) {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.ForeignKeyConstraintViolation, result.errorType)
	} else if result.database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.database)
	} else if result.table != "child" {
		t.Fatalf("expected table %q, got %q", "child", result.table)
	} else if result.primaryKey != "(10)" {
		t.Fatalf("expected primaryKey %q, got %q", "(10)", result.primaryKey)
	} else if result.repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.repaired)
	} else if !strings.Contains(result.details,
		`{"constraint_name":"fk_parent_id_ref_parent","row_data":{"child_id":"10","parent_id":"0"}}`) {
		t.Fatalf("expected erorr details to contain %s, got %s",
			`{"constraint_name":"fk_parent_id_ref_parent","row_data":{"child_id":"10","parent_id":"0"}}`,
			result.details)
	}
}

// TestScrubFKConstraintFKIsNullAndMissing tests that
// `SCRUB TABLE ... CONSTRAINT ALL` will fail if a foreign key
// constraint is violated when there is no referenced foreign key row
// found and the foreign key values are partially null. To test this, a
// row's underlying value is modified using the KV client.
func TestScrubFKConstraintFKIsNullAndMissing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.parent (
	id INT PRIMARY KEY,
	id2 INT,
	UNIQUE INDEX (id, id2)
);
CREATE TABLE t.child (
	child_id INT PRIMARY KEY,
	parent_id INT,
	parent_id2 INT,
	INDEX (parent_id, parent_id2),
	FOREIGN KEY (parent_id, parent_id2) REFERENCES t.parent (id, id2)
);
INSERT INTO t.parent VALUES (1337, 300);
INSERT INTO t.child VALUES (11, 1337, 300);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "child")

	// Construct datums for our row values (child_id, parent_id, parent_id2).
	values := []tree.Datum{tree.NewDInt(11), tree.NewDInt(1337), tree.NewDInt(300)}
	secondaryIndex := &tableDesc.Indexes[0]

	colIDtoRowIndex := make(map[sqlbase.ColumnID]int)
	colIDtoRowIndex[tableDesc.Columns[0].ID] = 0
	colIDtoRowIndex[tableDesc.Columns[1].ID] = 1
	colIDtoRowIndex[tableDesc.Columns[2].ID] = 2

	// Create the secondary index key that is currently in the database.
	secondaryIndexEntry, err := sqlbase.EncodeSecondaryIndex(
		tableDesc, secondaryIndex, colIDtoRowIndex, values)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Delete the entry.
	if err := kvDB.Del(context.TODO(), secondaryIndexEntry.Key); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Replace parent_id2 with NULL.
	values[2] = tree.DNull
	secondaryIndexEntry, err = sqlbase.EncodeSecondaryIndex(
		tableDesc, secondaryIndex, colIDtoRowIndex, values)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Add the entry, essentially replacing the index entry with (11, 1337, NULL).
	// This will be a foreign key violation.
	if err := kvDB.Put(context.TODO(), secondaryIndexEntry.Key,
		&secondaryIndexEntry.Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the FOREIGN KEY violation created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.child WITH OPTIONS CONSTRAINT ALL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := getResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}

	if result := results[0]; result.errorType != string(scrub.ForeignKeyConstraintViolation) {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.ForeignKeyConstraintViolation, result.errorType)
	} else if result.database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.database)
	} else if result.table != "child" {
		t.Fatalf("expected table %q, got %q", "child", result.table)
	} else if result.primaryKey != "(11)" {
		t.Fatalf("expected primaryKey %q, got %q", "(11)", result.primaryKey)
	} else if result.repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.repaired)
	} else if !strings.Contains(result.details,
		`{"constraint_name":"fk_parent_id_ref_parent","row_data":{"child_id":"11","parent_id":"1337","parent_id2":"NULL"}}`) {
		t.Fatalf("expected erorr details to contain %s, got %s",
			`{"constraint_name":"fk_parent_id_ref_parent","row_data":{"child_id":"11","parent_id":"1337","parent_id2":"NULL"}}`,
			result.details)
	}
}

// TestScrubPhysicalNonnullableNullInSingleColumnFamily tests that
// `SCRUB TABLE ... WITH OPTIONS PHYSICAL` will find any rows where a
// value is NULL for a column that is not-nullable and the only column
// in a family. To test this, a row is created that we later overwrite
// the value for. The value that is inserted is the sentinel value as
// the column is the only one in the family.
func TestScrubPhysicalNonnullableNullInSingleColumnFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT NOT NULL);
INSERT INTO t.test VALUES (217, 314);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Construct datums for our row values (k, v).
	values := []tree.Datum{tree.NewDInt(217), tree.NewDInt(314)}

	colIDtoRowIndex := make(map[sqlbase.ColumnID]int)
	colIDtoRowIndex[tableDesc.Columns[0].ID] = 0
	colIDtoRowIndex[tableDesc.Columns[1].ID] = 1

	// Create the primary index key
	primaryIndexKeyPrefix := sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)
	primaryIndexKey, _, err := sqlbase.EncodeIndexKey(
		tableDesc, &tableDesc.PrimaryIndex, colIDtoRowIndex, values, primaryIndexKeyPrefix)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Add the family suffix to the key.
	family := tableDesc.Families[0]
	primaryIndexKey = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))

	// Create an empty sentinel value.
	var value roachpb.Value
	value.SetTuple([]byte(nil))

	if err := kvDB.Put(context.TODO(), primaryIndexKey, &value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the errors we created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS PHYSICAL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := getResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}

	if result := results[0]; result.errorType != string(scrub.UnexpectedNullValueError) {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.UnexpectedNullValueError, result.errorType)
	} else if result.database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.database)
	} else if result.table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.table)
	} else if result.primaryKey != "(217)" {
		t.Fatalf("expected primaryKey %q, got %q", "(217)", result.primaryKey)
	} else if result.repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.repaired)
	} else if !strings.Contains(result.details, `"k":"217"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"k":"217"`, result.details)
	} else if !strings.Contains(result.details, `"v":"<unset>"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"v":"<unset>"`, result.details)
	}
}

// TestScrubPhysicalNonnullableNullInMulticolumnFamily tests that
// `SCRUB TABLE ... WITH OPTIONS PHYSICAL` will find any rows where a
// value is NULL for a column that is not-nullable and is not the only
// column in a family. To test this, a row is created that we later
// overwrite the value for. The value that is inserted is missing one of
// the columns that belongs in the family.
func TestScrubPhysicalNonnullableNullInMulticolumnFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT NOT NULL, b INT NOT NULL, FAMILY (k), FAMILY(v, b));
INSERT INTO t.test VALUES (217, 314, 1337);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Construct datums for our row values (k, v, b).
	values := []tree.Datum{tree.NewDInt(217), tree.NewDInt(314), tree.NewDInt(1337)}

	colIDtoRowIndex := make(map[sqlbase.ColumnID]int)
	colIDtoRowIndex[tableDesc.Columns[0].ID] = 0
	colIDtoRowIndex[tableDesc.Columns[1].ID] = 1
	colIDtoRowIndex[tableDesc.Columns[2].ID] = 2

	// Create the primary index key
	primaryIndexKeyPrefix := sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)
	primaryIndexKey, _, err := sqlbase.EncodeIndexKey(
		tableDesc, &tableDesc.PrimaryIndex, colIDtoRowIndex, values, primaryIndexKeyPrefix)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Add the family suffix to the key, in particular we care about the
	// second column family.
	family := tableDesc.Families[1]
	primaryIndexKey = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))

	// Encode the second column value.
	valueBuf, err := sqlbase.EncodeTableValue(
		[]byte(nil), tableDesc.Columns[1].ID, values[1], []byte(nil))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Construct the tuple for the family that is missing a column value, i.e. it is NULL.
	var value roachpb.Value
	value.SetTuple(valueBuf)

	// Overwrite the existing value.
	if err := kvDB.Put(context.TODO(), primaryIndexKey, &value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the errors we created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS PHYSICAL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := getResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}

	if result := results[0]; result.errorType != string(scrub.UnexpectedNullValueError) {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.UnexpectedNullValueError, result.errorType)
	} else if result.database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.database)
	} else if result.table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.table)
	} else if result.primaryKey != "(217)" {
		t.Fatalf("expected primaryKey %q, got %q", "(217)", result.primaryKey)
	} else if result.repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.repaired)
	} else if !strings.Contains(result.details, `"k":"217"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"k":"217"`, result.details)
	} else if !strings.Contains(result.details, `"v":"314"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"v":"314"`, result.details)
	} else if !strings.Contains(result.details, `"b":"<unset>"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"b":"<unset>"`, result.details)
	}
}

// TestScrubPhysicalUnexpectedFamilyID tests that `SCRUB TABLE ... WITH
// OPTIONS PHYSICAL` will find any rows where a primary index as key
// with an invalid family ID. To test this, a table is made with 2
// families and then the first family is dropped. A row is then inserted
// using the KV client which has the ID of the first family.
func TestScrubPhysicalUnexpectedFamilyID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("currently KV pairs with unexpected family IDs are not noticed by the fetcher")
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (
	k INT PRIMARY KEY,
	v1 INT NOT NULL,
	v2 INT NOT NULL,
	FAMILY first (v1),
	FAMILY second (v2)
);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	oldTableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Drop the first column family.
	if _, err := db.Exec(`ALTER TABLE t.test DROP COLUMN v1`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Construct datums for our row values (k, v1).
	values := []tree.Datum{tree.NewDInt(217), tree.NewDInt(314)}

	colIDtoRowIndex := make(map[sqlbase.ColumnID]int)
	colIDtoRowIndex[tableDesc.Columns[0].ID] = 0
	colIDtoRowIndex[tableDesc.Columns[1].ID] = 1

	// Create the primary index key
	primaryIndexKeyPrefix := sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)
	primaryIndexKey, _, err := sqlbase.EncodeIndexKey(
		tableDesc, &tableDesc.PrimaryIndex, colIDtoRowIndex, values, primaryIndexKeyPrefix)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Add the correct family suffix to the key.
	primaryIndexKeyWithFamily := keys.MakeFamilyKey(primaryIndexKey, uint32(tableDesc.Families[1].ID))

	// Encode the second column value.
	valueBuf, err := sqlbase.EncodeTableValue(
		[]byte(nil), tableDesc.Columns[1].ID, values[1], []byte(nil))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	var value roachpb.Value
	value.SetTuple(valueBuf)

	// Insert the value.
	if err := kvDB.Put(context.TODO(), primaryIndexKeyWithFamily, &value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Create a k/v with an incorrect family suffix to the key.
	primaryIndexKeyWithFamily = keys.MakeFamilyKey(primaryIndexKey,
		uint32(oldTableDesc.Families[1].ID))

	// Encode the second column value.
	valueBuf, err = sqlbase.EncodeTableValue(
		[]byte(nil), tableDesc.Columns[1].ID, values[1], []byte(nil))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	value = roachpb.Value{}
	value.SetTuple(valueBuf)

	// Insert the incorrect family k/v.
	if err := kvDB.Put(context.TODO(), primaryIndexKeyWithFamily, &value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the errors we created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS PHYSICAL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := getResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}

	if result := results[0]; result.errorType != string(scrub.UnexpectedNullValueError) {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.UnexpectedNullValueError, result.errorType)
	} else if result.database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.database)
	} else if result.table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.table)
	} else if result.primaryKey != "(217)" {
		t.Fatalf("expected primaryKey %q, got %q", "(217)", result.primaryKey)
	} else if result.repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.repaired)
	} else if !strings.Contains(result.details, `"k":"217"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"k":"217"`, result.details)
	} else if !strings.Contains(result.details, `"v":"314"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"v":"314"`, result.details)
	} else if !strings.Contains(result.details, `"b":"<unset>"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"b":"<unset>"`, result.details)
	}
}

// TestScrubPhysicalIncorrectPrimaryIndexValueColumn tests that
// `SCRUB TABLE ... WITH OPTIONS PHYSICAL` will find any rows where a
// value has an encoded column ID that does not correspond to the table
// descriptor. To test this, a row is inserted using the KV client.
func TestScrubPhysicalIncorrectPrimaryIndexValueColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("the test is not failing, as it would be expected")
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v1 INT, v2 INT);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")

	// Construct datums for our row values (k, v1, v2).
	values := []tree.Datum{tree.NewDInt(217), tree.NewDInt(314), tree.NewDInt(1337)}

	colIDtoRowIndex := make(map[sqlbase.ColumnID]int)
	colIDtoRowIndex[tableDesc.Columns[0].ID] = 0
	colIDtoRowIndex[tableDesc.Columns[1].ID] = 1
	colIDtoRowIndex[tableDesc.Columns[2].ID] = 2

	// Create the primary index key
	primaryIndexKeyPrefix := sqlbase.MakeIndexKeyPrefix(tableDesc, tableDesc.PrimaryIndex.ID)
	primaryIndexKey, _, err := sqlbase.EncodeIndexKey(
		tableDesc, &tableDesc.PrimaryIndex, colIDtoRowIndex, values, primaryIndexKeyPrefix)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Add the default family suffix to the key.
	primaryIndexKey = keys.MakeFamilyKey(primaryIndexKey, uint32(tableDesc.Families[0].ID))

	// Encode the second column values. The second column is encoded with
	// a garbage colIDDiff.
	valueBuf, err := sqlbase.EncodeTableValue(
		[]byte(nil), tableDesc.Columns[1].ID, values[1], []byte(nil))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	valueBuf, err = sqlbase.EncodeTableValue(valueBuf, 1000, values[2], []byte(nil))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Construct the tuple for the family that is missing a column value, i.e. it is NULL.
	var value roachpb.Value
	value.SetTuple(valueBuf)

	// Overwrite the existing value.
	if err := kvDB.Put(context.TODO(), primaryIndexKey, &value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the errors we created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS PHYSICAL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := getResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}

	if result := results[0]; result.errorType != string(scrub.UnexpectedNullValueError) {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.UnexpectedNullValueError, result.errorType)
	} else if result.database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.database)
	} else if result.table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.table)
	} else if result.primaryKey != "(217)" {
		t.Fatalf("expected primaryKey %q, got %q", "(217)", result.primaryKey)
	} else if result.repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.repaired)
	} else if !strings.Contains(result.details, `"k":"217"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"k":"217"`, result.details)
	} else if !strings.Contains(result.details, `"v":"314"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"v":"314"`, result.details)
	} else if !strings.Contains(result.details, `"b":"<unset>"`) {
		t.Fatalf("expected erorr details to contain `%s`, got %s", `"b":"<unset>"`, result.details)
	}
}
