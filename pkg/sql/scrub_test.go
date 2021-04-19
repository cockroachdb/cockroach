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
	gosql "database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestScrubIndexMissingIndexEntry tests that
// `SCRUB TABLE ... INDEX ALL`` will find missing index entries. To test
// this, a row's underlying secondary index k/v is deleted using the KV
// client. This causes a missing index entry error as the row is missing
// the expected secondary index k/v.
func TestScrubIndexMissingIndexEntry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	r := sqlutils.MakeSQLRunner(db)

	// Create the table and the row entry.
	// We use a table with mixed as a regression case for #38184.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t."tEst" ("K" INT PRIMARY KEY, v INT);
CREATE INDEX secondary ON t."tEst" (v);
INSERT INTO t."tEst" VALUES (10, 20);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Construct datums for our row values (k, v).
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(20)}
	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "tEst")
	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]

	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)

	// Construct the secondary index key that is currently in the
	// database.
	secondaryIndexKey, err := rowenc.EncodeSecondaryIndex(
		keys.SystemSQLCodec, tableDesc, secondaryIndex, colIDtoRowIndex, values, true /* includeEmpty */)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(secondaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d. got %#v", len(secondaryIndexKey), secondaryIndexKey)
	}

	// Delete the entry.
	if err := kvDB.Del(context.Background(), secondaryIndexKey[0].Key); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the index errors we created.
	exp := expectedScrubResult{
		ErrorType:    scrub.MissingIndexEntryError,
		Database:     "t",
		Table:        "tEst",
		PrimaryKey:   "(10)",
		Repaired:     false,
		DetailsRegex: `"v": "20"`,
	}
	runScrub(t, db, `EXPERIMENTAL SCRUB TABLE t."tEst" WITH OPTIONS INDEX ALL`, exp)
	// Run again with AS OF SYSTEM TIME.
	time.Sleep(1 * time.Millisecond)
	runScrub(t, db, `EXPERIMENTAL SCRUB TABLE t."tEst" AS OF SYSTEM TIME '-1ms' WITH OPTIONS INDEX ALL`, exp)

	// Verify that AS OF SYSTEM TIME actually operates in the past.
	ts := r.QueryStr(t, `SELECT cluster_logical_timestamp()`)[0][0]
	r.Exec(t, `DELETE FROM t."tEst"`)
	runScrub(
		t, db, fmt.Sprintf(
			`EXPERIMENTAL SCRUB TABLE t."tEst" AS OF SYSTEM TIME '%s' WITH OPTIONS INDEX ALL`, ts,
		),
		exp,
	)
}

// TestScrubIndexDanglingIndexReference tests that
// `SCRUB TABLE ... INDEX`` will find dangling index references, which
// are index entries that have no corresponding primary k/v. To test
// this an index entry is generated and inserted. This creates a
// dangling index error as the corresponding primary k/v is not equal.
func TestScrubIndexDanglingIndexReference(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
CREATE INDEX secondary ON t.test (v);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]

	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)

	// Construct datums and secondary k/v for our row values (k, v).
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(314)}
	secondaryIndexKey, err := rowenc.EncodeSecondaryIndex(
		keys.SystemSQLCodec, tableDesc, secondaryIndex, colIDtoRowIndex, values, true /* includeEmpty */)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(secondaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d. got %#v", len(secondaryIndexKey), secondaryIndexKey)
	}

	// Put the new secondary k/v into the database.
	if err := kvDB.Put(context.Background(), secondaryIndexKey[0].Key, &secondaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the index errors we created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS INDEX ALL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := sqlutils.GetScrubResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}
	if result := results[0]; result.ErrorType != scrub.DanglingIndexReferenceError {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.DanglingIndexReferenceError, result.ErrorType)
	} else if result.Database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.Database)
	} else if result.Table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.Table)
	} else if result.PrimaryKey != "(10)" {
		t.Fatalf("expected primaryKey %q, got %q", "(10)", result.PrimaryKey)
	} else if result.Repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.Repaired)
	} else if !strings.Contains(result.Details, `"v": "314"`) {
		t.Fatalf("expected error details to contain `%s`, got %s", `"v": "314"`, result.Details)
	}

	// Run SCRUB DATABASE to make sure it also catches the problem.
	rows, err = db.Query(`EXPERIMENTAL SCRUB DATABASE t`)
	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}
	defer rows.Close()
	scrubDatabaseResults, err := sqlutils.GetScrubResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(scrubDatabaseResults) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(scrubDatabaseResults), scrubDatabaseResults)
	} else if !(scrubDatabaseResults[0].ErrorType == results[0].ErrorType &&
		scrubDatabaseResults[0].Database == results[0].Database &&
		scrubDatabaseResults[0].Table == results[0].Table &&
		scrubDatabaseResults[0].Details == results[0].Details) {
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
	defer log.Scope(t).Close(t)
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT, data INT);
CREATE INDEX secondary ON t.test (v) STORING (data);
INSERT INTO t.test VALUES (10, 20, 1337);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]

	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[2].GetID(), 2)

	// Generate the existing secondary index key.
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(20), tree.NewDInt(1337)}
	secondaryIndexKey, err := rowenc.EncodeSecondaryIndex(
		keys.SystemSQLCodec, tableDesc, secondaryIndex, colIDtoRowIndex, values, true /* includeEmpty */)

	if len(secondaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d. got %#v", len(secondaryIndexKey), secondaryIndexKey)
	}

	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Delete the existing secondary k/v.
	if err := kvDB.Del(context.Background(), secondaryIndexKey[0].Key); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Generate a secondary index k/v that has a different value.
	values = []tree.Datum{tree.NewDInt(10), tree.NewDInt(20), tree.NewDInt(314)}
	secondaryIndexKey, err = rowenc.EncodeSecondaryIndex(
		keys.SystemSQLCodec, tableDesc, secondaryIndex, colIDtoRowIndex, values, true /* includeEmpty */)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Put the incorrect secondary k/v.
	if err := kvDB.Put(context.Background(), secondaryIndexKey[0].Key, &secondaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the index errors we created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS INDEX ALL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := sqlutils.GetScrubResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// We will receive both a missing_index_entry and dangling_index_reference.
	if len(results) != 2 {
		t.Fatalf("expected 2 result, got %d. got %#v", len(results), results)
	}

	// Assert the missing index error is correct.
	var missingIndexError *sqlutils.ScrubResult
	for _, result := range results {
		if result.ErrorType == scrub.MissingIndexEntryError {
			missingIndexError = &result
			break
		}
	}
	if result := missingIndexError; result == nil {
		t.Fatalf("expected errors to include %q error, but got errors: %#v",
			scrub.MissingIndexEntryError, results)
	} else if result.Database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.Database)
	} else if result.Table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.Table)
	} else if result.PrimaryKey != "(10)" {
		t.Fatalf("expected primaryKey %q, got %q", "(10)", result.PrimaryKey)
	} else if result.Repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.Repaired)
	} else if !strings.Contains(result.Details, `"data": "1337"`) {
		t.Fatalf("expected error details to contain `%s`, got %s", `"data": "1337"`, result.Details)
	}

	// Assert the dangling index error is correct.
	var danglingIndexResult *sqlutils.ScrubResult
	for _, result := range results {
		if result.ErrorType == scrub.DanglingIndexReferenceError {
			danglingIndexResult = &result
			break
		}
	}
	if result := danglingIndexResult; result == nil {
		t.Fatalf("expected errors to include %q error, but got errors: %#v",
			scrub.DanglingIndexReferenceError, results)
	} else if result.Database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.Database)
	} else if result.Table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.Table)
	} else if result.PrimaryKey != "(10)" {
		t.Fatalf("expected primaryKey %q, got %q", "(10)", result.PrimaryKey)
	} else if result.Repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.Repaired)
	} else if !strings.Contains(result.Details, `"data": "314"`) {
		t.Fatalf("expected error details to contain `%s`, got %s", `"data": "314"`, result.Details)
	}
}

// TestScrubCheckConstraint tests that `SCRUB TABLE ... CONSTRAINT ALL`
// will fail if a check constraint is violated. To test this, a row's
// underlying value is updated using the KV client so the row violates
// the constraint..
func TestScrubCheckConstraint(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT, CHECK (v > 1));
INSERT INTO t.test VALUES (10, 2);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)

	// Create the primary index key.
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(2)}
	primaryIndexKeyPrefix := rowenc.MakeIndexKeyPrefix(
		keys.SystemSQLCodec, tableDesc, tableDesc.GetPrimaryIndexID())
	primaryIndexKey, _, err := rowenc.EncodeIndexKey(
		tableDesc, tableDesc.GetPrimaryIndex(), colIDtoRowIndex, values, primaryIndexKeyPrefix)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Add the family suffix to the key.
	family := tableDesc.GetFamilies()[0]
	primaryIndexKey = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))

	// Generate a k/v that has a different value that violates the
	// constraint.
	values = []tree.Datum{tree.NewDInt(10), tree.NewDInt(0)}
	// Encode the column value.
	valueBuf, err := rowenc.EncodeTableValue(
		[]byte(nil), tableDesc.PublicColumns()[1].GetID(), values[1], []byte(nil))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Construct the tuple for the family value.
	var value roachpb.Value
	value.SetTuple(valueBuf)

	// Overwrite the existing value.
	if err := kvDB.Put(context.Background(), primaryIndexKey, &value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Run SCRUB and find the CHECK violation created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS CONSTRAINT ALL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()
	results, err := sqlutils.GetScrubResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}

	if result := results[0]; result.ErrorType != string(scrub.CheckConstraintViolation) {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.CheckConstraintViolation, result.ErrorType)
	} else if result.Database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.Database)
	} else if result.Table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.Table)
	} else if result.PrimaryKey != "(10)" {
		t.Fatalf("expected primaryKey %q, got %q", "(10)", result.PrimaryKey)
	} else if result.Repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.Repaired)
	} else if !strings.Contains(result.Details,
		`{"constraint_name": "check_v", "row_data": {"k": "10", "v": "0"}}`) {
		t.Fatalf("expected error details to contain `%s`, got %s",
			`{"constraint_name": "check_v", "row_data": {"k": "10", "v": "0"}}`,
			result.Details)
	}
}

// TestScrubFKConstraintFKMissing tests that `SCRUB TABLE ... CONSTRAINT
// ALL` will report an error when a foreign key constraint is violated.
// To test this, the secondary index used for the foreign key lookup is
// modified using the KV client to change the value and cause a
// violation.
func TestScrubFKConstraintFKMissing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	r := sqlutils.MakeSQLRunner(db)

	// Create the table and the row entry.
	r.Exec(t, `
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
	`)

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "child")

	// Construct datums for the child row values (child_id, parent_id).
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(314)}
	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]

	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)

	// Construct the secondary index key entry as it exists in the
	// database.
	secondaryIndexKey, err := rowenc.EncodeSecondaryIndex(
		keys.SystemSQLCodec, tableDesc, secondaryIndex, colIDtoRowIndex, values, true /* includeEmpty */)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(secondaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d. got %#v", len(secondaryIndexKey), secondaryIndexKey)
	}

	// Delete the existing secondary key entry, as we will later replace
	// it.
	if err := kvDB.Del(context.Background(), secondaryIndexKey[0].Key); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Replace the foreign key value.
	values[1] = tree.NewDInt(0)

	// Construct the new secondary index key that will be inserted.
	secondaryIndexKey, err = rowenc.EncodeSecondaryIndex(
		keys.SystemSQLCodec, tableDesc, secondaryIndex, colIDtoRowIndex, values, true /* includeEmpty */)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(secondaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d. got %#v", len(secondaryIndexKey), secondaryIndexKey)
	}

	// Add the new, replacement secondary index entry.
	if err := kvDB.Put(context.Background(), secondaryIndexKey[0].Key, &secondaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the FOREIGN KEY violation created.
	exp := expectedScrubResult{
		ErrorType:    scrub.ForeignKeyConstraintViolation,
		Database:     "t",
		Table:        "child",
		PrimaryKey:   "(10)",
		DetailsRegex: `{"constraint_name": "fk_parent_id_ref_parent", "row_data": {"child_id": "10", "parent_id": "0"}}`,
	}
	runScrub(t, db, `EXPERIMENTAL SCRUB TABLE t.child WITH OPTIONS CONSTRAINT ALL`, exp)
	// Run again with AS OF SYSTEM TIME.
	time.Sleep(1 * time.Millisecond)
	runScrub(t, db, `EXPERIMENTAL SCRUB TABLE t.child AS OF SYSTEM TIME '-1ms' WITH OPTIONS CONSTRAINT ALL`, exp)

	// Verify that AS OF SYSTEM TIME actually operates in the past.
	ts := r.QueryStr(t, `SELECT cluster_logical_timestamp()`)[0][0]
	r.Exec(t, "INSERT INTO t.parent VALUES (0)")
	runScrub(
		t, db, fmt.Sprintf(
			`EXPERIMENTAL SCRUB TABLE t.child AS OF SYSTEM TIME '%s' WITH OPTIONS CONSTRAINT ALL`, ts,
		),
		exp,
	)
}

// TestScrubFKConstraintFKNulls tests that `SCRUB TABLE ... CONSTRAINT ALL` will
// fail if a MATCH FULL foreign key constraint is violated when foreign key
// values are partially null.
// TODO (lucy): This is making use of the fact that SCRUB reports errors for
// unvalidated FKs, even when it's fine for rows to violate the constraint.
// Ideally we would have SCRUB not report errors for those, and use a validated
// constraint in this test with corrupted KVs.
func TestScrubFKConstraintFKNulls(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

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
	INDEX (parent_id, parent_id2)
);
INSERT INTO t.parent VALUES (1337, NULL);
INSERT INTO t.child VALUES (11, 1337, NULL);
ALTER TABLE t.child ADD FOREIGN KEY (parent_id, parent_id2) REFERENCES t.parent (id, id2) MATCH FULL NOT VALID;
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the FOREIGN KEY violation created.
	exp := expectedScrubResult{
		ErrorType:    scrub.ForeignKeyConstraintViolation,
		Database:     "t",
		Table:        "child",
		PrimaryKey:   "(11)",
		DetailsRegex: `{"constraint_name": "fk_parent_id_ref_parent", "row_data": {"child_id": "11", "parent_id": "1337", "parent_id2": "NULL"}}`,
	}
	runScrub(t, db, `EXPERIMENTAL SCRUB TABLE t.child WITH OPTIONS CONSTRAINT ALL`, exp)
	time.Sleep(1 * time.Millisecond)
	runScrub(t, db, `EXPERIMENTAL SCRUB TABLE t.child AS OF SYSTEM TIME '-1ms' WITH OPTIONS CONSTRAINT ALL`, exp)
}

// TestScrubPhysicalNonnullableNullInSingleColumnFamily tests that
// `SCRUB TABLE ... WITH OPTIONS PHYSICAL` will find any rows where a
// value is NULL for a column that is not-nullable and the only column
// in a family. To test this, a row is created that we later overwrite
// the value for. The value that is inserted is the sentinel value as
// the column is the only one in the family.
func TestScrubPhysicalNonnullableNullInSingleColumnFamily(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT NOT NULL);
INSERT INTO t.test VALUES (217, 314);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	// Construct datums for our row values (k, v).
	values := []tree.Datum{tree.NewDInt(217), tree.NewDInt(314)}

	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)

	// Create the primary index key
	primaryIndexKeyPrefix := rowenc.MakeIndexKeyPrefix(
		keys.SystemSQLCodec, tableDesc, tableDesc.GetPrimaryIndexID())
	primaryIndexKey, _, err := rowenc.EncodeIndexKey(
		tableDesc, tableDesc.GetPrimaryIndex(), colIDtoRowIndex, values, primaryIndexKeyPrefix)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Add the family suffix to the key.
	family := tableDesc.GetFamilies()[0]
	primaryIndexKey = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))

	// Create an empty sentinel value.
	var value roachpb.Value
	value.SetTuple([]byte(nil))

	if err := kvDB.Put(context.Background(), primaryIndexKey, &value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the errors we created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS PHYSICAL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()
	results, err := sqlutils.GetScrubResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}

	if result := results[0]; result.ErrorType != string(scrub.UnexpectedNullValueError) {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.UnexpectedNullValueError, result.ErrorType)
	} else if result.Database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.Database)
	} else if result.Table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.Table)
	} else if result.PrimaryKey != "(217)" {
		t.Fatalf("expected primaryKey %q, got %q", "(217)", result.PrimaryKey)
	} else if result.Repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.Repaired)
	} else if !strings.Contains(result.Details, `"k": "217"`) {
		t.Fatalf("expected error details to contain `%s`, got %s", `"k": "217"`, result.Details)
	} else if !strings.Contains(result.Details, `"v": "<unset>"`) {
		t.Fatalf("expected error details to contain `%s`, got %s", `"v": "<unset>"`, result.Details)
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
	defer log.Scope(t).Close(t)
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT NOT NULL, b INT NOT NULL, FAMILY (k), FAMILY (v, b));
INSERT INTO t.test VALUES (217, 314, 1337);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	// Construct datums for our row values (k, v, b).
	values := []tree.Datum{tree.NewDInt(217), tree.NewDInt(314), tree.NewDInt(1337)}

	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[2].GetID(), 2)

	// Create the primary index key
	primaryIndexKeyPrefix := rowenc.MakeIndexKeyPrefix(
		keys.SystemSQLCodec, tableDesc, tableDesc.GetPrimaryIndexID())
	primaryIndexKey, _, err := rowenc.EncodeIndexKey(
		tableDesc, tableDesc.GetPrimaryIndex(), colIDtoRowIndex, values, primaryIndexKeyPrefix)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Add the family suffix to the key, in particular we care about the
	// second column family.
	family := tableDesc.GetFamilies()[1]
	primaryIndexKey = keys.MakeFamilyKey(primaryIndexKey, uint32(family.ID))

	// Encode the second column value.
	valueBuf, err := rowenc.EncodeTableValue(
		[]byte(nil), tableDesc.PublicColumns()[1].GetID(), values[1], []byte(nil))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Construct the tuple for the family that is missing a column value, i.e. it is NULL.
	var value roachpb.Value
	value.SetTuple(valueBuf)

	// Overwrite the existing value.
	if err := kvDB.Put(context.Background(), primaryIndexKey, &value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the errors we created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS PHYSICAL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()
	results, err := sqlutils.GetScrubResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}

	if result := results[0]; result.ErrorType != string(scrub.UnexpectedNullValueError) {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.UnexpectedNullValueError, result.ErrorType)
	} else if result.Database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.Database)
	} else if result.Table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.Table)
	} else if result.PrimaryKey != "(217)" {
		t.Fatalf("expected primaryKey %q, got %q", "(217)", result.PrimaryKey)
	} else if result.Repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.Repaired)
	} else if !strings.Contains(result.Details, `"k": "217"`) {
		t.Fatalf("expected error details to contain `%s`, got %s", `"k": "217"`, result.Details)
	} else if !strings.Contains(result.Details, `"v": "314"`) {
		t.Fatalf("expected error details to contain `%s`, got %s", `"v": "314"`, result.Details)
	} else if !strings.Contains(result.Details, `"b": "<unset>"`) {
		t.Fatalf("expected error details to contain `%s`, got %s", `"b": "<unset>"`, result.Details)
	}
}

// TestScrubPhysicalUnexpectedFamilyID tests that `SCRUB TABLE ... WITH
// OPTIONS PHYSICAL` will find any rows where a primary index as key
// with an invalid family ID. To test this, a table is made with 2
// families and then the first family is dropped. A row is then inserted
// using the KV client which has the ID of the first family.
func TestScrubPhysicalUnexpectedFamilyID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.WithIssue(t, 51797, "currently KV pairs with unexpected family IDs are not noticed by the fetcher")
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

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

	oldTableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	// Drop the first column family.
	if _, err := db.Exec(`ALTER TABLE t.test DROP COLUMN v1`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	// Construct datums for our row values (k, v1).
	values := []tree.Datum{tree.NewDInt(217), tree.NewDInt(314)}

	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)

	// Create the primary index key
	primaryIndexKeyPrefix := rowenc.MakeIndexKeyPrefix(
		keys.SystemSQLCodec, tableDesc, tableDesc.GetPrimaryIndexID())
	primaryIndexKey, _, err := rowenc.EncodeIndexKey(
		tableDesc, tableDesc.GetPrimaryIndex(), colIDtoRowIndex, values, primaryIndexKeyPrefix)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Add the correct family suffix to the key.
	primaryIndexKeyWithFamily := keys.MakeFamilyKey(primaryIndexKey, uint32(tableDesc.GetFamilies()[1].ID))

	// Encode the second column value.
	valueBuf, err := rowenc.EncodeTableValue(
		[]byte(nil), tableDesc.PublicColumns()[1].GetID(), values[1], []byte(nil))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	var value roachpb.Value
	value.SetTuple(valueBuf)

	// Insert the value.
	if err := kvDB.Put(context.Background(), primaryIndexKeyWithFamily, &value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Create a k/v with an incorrect family suffix to the key.
	primaryIndexKeyWithFamily = keys.MakeFamilyKey(primaryIndexKey,
		uint32(oldTableDesc.GetFamilies()[1].ID))

	// Encode the second column value.
	valueBuf, err = rowenc.EncodeTableValue(
		[]byte(nil), tableDesc.PublicColumns()[1].GetID(), values[1], []byte(nil))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	value = roachpb.Value{}
	value.SetTuple(valueBuf)

	// Insert the incorrect family k/v.
	if err := kvDB.Put(context.Background(), primaryIndexKeyWithFamily, &value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the errors we created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS PHYSICAL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()
	results, err := sqlutils.GetScrubResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}

	if result := results[0]; result.ErrorType != string(scrub.UnexpectedNullValueError) {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.UnexpectedNullValueError, result.ErrorType)
	} else if result.Database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.Database)
	} else if result.Table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.Table)
	} else if result.PrimaryKey != "(217)" {
		t.Fatalf("expected primaryKey %q, got %q", "(217)", result.PrimaryKey)
	} else if result.Repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.Repaired)
	} else if !strings.Contains(result.Details, `"k": "217"`) {
		t.Fatalf("expected error details to contain `%s`, got %s", `"k": "217"`, result.Details)
	} else if !strings.Contains(result.Details, `"v": "314"`) {
		t.Fatalf("expected error details to contain `%s`, got %s", `"v": "314"`, result.Details)
	} else if !strings.Contains(result.Details, `"b": "<unset>"`) {
		t.Fatalf("expected error details to contain `%s`, got %s", `"b": "<unset>"`, result.Details)
	}
}

// TestScrubPhysicalIncorrectPrimaryIndexValueColumn tests that
// `SCRUB TABLE ... WITH OPTIONS PHYSICAL` will find any rows where a
// value has an encoded column ID that does not correspond to the table
// descriptor. To test this, a row is inserted using the KV client.
func TestScrubPhysicalIncorrectPrimaryIndexValueColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.WithIssue(t, 51797, "the test is not failing, as it would be expected")
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Create the table and the row entry.
	if _, err := db.Exec(`
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v1 INT, v2 INT);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	tableDesc := catalogkv.TestingGetTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	// Construct datums for our row values (k, v1, v2).
	values := []tree.Datum{tree.NewDInt(217), tree.NewDInt(314), tree.NewDInt(1337)}

	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[2].GetID(), 2)

	// Create the primary index key
	primaryIndexKeyPrefix := rowenc.MakeIndexKeyPrefix(
		keys.SystemSQLCodec, tableDesc, tableDesc.GetPrimaryIndexID())
	primaryIndexKey, _, err := rowenc.EncodeIndexKey(
		tableDesc, tableDesc.GetPrimaryIndex(), colIDtoRowIndex, values, primaryIndexKeyPrefix)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	// Add the default family suffix to the key.
	primaryIndexKey = keys.MakeFamilyKey(primaryIndexKey, uint32(tableDesc.GetFamilies()[0].ID))

	// Encode the second column values. The second column is encoded with
	// a garbage colIDDiff.
	valueBuf, err := rowenc.EncodeTableValue(
		[]byte(nil), tableDesc.PublicColumns()[1].GetID(), values[1], []byte(nil))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	valueBuf, err = rowenc.EncodeTableValue(valueBuf, 1000, values[2], []byte(nil))
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Construct the tuple for the family that is missing a column value, i.e. it is NULL.
	var value roachpb.Value
	value.SetTuple(valueBuf)

	// Overwrite the existing value.
	if err := kvDB.Put(context.Background(), primaryIndexKey, &value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the errors we created.
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS PHYSICAL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := sqlutils.GetScrubResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}

	if result := results[0]; result.ErrorType != string(scrub.UnexpectedNullValueError) {
		t.Fatalf("expected %q error, instead got: %s",
			scrub.UnexpectedNullValueError, result.ErrorType)
	} else if result.Database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.Database)
	} else if result.Table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.Table)
	} else if result.PrimaryKey != "(217)" {
		t.Fatalf("expected primaryKey %q, got %q", "(217)", result.PrimaryKey)
	} else if result.Repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.Repaired)
	} else if !strings.Contains(result.Details, `"k": "217"`) {
		t.Fatalf("expected error details to contain `%s`, got %s", `"k": "217"`, result.Details)
	} else if !strings.Contains(result.Details, `"v": "314"`) {
		t.Fatalf("expected error details to contain `%s`, got %s", `"v": "314"`, result.Details)
	} else if !strings.Contains(result.Details, `"b": "<unset>"`) {
		t.Fatalf("expected error details to contain `%s`, got %s", `"b": "<unset>"`, result.Details)
	}
}

type expectedScrubResult struct {
	ErrorType    string
	Database     string
	Table        string
	PrimaryKey   string
	Repaired     bool
	DetailsRegex string
}

func checkScrubResult(t *testing.T, res sqlutils.ScrubResult, exp expectedScrubResult) {
	t.Helper()

	if res.ErrorType != exp.ErrorType {
		t.Errorf("expected %q error, instead got: %s", exp.ErrorType, res.ErrorType)
	}

	if res.Database != exp.Database {
		t.Errorf("expected database %q, got %q", exp.Database, res.Database)
	}

	if res.Table != exp.Table {
		t.Errorf("expected table %q, got %q", exp.Table, res.Table)
	}

	if res.PrimaryKey != exp.PrimaryKey {
		t.Errorf("expected primary key %q, got %q", exp.PrimaryKey, res.PrimaryKey)
	}
	if res.Repaired != exp.Repaired {
		t.Fatalf("expected repaired %v, got %v", exp.Repaired, res.Repaired)
	}

	if matched, err := regexp.MatchString(exp.DetailsRegex, res.Details); err != nil {
		t.Fatal(err)
	} else if !matched {
		t.Errorf("expected error details to contain `%s`, got `%s`", exp.DetailsRegex, res.Details)
	}
}

// runScrub runs a SCRUB statement and checks that it returns exactly one scrub
// result and that it matches the expected result.
func runScrub(t *testing.T, db *gosql.DB, scrubStmt string, exp expectedScrubResult) {
	t.Helper()

	// Run SCRUB and find the FOREIGN KEY violation created.
	rows, err := db.Query(scrubStmt)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := sqlutils.GetScrubResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}
	checkScrubResult(t, results[0], exp)
}
