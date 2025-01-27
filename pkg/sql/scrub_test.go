// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub/scrubtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// TestScrubIndexMissingIndexEntry tests that
// `SCRUB TABLE ... INDEX ALL“ will find missing index entries. To test
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
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "tEst")
	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]
	if err := removeIndexEntryForDatums(values, kvDB, tableDesc, secondaryIndex); err != nil {
		t.Fatalf("unexpected error: %s", err.Error())
	}

	// Run SCRUB and find the index errors we created.
	exp := []scrubtestutils.ExpectedScrubResult{
		{
			ErrorType:    scrub.MissingIndexEntryError,
			Database:     "t",
			Table:        "tEst",
			PrimaryKey:   "(10)",
			Repaired:     false,
			DetailsRegex: `"v": "20"`,
		},
	}
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE t."tEst" WITH OPTIONS INDEX ALL`, exp)
	// Run again with AS OF SYSTEM TIME.
	time.Sleep(1 * time.Millisecond)
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE t."tEst" AS OF SYSTEM TIME '-1ms' WITH OPTIONS INDEX ALL`, exp)

	// Verify that AS OF SYSTEM TIME actually operates in the past.
	ts := r.QueryStr(t, `SELECT cluster_logical_timestamp()`)[0][0]
	r.Exec(t, `DELETE FROM t."tEst"`)
	scrubtestutils.RunScrub(
		t, db, fmt.Sprintf(
			`EXPERIMENTAL SCRUB TABLE t."tEst" AS OF SYSTEM TIME '%s' WITH OPTIONS INDEX ALL`, ts,
		),
		exp,
	)
}

// TestScrubIndexPartialIndex tests that SCRUB catches various anomalies in the data contained in a
// partial secondary index.
func TestScrubIndexPartialIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	r := sqlutils.MakeSQLRunner(db)

	t.Run("missing index entry", func(t *testing.T) {
		r.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
CREATE INDEX secondary ON t.test (v) WHERE v > 10;
INSERT INTO t.test VALUES (1, 5);
INSERT INTO t.test VALUES (2, 15);
`)
		defer r.Exec(t, "DROP DATABASE t")
		values := []tree.Datum{tree.NewDInt(2), tree.NewDInt(15)}
		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
		secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]
		if err := removeIndexEntryForDatums(values, kvDB, tableDesc, secondaryIndex); err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}
		// Run SCRUB and find the index errors we created.
		exp := []scrubtestutils.ExpectedScrubResult{
			{
				ErrorType:    scrub.MissingIndexEntryError,
				Database:     "t",
				Table:        "test",
				PrimaryKey:   "(2)",
				Repaired:     false,
				DetailsRegex: `"v": "15"`,
			},
		}
		scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS INDEX ALL`, exp)
	})
	t.Run("dangling index entry that matches predicate", func(t *testing.T) {
		r.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
CREATE INDEX secondary ON t.test (v) WHERE v > 10;
INSERT INTO t.test VALUES (1, 5);
INSERT INTO t.test VALUES (2, 15);
`)
		defer r.Exec(t, "DROP DATABASE t")
		values := []tree.Datum{tree.NewDInt(3), tree.NewDInt(25)}
		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
		secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]
		if err := addIndexEntryForDatums(values, kvDB, tableDesc, secondaryIndex); err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}
		// Run SCRUB and find the index errors we created.
		exp := []scrubtestutils.ExpectedScrubResult{
			{
				ErrorType:    scrub.DanglingIndexReferenceError,
				Database:     "t",
				Table:        "test",
				PrimaryKey:   "(3)",
				Repaired:     false,
				DetailsRegex: `"v": "25"`,
			},
		}
		scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS INDEX ALL`, exp)
	})
	t.Run("dangling index entry that does not match predicate", func(t *testing.T) {
		r.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
CREATE INDEX secondary ON t.test (v) WHERE v > 10;
INSERT INTO t.test VALUES (1, 5);
INSERT INTO t.test VALUES (2, 15);
`)
		defer r.Exec(t, "DROP DATABASE t")
		values := []tree.Datum{tree.NewDInt(3), tree.NewDInt(7)}
		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
		secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]
		if err := addIndexEntryForDatums(values, kvDB, tableDesc, secondaryIndex); err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}
		// Run SCRUB and find the index errors we created.
		exp := []scrubtestutils.ExpectedScrubResult{
			{
				ErrorType:    scrub.DanglingIndexReferenceError,
				Database:     "t",
				Table:        "test",
				PrimaryKey:   "(3)",
				Repaired:     false,
				DetailsRegex: `"v": "7"`,
			},
		}
		scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS INDEX ALL`, exp)
	})
	t.Run("index entry that does not match predicate", func(t *testing.T) {
		r.Exec(t, `
CREATE DATABASE t;
CREATE TABLE t.test (k INT PRIMARY KEY, v INT);
CREATE INDEX secondary ON t.test (v) WHERE v > 10;
INSERT INTO t.test VALUES (1, 5);
INSERT INTO t.test VALUES (2, 15);
`)
		defer r.Exec(t, "DROP DATABASE t")
		values := []tree.Datum{tree.NewDInt(1), tree.NewDInt(5)}
		tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
		secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]
		if err := addIndexEntryForDatums(values, kvDB, tableDesc, secondaryIndex); err != nil {
			t.Fatalf("unexpected error: %s", err.Error())
		}
		// Run SCRUB and find the index errors we created.
		exp := []scrubtestutils.ExpectedScrubResult{
			{
				ErrorType:    scrub.DanglingIndexReferenceError,
				Database:     "t",
				Table:        "test",
				PrimaryKey:   "(1)",
				Repaired:     false,
				DetailsRegex: `"v": "5"`,
			},
		}
		scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE t.test WITH OPTIONS INDEX ALL`, exp)
	})

}

func indexEntryForDatums(
	row []tree.Datum, tableDesc catalog.TableDescriptor, index catalog.Index,
) (rowenc.IndexEntry, error) {
	var colIDtoRowIndex catalog.TableColMap
	for i, c := range tableDesc.PublicColumns() {
		colIDtoRowIndex.Set(c.GetID(), i)
	}
	indexEntries, err := rowenc.EncodeSecondaryIndex(
		context.Background(), keys.SystemSQLCodec, tableDesc, index,
		colIDtoRowIndex, row, true, /* includeEmpty */
	)
	if err != nil {
		return rowenc.IndexEntry{}, err
	}

	if len(indexEntries) != 1 {
		return rowenc.IndexEntry{}, errors.Newf("expected 1 index entry, got %d. got %#v", len(indexEntries), indexEntries)
	}
	return indexEntries[0], nil
}

// removeIndexEntryForDatums removes the index entries for the row
// that represents the given datums. It assumes the datums are in the
// order of the public columns of the table. It further assumes that
// the row only produces a single index entry.
func removeIndexEntryForDatums(
	row []tree.Datum, kvDB *kv.DB, tableDesc catalog.TableDescriptor, index catalog.Index,
) error {
	entry, err := indexEntryForDatums(row, tableDesc, index)
	if err != nil {
		return err
	}
	_, err = kvDB.Del(context.Background(), entry.Key)
	return err
}

// addIndexEntryForDatums adds an index entry for the given datums. It assumes the datums are in the
// order of the public columns of the table. It further assumes that the row only produces a single
// index entry.
func addIndexEntryForDatums(
	row []tree.Datum, kvDB *kv.DB, tableDesc catalog.TableDescriptor, index catalog.Index,
) error {
	entry, err := indexEntryForDatums(row, tableDesc, index)
	if err != nil {
		return err
	}
	return kvDB.Put(context.Background(), entry.Key, &entry.Value)
}

// TestScrubIndexDanglingIndexReference tests that
// `SCRUB TABLE ... INDEX“ will find dangling index references, which
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

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]
	// Construct datums and secondary k/v for our row values (k, v).
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(314)}

	// Put the new secondary k/v into the database.
	if err := addIndexEntryForDatums(values, kvDB, tableDesc, secondaryIndex); err != nil {
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

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")
	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]
	// Generate the existing secondary index key.
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(20), tree.NewDInt(1337)}

	// Delete the existing secondary k/v.
	if err := removeIndexEntryForDatums(values, kvDB, tableDesc, secondaryIndex); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Generate a secondary index k/v that has a different value.
	values = []tree.Datum{tree.NewDInt(10), tree.NewDInt(20), tree.NewDInt(314)}

	// Put the incorrect secondary k/v.
	if err := addIndexEntryForDatums(values, kvDB, tableDesc, secondaryIndex); err != nil {
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

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "test")

	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)

	// Create the primary index key.
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(2)}
	primaryIndexKeyPrefix := rowenc.MakeIndexKeyPrefix(
		keys.SystemSQLCodec, tableDesc.GetID(), tableDesc.GetPrimaryIndexID())
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
	valueBuf, err := valueside.Encode(
		[]byte(nil), valueside.MakeColumnIDDelta(0, tableDesc.PublicColumns()[1].GetID()), values[1])
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
	r.Exec(t, `SET autocommit_before_ddl = false`)

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

	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "t", "child")

	// Construct datums for the child row values (child_id, parent_id).
	values := []tree.Datum{tree.NewDInt(10), tree.NewDInt(314)}
	secondaryIndex := tableDesc.PublicNonPrimaryIndexes()[0]

	// Delete the existing secondary key entry, as we will later replace
	// it.
	if err := removeIndexEntryForDatums(values, kvDB, tableDesc, secondaryIndex); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Replace the foreign key value.
	values[1] = tree.NewDInt(0)

	// Add the new, replacement secondary index entry.
	if err := addIndexEntryForDatums(values, kvDB, tableDesc, secondaryIndex); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB and find the FOREIGN KEY violation created.
	exp := []scrubtestutils.ExpectedScrubResult{
		{
			ErrorType:    scrub.ForeignKeyConstraintViolation,
			Database:     "t",
			Table:        "child",
			PrimaryKey:   "(10)",
			DetailsRegex: `{"constraint_name": "child_parent_id_fkey", "row_data": {"child_id": "10", "parent_id": "0"}}`,
		},
	}
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE t.child WITH OPTIONS CONSTRAINT ALL`, exp)
	// Run again with AS OF SYSTEM TIME.
	time.Sleep(1 * time.Millisecond)
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE t.child AS OF SYSTEM TIME '-1ms' WITH OPTIONS CONSTRAINT ALL`, exp)

	// Verify that AS OF SYSTEM TIME actually operates in the past.
	ts := r.QueryStr(t, `SELECT cluster_logical_timestamp()`)[0][0]
	r.Exec(t, "INSERT INTO t.parent VALUES (0)")
	scrubtestutils.RunScrub(
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
	exp := []scrubtestutils.ExpectedScrubResult{
		{
			ErrorType:    scrub.ForeignKeyConstraintViolation,
			Database:     "t",
			Table:        "child",
			PrimaryKey:   "(11)",
			DetailsRegex: `{"constraint_name": "child_parent_id_parent_id2_fkey", "row_data": {"child_id": "11", "parent_id": "1337", "parent_id2": "NULL"}}`,
		},
	}
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE t.child WITH OPTIONS CONSTRAINT ALL`, exp)
	time.Sleep(1 * time.Millisecond)
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE t.child AS OF SYSTEM TIME '-1ms' WITH OPTIONS CONSTRAINT ALL`, exp)
}

// TestScrubUniqueWithoutIndex tests SCRUB on a table that violates a
// UNIQUE WITHOUT INDEX constraint.
func TestScrubUniqueWithoutIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// Create the table and row entries.
	if _, err := db.Exec(`
CREATE DATABASE db;
SET experimental_enable_unique_without_index_constraints = true;
CREATE TABLE db.t (
	id INT PRIMARY KEY,
	id2 INT UNIQUE WITHOUT INDEX
);

INSERT INTO db.t VALUES (1, 2), (2,3);
`); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Overwrite one of the values with a duplicate unique value.
	values := []tree.Datum{tree.NewDInt(1), tree.NewDInt(3)}
	tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, keys.SystemSQLCodec, "db", "t")
	primaryIndex := tableDesc.GetPrimaryIndex()
	var colIDtoRowIndex catalog.TableColMap
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[0].GetID(), 0)
	colIDtoRowIndex.Set(tableDesc.PublicColumns()[1].GetID(), 1)
	primaryIndexKey, err := rowenc.EncodePrimaryIndex(keys.SystemSQLCodec, tableDesc, primaryIndex, colIDtoRowIndex, values, true)
	if err != nil {
		t.Fatalf("unexpected error %s", err)
	}
	if len(primaryIndexKey) != 1 {
		t.Fatalf("expected 1 index entry, got %d", len(primaryIndexKey))
	}
	// Put a duplicate unique value via KV.
	if err := kvDB.Put(context.Background(), primaryIndexKey[0].Key, &primaryIndexKey[0].Value); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// Run SCRUB
	exp := []scrubtestutils.ExpectedScrubResult{
		{
			ErrorType:    scrub.UniqueConstraintViolation,
			Database:     "db",
			Table:        "t",
			PrimaryKey:   "(1)",
			DetailsRegex: `{"constraint_name": "unique_id2", "row_data": {"id": "1", "id2": "3"}`,
		},
		{
			ErrorType:    scrub.UniqueConstraintViolation,
			Database:     "db",
			Table:        "t",
			PrimaryKey:   "(2)",
			DetailsRegex: `{"constraint_name": "unique_id2", "row_data": {"id": "2", "id2": "3"}`,
		},
	}
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE db.t WITH OPTIONS CONSTRAINT ALL`, exp)
	time.Sleep(1 * time.Millisecond)
	scrubtestutils.RunScrub(t, db, `EXPERIMENTAL SCRUB TABLE db.t AS OF SYSTEM TIME '-1ms' WITH OPTIONS CONSTRAINT ALL`, exp)
}
