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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/inspect/inspecttestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/scrub"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// TestInspectIndexMissingIndexEntry tests that
// `INSPECT TABLE ... INDEX ALL“ will find missing index entries. To test
// this, a row's underlying secondary index k/v is deleted using the KV
// client. This causes a missing index entry error as the row is missing
// the expected secondary index k/v.
func TestInspectIndexMissingIndexEntry(t *testing.T) {
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

	// Run INSPECT and find the index errors we created.
	exp := []inspecttestutils.ExpectedInspectResult{
		{
			ErrorType:    scrub.MissingIndexEntryError,
			Database:     "t",
			Table:        "tEst",
			PrimaryKey:   "(10)",
			Repaired:     false,
			DetailsRegex: `"v": "20"`,
		},
	}
	inspecttestutils.RunInspect(t, db, `INSPECT TABLE t."tEst" WITH OPTIONS INDEX ALL`, exp)
	// Run again with AS OF SYSTEM TIME.
	time.Sleep(1 * time.Millisecond)
	inspecttestutils.RunInspect(t, db, `INSPECT TABLE t."tEst" AS OF SYSTEM TIME '-1ms' WITH OPTIONS INDEX ALL`, exp)

	// Verify that AS OF SYSTEM TIME actually operates in the past.
	ts := r.QueryStr(t, `SELECT cluster_logical_timestamp()`)[0][0]
	r.Exec(t, `DELETE FROM t."tEst"`)
	inspecttestutils.RunInspect(
		t, db, fmt.Sprintf(
			`INSPECT TABLE t."tEst" AS OF SYSTEM TIME '%s' WITH OPTIONS INDEX ALL`, ts,
		),
		exp,
	)
}

// TestInspectIndexPartialIndex tests that INSPECT catches various anomalies in the data contained in a
// partial secondary index.
func TestInspectIndexPartialIndex(t *testing.T) {
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
		// Run INSPECT and find the index errors we created.
		exp := []inspecttestutils.ExpectedInspectResult{
			{
				ErrorType:    scrub.MissingIndexEntryError,
				Database:     "t",
				Table:        "test",
				PrimaryKey:   "(2)",
				Repaired:     false,
				DetailsRegex: `"v": "15"`,
			},
		}
		inspecttestutils.RunInspect(t, db, `INSPECT TABLE t.test WITH OPTIONS INDEX ALL`, exp)
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
		// Run INSPECT and find the index errors we created.
		exp := []inspecttestutils.ExpectedInspectResult{
			{
				ErrorType:    scrub.DanglingIndexReferenceError,
				Database:     "t",
				Table:        "test",
				PrimaryKey:   "(3)",
				Repaired:     false,
				DetailsRegex: `"v": "25"`,
			},
		}
		inspecttestutils.RunInspect(t, db, `INSPECT TABLE t.test WITH OPTIONS INDEX ALL`, exp)
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
		// Run INSPECT and find the index errors we created.
		exp := []inspecttestutils.ExpectedInspectResult{
			{
				ErrorType:    scrub.DanglingIndexReferenceError,
				Database:     "t",
				Table:        "test",
				PrimaryKey:   "(3)",
				Repaired:     false,
				DetailsRegex: `"v": "7"`,
			},
		}
		inspecttestutils.RunInspect(t, db, `INSPECT TABLE t.test WITH OPTIONS INDEX ALL`, exp)
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
		// Run INSPECT and find the index errors we created.
		exp := []inspecttestutils.ExpectedInspectResult{
			{
				ErrorType:    scrub.DanglingIndexReferenceError,
				Database:     "t",
				Table:        "test",
				PrimaryKey:   "(1)",
				Repaired:     false,
				DetailsRegex: `"v": "5"`,
			},
		}
		inspecttestutils.RunInspect(t, db, `INSPECT TABLE t.test WITH OPTIONS INDEX ALL`, exp)
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
		colIDtoRowIndex, row, rowenc.EmptyVectorIndexEncodingHelper, true, /* includeEmpty */
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

// TestInspectIndexDanglingIndexReference tests that
// `INSPECT TABLE ... INDEX“ will find dangling index references, which
// are index entries that have no corresponding primary k/v. To test
// this an index entry is generated and inserted. This creates a
// dangling index error as the corresponding primary k/v is not equal.
func TestInspectIndexDanglingIndexReference(t *testing.T) {
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

	// Run INSPECT and find the index errors we created.
	rows, err := db.Query(`INSPECT TABLE t.test WITH OPTIONS INDEX ALL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := sqlutils.GetInspectResultRows(rows)
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

	// Run INSPECT DATABASE to make sure it also catches the problem.
	rows, err = db.Query(`INSPECT DATABASE t`)
	if err != nil {
		t.Fatalf("unexpected error: %+v", err)
	}
	defer rows.Close()
	inspectDatabaseResults, err := sqlutils.GetInspectResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if len(inspectDatabaseResults) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(inspectDatabaseResults), inspectDatabaseResults)
	} else if !(inspectDatabaseResults[0].ErrorType == results[0].ErrorType &&
		inspectDatabaseResults[0].Database == results[0].Database &&
		inspectDatabaseResults[0].Table == results[0].Table &&
		inspectDatabaseResults[0].Details == results[0].Details) {
		t.Fatalf("expected results to be equal, INSPECT TABLE got %v. INSPECT DATABASE got %v",
			results, inspectDatabaseResults)
	}
}

// TestInspectIndexCatchesStoringMismatch tests that
// `INSPECT TABLE ... INDEX ALL` will fail if an index entry only differs
// by its STORING values. To test this, a row's underlying secondary
// index k/v is updated using the KV client to have a different value.
func TestInspectIndexCatchesStoringMismatch(t *testing.T) {
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

	// Run INSPECT and find the index errors we created.
	rows, err := db.Query(`INSPECT TABLE t.test WITH OPTIONS INDEX ALL`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	results, err := sqlutils.GetInspectResultRows(rows)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	// We will receive both a missing_index_entry and dangling_index_reference.
	if len(results) != 2 {
		t.Fatalf("expected 2 result, got %d. got %#v", len(results), results)
	}

	// Assert the missing index error is correct.
	var missingIndexError *sqlutils.InspectResult
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
	var danglingIndexResult *sqlutils.InspectResult
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
