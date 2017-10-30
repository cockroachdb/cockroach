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
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
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

// TestScrubIndexMissingIndexEntry tests that SCRUB TABLE ... INDEX will
// find missing index entries. To test this, a row's underlying
// secondary index k/v is deleted using the KV client. This creates a
// missing index entry error as the row is missing the expected
// secondary index k/v.
func TestScrubIndexMissingIndexEntry(t *testing.T) {
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
	values := []parser.Datum{parser.NewDInt(10), parser.NewDInt(20)}
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	secondaryIndex := &tableDesc.Indexes[0]

	colIDtoRowIndex := make(map[sqlbase.ColumnID]int)
	// FIXME(joey): This assumes the order of columns carries over from
	// table creation for the query results.
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
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH INDEX`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	// Get the results.
	results := []scrubResult(nil)
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
			&result.timestamp, /* timestamp */
			&result.repaired,
			&result.details,
		); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		results = append(results, result)
	}

	if rows.Err() != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}
	if result := results[0]; result.errorType != string(sql.ScrubErrorMissingIndexEntry) {
		t.Fatalf("expected %q error, instead got: %s",
			sql.ScrubErrorMissingIndexEntry, result.errorType)
	} else if result.database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.database)
	} else if result.table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.table)
	} else if result.primaryKey != "(10)" {
		t.Fatalf("expected primaryKey %q, got %q", "(10)", result.primaryKey)
	} else if result.repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.repaired)
	}
}

// TestScrubIndexDanglingIndexReference tests that SCRUB TABLE ... INDEX
// will pick missing index entries. To test this, a row is made with no
// secondary index value, then one is inserted. This creates a
// dangling index error as the corresponding primary k/v is not equal.
func TestScrubIndexDanglingIndexReference(t *testing.T) {
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
	values := []parser.Datum{parser.NewDInt(10), parser.DNull}
	tableDesc := sqlbase.GetTableDescriptor(kvDB, "t", "test")
	secondaryIndexDesc := &tableDesc.Indexes[0]

	colIDtoRowIndex := make(map[sqlbase.ColumnID]int)
	// FIXME(joey): This assumes the order of columns carries over from
	// table creation for the query results.
	colIDtoRowIndex[tableDesc.Columns[0].ID] = 0
	colIDtoRowIndex[tableDesc.Columns[1].ID] = 1

	// Modify the existing datums so it now corresponds to a valid
	// secondary index.
	values[1] = parser.NewDInt(1337)
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
	rows, err := db.Query(`EXPERIMENTAL SCRUB TABLE t.test WITH INDEX`)
	if err != nil {
		t.Fatalf("unexpected error: %s", err)
	} else if rows.Err() != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	defer rows.Close()

	// Get the results.
	results := []scrubResult(nil)
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
			&result.timestamp, /* timestamp */
			&result.repaired,
			&result.details,
		); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
		results = append(results, result)
	}

	if rows.Err() != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d. got %#v", len(results), results)
	}
	if result := results[0]; result.errorType != string(sql.ScrubErrorDanglingIndexReference) {
		t.Fatalf("expected %q error, instead got: %s",
			sql.ScrubErrorDanglingIndexReference, result.errorType)
	} else if result.database != "t" {
		t.Fatalf("expected database %q, got %q", "t", result.database)
	} else if result.table != "test" {
		t.Fatalf("expected table %q, got %q", "test", result.table)
	} else if result.primaryKey != "(10)" {
		t.Fatalf("expected primaryKey %q, got %q", "(10)", result.primaryKey)
	} else if result.repaired {
		t.Fatalf("expected repaired %v, got %v", false, result.repaired)
	}
}
