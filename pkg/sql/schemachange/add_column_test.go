// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachange

import (
	"context"
	gosql "database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

// TestHalfAddedColumn offers a sanity check to ensure that
// data can exist which do not have any referenced columns in
// the table descriptor. This can occur if a BACKUP was taken
// during a column deletion and then RESTOREd. The RESTORE will
// clean up the descriptor to remove mutations and the relevant
// columns, but the data will not be backfilled or cleaned up.
func TestHalfAddedColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	type testCase struct {
		schema   [][]string
		families map[string][]string
	}

	testCases := []testCase{
		{
			schema: [][]string{
				{"a", "INT"},
				{"b", "INT"},
				{"c", "STRING"},
			},
			families: map[string][]string{
				"f0": {"a", "c"},
				"f1": {"b"},
			},
		},
		{
			schema: [][]string{
				{"a", "INT"},
				{"b", "INT"},
				{"c", "STRING"},
				{"d", "INT"},
			},
			families: map[string][]string{
				"f0": {"a", "c"},
				"f1": {"b"},
				"f2": {"d"},
			},
		},
	}

	tableName := "foo"
	for _, testCase := range testCases {
		schema := testCase.schema
		families := testCase.families

		runTestCase(ctx, t, tableName, schema, families)
	}
}

func runTestCase(
	ctx context.Context,
	t *testing.T,
	tableName string,
	schema [][]string,
	families map[string][]string,
) {
	// Set up the server.
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()
	path := filepath.Join(dir, "testserver")
	args := base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{{InMemory: false, Path: path}},
	}
	tc, sqlDB, kvDB := serverutils.StartServer(t, args)
	defer func() {
		// We modify the value of `tc` below to start up a second cluster, so in
		// contrast to other tests, run this `defer Stop` in an anonymous func.
		tc.Stopper().Stop(ctx)
	}()

	fullTableName := fmt.Sprintf("%s.%s", sqlutils.TestDB, tableName)

	// Attempt to forcefully remove each of the columns one at a time.
	for colIdxToRemove := 0; colIdxToRemove < len(schema); colIdxToRemove++ {
		if _, err := sqlDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sqlutils.TestDB)); err != nil {
			t.Fatal(err)
		}
		colIDToRemove := colIdxToRemove + 1

		// Create a fresh table every time we try and remove a column.
		createTable(t, sqlDB, fullTableName, schema, families)

		// Insert data in the old format.
		originalDataSize := 10
		for i := 0; i < originalDataSize; i++ {
			stringToInsert := fmt.Sprintf("my_string%d", i)
			insert := fmt.Sprintf("INSERT INTO %s VALUES (%d, %d, '%s')", fullTableName, i, 2*i, stringToInsert)
			if _, err := sqlDB.Exec(insert); err != nil {
				t.Fatal(err)
			}
		}
		deletedIndexName := mutateDescriptor(ctx, t, kvDB, tableName, colIdxToRemove, colIDToRemove)

		tc.Stopper().Stop(ctx)

		// Force refresh the descriptors by starting up a new cluster with the same store.
		tc, sqlDB, kvDB = serverutils.StartServer(t, args)

		insertNewColumnAndData(t, sqlDB, fullTableName, schema, colIdxToRemove)

		indexes := make([]string, 0, len(families)+1)
		for family := range families {
			indexes = append(indexes, "idx_"+family)
		}
		indexes = append(indexes, "primary")
		for _, index := range indexes {
			if index == deletedIndexName {
				continue
			}
			target := fmt.Sprintf("%s@%s", fullTableName, index)
			count := 0
			if err := sqlDB.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", target)).Scan(&count); err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, originalDataSize+1, count)

			rows, err := sqlDB.Query(fmt.Sprintf("SELECT * FROM %s", target))
			if err != nil {
				t.Fatal(err)
			}
			// Expect one more row than the original data size.
			columns, err := rows.Columns()
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, len(schema), len(columns))
		}
		if _, err := sqlDB.Exec(fmt.Sprintf("DROP TABLE %s", fullTableName)); err != nil {
			t.Fatal(err)
		}
	}
}

func insertNewColumnAndData(
	t *testing.T, sqlDB *gosql.DB, fullTableName string, schema [][]string, colIdxToRemove int,
) {
	sampleValues := map[string]string{
		"INT":    "300",
		"STRING": "'a sample string'",
		"BOOL":   "true",
	}
	newType := "BOOL"

	if _, err := sqlDB.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN new_col %s", fullTableName, newType)); err != nil {
		t.Fatal(err)
	}
	// Insert one row, the contents of the row will depend on which column was removed.
	var insertStmt strings.Builder
	_, _ = fmt.Fprintf(&insertStmt, "INSERT INTO %s VALUES (", fullTableName)
	isFirstVal := true
	for i, col := range schema {
		if i == colIdxToRemove {
			continue
		}
		if !isFirstVal {
			_, _ = fmt.Fprintf(&insertStmt, ", ")
		}
		isFirstVal = false
		colType := col[1]
		_, _ = fmt.Fprintf(&insertStmt, "%s", sampleValues[colType])
	}
	_, _ = fmt.Fprintf(&insertStmt, ", %s)", sampleValues[newType])
	if _, err := sqlDB.Exec(insertStmt.String()); err != nil {
		t.Fatal(err)
	}
}

func mutateDescriptor(
	ctx context.Context,
	t *testing.T,
	kvDB *kv.DB,
	tableName string,
	colIdxToRemove int,
	colIDToRemove int,
) string {
	testDescriptor := sqlbase.NewMutableExistingTableDescriptor(*sqlbase.GetTableDescriptor(kvDB, sqlutils.TestDB, tableName))
	// Remove the column and the column families.
	testDescriptor.Columns = append(testDescriptor.Columns[:colIdxToRemove], testDescriptor.Columns[colIdxToRemove+1:]...)

	// Drop indexes
	indexToDelete := -1
	var deletedIndexName string
	for i, index := range testDescriptor.Indexes {
		colIdx := -1
		for idx, id := range index.ColumnIDs {
			if id == sqlbase.ColumnID(colIDToRemove) {
				colIdx = idx
				break
			}
		}
		if colIdx == -1 {
			continue
		}
		if len(index.ColumnIDs) == 1 {
			// If there's only 1 column in this index, we need to get rid of the
			// index. We will at most drop 1 index since we only drop 1 column and
			// we constructed our indexes to contain disjoint set of columns.
			indexToDelete = i
			deletedIndexName = index.Name
		}
		index.ColumnIDs = append(index.ColumnIDs[:colIdx], index.ColumnIDs[colIdx+1:]...)
		index.ColumnDirections = append(index.ColumnDirections[:colIdx], index.ColumnDirections[colIdx+1:]...)
		index.ColumnNames = append(index.ColumnNames[:colIdx], index.ColumnNames[colIdx+1:]...)
		testDescriptor.Indexes[i] = index
	}
	if indexToDelete != -1 {
		testDescriptor.Indexes = append(testDescriptor.Indexes[:indexToDelete], testDescriptor.Indexes[indexToDelete+1:]...)
	}

	testDescriptor.RemoveColumnFromFamily(sqlbase.ColumnID(colIDToRemove))
	desc := testDescriptor.TableDesc()
	if err := writeTableDesc(ctx, kvDB, desc); err != nil {
		t.Fatal(err)
	}
	return deletedIndexName
}

func writeTableDesc(ctx context.Context, db *kv.DB, tableDesc *sqlbase.TableDescriptor) error {
	return db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		tableDesc.ModificationTime = txn.CommitTimestamp()
		return txn.Put(ctx, sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(tableDesc))
	})
}

// createTable constructs and executes the SQL command to create a table with a
// given name and a given schema. The table will also have an index for each
// family.
func createTable(
	t *testing.T,
	sqlDB *gosql.DB,
	fullTableName string,
	schema [][]string,
	families map[string][]string,
) {
	var createStmt strings.Builder
	fmt.Fprintf(&createStmt, "CREATE TABLE %s (", fullTableName)

	// Add the columns.
	for i, row := range schema {
		if i != 0 {
			_, _ = fmt.Fprint(&createStmt, ",")
		}
		colName := row[0]
		colType := row[1]
		fmt.Fprintf(&createStmt, "%s %s", colName, colType)
	}

	// Add the families.
	for familyName, cols := range families {
		_, _ = fmt.Fprintf(&createStmt, ", FAMILY %s (", familyName)
		fmt.Fprint(&createStmt, strings.Join(cols, ", "))
		fmt.Fprint(&createStmt, ")")
	}

	// Create an index for each family.
	for familyName, cols := range families {
		fmt.Fprintf(&createStmt, ", INDEX idx_%s (", familyName)
		fmt.Fprint(&createStmt, strings.Join(cols, ", "))
		fmt.Fprint(&createStmt, ")")
	}

	// Close schema definition.
	fmt.Fprint(&createStmt, ")")

	if _, err := sqlDB.Exec(createStmt.String()); err != nil {
		t.Fatal(err)
	}
}
