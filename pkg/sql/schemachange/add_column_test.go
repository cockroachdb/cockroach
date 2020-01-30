// Copyright 2018 The Cockroach Authors.
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
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

// TestNewSchemaOldData offers a sanity check to ensure that
// data can exist which do not have any referenced columns in
// the table descriptor. This can occur if a BACKUP was taken
// during a column deletion and then RESTOREd. The RESTORE will
// clean up the descriptor to remove mutations and the relevant
// columns, but the data will not be backfilled or cleaned up.
func TestNewSchemaOldData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Set up the server.
	ctx := context.Background()
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
	tableName := "foo"
	fullTableName := fmt.Sprintf("%s.%s", sqlutils.TestDB, tableName)
	if _, err := sqlDB.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", sqlutils.TestDB)); err != nil {
		t.Fatal(err)
	}
	createTableStmt := fmt.Sprintf("CREATE TABLE %s (a INT, b INT, c STRING, FAMILY f1 (a, c), FAMILY f2 (b))", fullTableName)
	originalColumns := [][]string{
		{"a", "INT"},
		{"b", "INT"},
		{"c", "STRING"},
	}
	// Attempt to forcefully remove each of the columns one at a time.
	for colIdxToRemove := 0; colIdxToRemove < 3; colIdxToRemove++ {
		t.Run(fmt.Sprintf("removing_column_%d", colIdxToRemove), func(t *testing.T) {
			// Create a fresh table every time we try and remove a column.
			sqlDB.Exec(createTableStmt)
			// Note that this is not generally true, but it is for the table created above.
			colIDToRemove := sqlbase.ColumnID(colIdxToRemove + 1)

			// Insert a row into the table with the original schema.
			insert := fmt.Sprintf("INSERT INTO %s VALUES (%d, %d, '%s')", fullTableName, 1, 2, "test string")
			if _, err := sqlDB.Exec(insert); err != nil {
				t.Fatal(err)
			}
			testDescriptor := sqlbase.NewMutableExistingTableDescriptor(*sqlbase.GetTableDescriptor(kvDB, sqlutils.TestDB, tableName))
			// Remove the column and the column families.
			testDescriptor.Columns = append(testDescriptor.Columns[:colIdxToRemove], testDescriptor.Columns[colIdxToRemove+1:]...)
			testDescriptor.CleanColumnFromFamily(colIDToRemove)
			desc := testDescriptor.TableDesc()
			if err := writeTableDesc(ctx, kvDB, desc); err != nil {
				t.Fatal(err)
			}
			tc.Stopper().Stop(ctx)

			// Force refresh the descriptors by restarting the cluster with the same store.
			tc, sqlDB, kvDB = serverutils.StartServer(t, args)

			// Add a new column to the table with a different type.
			newColType := "BOOL"
			if _, err := sqlDB.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN new_col %s", fullTableName, newColType)); err != nil {
				t.Fatal(err)
			}
			// Insert one row, the contents of the row will depend on which column was removed.
			insertStmt := insertNewRow(fullTableName, originalColumns, newColType, colIdxToRemove)
			if _, err := sqlDB.Exec(insertStmt); err != nil {
				t.Fatal(err)
			}

			// Ensure that the table has the correct dimensions.
			count := 0
			if err := sqlDB.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", fullTableName)).Scan(&count); err != nil {
				t.Fatal(err)
			}
			// Ensure that both the old and the new schema are present.
			assert.Equal(t, 2, count)

			rows, _ := sqlDB.Query(fmt.Sprintf("SELECT * FROM %s", fullTableName))
			columns, _ := rows.Columns()
			assert.Equal(t, 3, len(columns))

			// Clean up for the next iteration of the test.
			if _, err := sqlDB.Exec(fmt.Sprintf("DROP TABLE %s", fullTableName)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func insertNewRow(
	fullTableName string, originalColumns [][]string, newColType string, colIdxToRemove int,
) string {
	sampleValues := map[string]string{
		"INT":    "300",
		"STRING": "'a sample string'",
		"BOOL":   "true",
	}

	var insertStmt strings.Builder
	_, _ = fmt.Fprintf(&insertStmt, "INSERT INTO %s VALUES (", fullTableName)
	isFirstVal := true
	for i, col := range originalColumns {
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
	_, _ = fmt.Fprintf(&insertStmt, ", %s)", sampleValues[newColType])
	return insertStmt.String()
}

// writeTableDesc inserts (and possibly overwrites) a given table descriptor
// into system.descriptors. This simulates the effects of performing a RESTORE.
func writeTableDesc(ctx context.Context, db *client.DB, tableDesc *sqlbase.TableDescriptor) error {
	return db.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		if err := txn.SetSystemConfigTrigger(); err != nil {
			return err
		}
		tableDesc.ModificationTime = txn.CommitTimestamp()
		return txn.Put(ctx, sqlbase.MakeDescMetadataKey(tableDesc.ID), sqlbase.WrapDescriptor(tableDesc))
	})
}
