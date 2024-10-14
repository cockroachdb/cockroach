// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCreateTargetTableStmt verifies that createTargetTableStmt returns the
// correct create and drop statments given the target table and new table name.
func TestCreateTargetTableStmt(t *testing.T) {
	createStmt, dropStmt := createTargetTableStmt("stock", "stock_1")
	require.Equal(t, "CREATE TABLE stock_1 (LIKE tpcc.stock INCLUDING ALL)", createStmt)
	require.Equal(t, "DROP TABLE stock_1", dropStmt)
}

// TestExtractTableNameFromFileName verifies that extractTableNameFromFileName
// correctly extracts table name from a file name.
func TestExtractTableNameFromFileName(t *testing.T) {
	str, err := extractTableNameFromFileName("/2023-11-07/202311071946288411402400000000000-c1a4f08eaf3f6ecd-1-5-000000b7-stock-7.parquet")
	require.Equal(t, "stock", str)
	require.NoError(t, err)
	str, err = extractTableNameFromFileName("/202311071946288411402400000000000-c1a4f08eaf3f6ecd-1-5-000000b7-stock-7.parquet")
	require.Equal(t, "stock", str)
	require.NoError(t, err)
}

// TestUpsertStmtForTable verifies that upsertStmtForTable correctly formats
// upsert statement for the given target table and args.
func TestUpsertStmtForTable(t *testing.T) {
	upsertStmt := upsertStmtForTable("stock", []string{"1", "a", "2022"})
	require.Equal(t, "UPSERT INTO stock VALUES (1,a,2022)", upsertStmt)
}
