// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// mockWriter records batches written to it for verification.
type mockWriter struct {
	batches [][]any
}

var _ FormatWriter = (*mockWriter)(nil)

func (m *mockWriter) WriteBatch(rows [][]any) error {
	// Copy rows to avoid aliasing with the caller's reusable slice.
	cp := make([][]any, len(rows))
	for i, row := range rows {
		rowCp := make([]any, len(row))
		copy(rowCp, row)
		cp[i] = rowCp
	}
	m.batches = append(m.batches, cp...)
	return nil
}

func (m *mockWriter) Close() error { return nil }

// testTable is a small schema used across tests.
var testTable = TableDef{
	Name: "test",
	Columns: []ColumnDef{
		longCol("id"),
		stringCol("name"),
		doubleCol("price"),
	},
}

func TestProcessShard(t *testing.T) {
	input := strings.Join([]string{
		"1|Alice|9.99|",
		"2|Bob|19.99|",
		"3|Carol|29.99|",
	}, "\n")

	w := &mockWriter{}
	n, err := processShard(strings.NewReader(input), testTable, w)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Len(t, w.batches, 3)

	// Verify parsed values.
	require.Equal(t, int64(1), w.batches[0][0])
	require.Equal(t, "Alice", w.batches[0][1])
	require.InDelta(t, 9.99, w.batches[0][2], 0.001)
}

func TestProcessShardTrailingPipe(t *testing.T) {
	// dbgen produces trailing pipes; verify they are stripped.
	input := "1|Alice|9.99|\n"
	w := &mockWriter{}
	n, err := processShard(strings.NewReader(input), testTable, w)
	require.NoError(t, err)
	require.Equal(t, 1, n)
}

func TestProcessShardFieldCountMismatch(t *testing.T) {
	input := "1|Alice|\n" // Only 2 fields, but testTable expects 3.
	w := &mockWriter{}
	_, err := processShard(strings.NewReader(input), testTable, w)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 3 fields")
}

func TestProcessShardParseError(t *testing.T) {
	input := "notanumber|Alice|9.99|\n"
	w := &mockWriter{}
	_, err := processShard(strings.NewReader(input), testTable, w)
	require.Error(t, err)
	require.Contains(t, err.Error(), "column id")
}

func TestProcessShardEmptyLines(t *testing.T) {
	input := "\n1|Alice|9.99|\n\n2|Bob|19.99|\n\n"
	w := &mockWriter{}
	n, err := processShard(strings.NewReader(input), testTable, w)
	require.NoError(t, err)
	require.Equal(t, 2, n)
}

func TestShardFiles(t *testing.T) {
	// Dimension table: single file.
	regionShards := shardFiles("region")
	require.Len(t, regionShards, 1)
	require.Equal(t, "region.tbl", regionShards[0].fileName)
	require.Equal(t, 1, regionShards[0].idx)

	// Fact table: 8 shards.
	lineitemShards := shardFiles("lineitem")
	require.Len(t, lineitemShards, 8)
	require.Equal(t, "lineitem.tbl.1", lineitemShards[0].fileName)
	require.Equal(t, 1, lineitemShards[0].idx)
	require.Equal(t, "lineitem.tbl.8", lineitemShards[7].fileName)
	require.Equal(t, 8, lineitemShards[7].idx)
}

func TestAvailableFormats(t *testing.T) {
	result := availableFormats()
	// Should be sorted.
	require.Equal(t, "avro, parquet", result)
}
