// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/stretchr/testify/require"
)

func TestParquetRoundTrip(t *testing.T) {
	table := TableDef{
		Name: "roundtrip",
		Columns: []ColumnDef{
			longCol("id"),
			doubleCol("price"),
			stringCol("label"),
			dateCol("dt"),
		},
	}

	dir := t.TempDir()
	format := &parquetFormat{}
	w, err := format.NewWriter(table, dir, 1)
	require.NoError(t, err)

	date1 := time.Date(1995, 3, 15, 0, 0, 0, 0, time.UTC)
	date2 := time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

	rows := [][]any{
		{int64(1), 9.99, "hello", date1},
		{int64(2), 0.0, "world", date2},
	}
	require.NoError(t, w.WriteBatch(rows))
	require.NoError(t, w.Close())

	// Read back and verify.
	outPath := filepath.Join(dir, "roundtrip.parquet.1")
	reader, err := file.OpenParquetFile(outPath, false)
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()

	require.Equal(t, 4, reader.MetaData().Schema.NumColumns())
	require.Equal(t, 1, reader.NumRowGroups())

	rg := reader.RowGroup(0)
	require.Equal(t, int64(2), rg.NumRows())

	// Verify int64 column.
	int64Reader, err := rg.Column(0)
	require.NoError(t, err)
	int64Chunk := int64Reader.(*file.Int64ColumnChunkReader)
	int64Vals := make([]int64, 2)
	n, _, err := int64Chunk.ReadBatch(2, int64Vals, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
	require.Equal(t, []int64{1, 2}, int64Vals)

	// Verify float64 column.
	float64Reader, err := rg.Column(1)
	require.NoError(t, err)
	float64Chunk := float64Reader.(*file.Float64ColumnChunkReader)
	float64Vals := make([]float64, 2)
	n, _, err = float64Chunk.ReadBatch(2, float64Vals, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
	require.InDelta(t, 9.99, float64Vals[0], 0.001)
	require.InDelta(t, 0.0, float64Vals[1], 0.001)

	// Verify date column (days since epoch).
	dateReader, err := rg.Column(3)
	require.NoError(t, err)
	dateChunk := dateReader.(*file.Int32ColumnChunkReader)
	dateVals := make([]int32, 2)
	n, _, err = dateChunk.ReadBatch(2, dateVals, nil, nil)
	require.NoError(t, err)
	require.Equal(t, int64(2), n)
	// 1995-03-15 is 9204 days since epoch.
	require.Equal(t, int32(9204), dateVals[0])
	// 1970-01-01 is day 0.
	require.Equal(t, int32(0), dateVals[1])
}

func TestParquetWriteBatchTypeMismatch(t *testing.T) {
	table := TableDef{
		Name:    "mismatch",
		Columns: []ColumnDef{longCol("id")},
	}

	dir := t.TempDir()
	format := &parquetFormat{}
	w, err := format.NewWriter(table, dir, 1)
	require.NoError(t, err)
	defer func() {
		// Close may fail since we wrote bad data; that's fine.
		_ = w.Close()
		require.NoError(t, os.Remove(filepath.Join(dir, "mismatch.parquet.1")))
	}()

	// Pass a string where int64 is expected.
	rows := [][]any{{"not-a-number"}}
	err = w.WriteBatch(rows)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected int64")
}

func TestParquetWriteBatchUnknownType(t *testing.T) {
	table := TableDef{
		Name: "unknown",
		Columns: []ColumnDef{
			{Name: "col", Type: ColumnType(99), Parse: parseString},
		},
	}

	dir := t.TempDir()
	format := &parquetFormat{}
	// buildParquetSchema should fail on unknown type.
	_, err := format.NewWriter(table, dir, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported column type")
}

func TestBuildParquetSchema(t *testing.T) {
	table := TableDef{
		Name: "test",
		Columns: []ColumnDef{
			longCol("a"),
			doubleCol("b"),
			stringCol("c"),
			dateCol("d"),
		},
	}

	sch, err := buildParquetSchema(table)
	require.NoError(t, err)
	require.Equal(t, 4, sch.NumColumns())
	require.Equal(t, "a", sch.Column(0).Name())
	require.Equal(t, "b", sch.Column(1).Name())
	require.Equal(t, "c", sch.Column(2).Name())
	require.Equal(t, "d", sch.Column(3).Name())
}
