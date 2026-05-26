// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/linkedin/goavro/v2"
	"github.com/stretchr/testify/require"
)

func TestAvroRoundTrip(t *testing.T) {
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
	format := &avroFormat{}
	w, err := format.NewWriter(table, dir, 1)
	require.NoError(t, err)

	date1 := time.Date(1995, 3, 15, 0, 0, 0, 0, time.UTC)

	rows := [][]any{
		{int64(1), 9.99, "hello", date1},
		{int64(2), 0.0, "world", time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	require.NoError(t, w.WriteBatch(rows))
	require.NoError(t, w.Close())

	// Read back the OCF file and verify.
	ocfPath := filepath.Join(dir, "roundtrip.1.ocf")
	ocfFile, err := os.Open(ocfPath)
	require.NoError(t, err)
	defer ocfFile.Close()

	reader, err := goavro.NewOCFReader(ocfFile)
	require.NoError(t, err)

	var records []map[string]any
	for reader.Scan() {
		datum, err := reader.Read()
		require.NoError(t, err)
		record, ok := datum.(map[string]any)
		require.True(t, ok)
		records = append(records, record)
	}
	require.NoError(t, reader.Err())
	require.Len(t, records, 2)

	// Verify first record.
	require.Equal(t, int64(1), records[0]["id"])
	require.InDelta(t, 9.99, records[0]["price"], 0.001)
	require.Equal(t, "hello", records[0]["label"])
	// Dates are stored as strings in AVRO.
	require.Equal(t, "1995-03-15", records[0]["dt"])

	// Verify epoch date.
	require.Equal(t, "1970-01-01", records[1]["dt"])
}

func TestBuildAvroSchema(t *testing.T) {
	table := TableDef{
		Name: "test",
		Columns: []ColumnDef{
			longCol("a"),
			doubleCol("b"),
			stringCol("c"),
			dateCol("d"),
		},
	}

	schemaJSON, err := buildAvroSchema(table)
	require.NoError(t, err)

	var schema map[string]any
	require.NoError(t, json.Unmarshal([]byte(schemaJSON), &schema))
	require.Equal(t, "record", schema["type"])
	require.Equal(t, "test", schema["name"])

	fields, ok := schema["fields"].([]any)
	require.True(t, ok)
	require.Len(t, fields, 4)

	// Verify type mappings.
	f0 := fields[0].(map[string]any)
	require.Equal(t, "a", f0["name"])
	require.Equal(t, "long", f0["type"])

	f3 := fields[3].(map[string]any)
	require.Equal(t, "d", f3["name"])
	require.Equal(t, "string", f3["type"]) // Date -> string in AVRO.
}

func TestBuildAvroSchemaUnknownType(t *testing.T) {
	table := TableDef{
		Name: "bad",
		Columns: []ColumnDef{
			{Name: "col", Type: ColumnType(99), Parse: parseString},
		},
	}
	_, err := buildAvroSchema(table)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported column type")
}

func TestColumnTypeToAvro(t *testing.T) {
	tests := []struct {
		ct   ColumnType
		want string
	}{
		{Long, "long"},
		{Double, "double"},
		{Text, "string"},
		{Date, "string"},
	}
	for _, tt := range tests {
		got, err := columnTypeToAvro(tt.ct)
		require.NoError(t, err)
		require.Equal(t, tt.want, got)
	}

	_, err := columnTypeToAvro(ColumnType(99))
	require.Error(t, err)
}

func TestRowToMap(t *testing.T) {
	cols := []ColumnDef{
		longCol("id"),
		stringCol("name"),
		dateCol("dt"),
	}
	date := time.Date(1995, 3, 15, 0, 0, 0, 0, time.UTC)
	row := []any{int64(42), "Alice", date}

	m := rowToMap(cols, row)
	require.Equal(t, int64(42), m["id"])
	require.Equal(t, "Alice", m["name"])
	// Date should be converted to string.
	require.Equal(t, "1995-03-15", m["dt"])
}
