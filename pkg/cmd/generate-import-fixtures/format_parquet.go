// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/compress"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/schema"
)

// parquetFormat implements OutputFormat for Parquet. It produces Parquet files
// with Snappy compression. Date columns are stored as INT32 with DATE logical
// type (days since Unix epoch).
type parquetFormat struct{}

var _ OutputFormat = (*parquetFormat)(nil)

func (p *parquetFormat) Name() string { return "parquet" }

// defaultSchemaFieldID lets the parquet library auto-assign field IDs.
const defaultSchemaFieldID = int32(-1)

// defaultTypeLength signals no fixed type length for variable-length types.
const defaultTypeLength = -1

// buildParquetSchema constructs a Parquet schema from a TPC-H TableDef. All
// columns are Required (non-nullable), matching the TPC-H specification.
func buildParquetSchema(table TableDef) (*schema.Schema, error) {
	fields := make([]schema.Node, len(table.Columns))
	for i, col := range table.Columns {
		var err error
		switch col.Type {
		case Long:
			fields[i], err = schema.NewPrimitiveNodeLogical(col.Name,
				parquet.Repetitions.Required, schema.NewIntLogicalType(64, true),
				parquet.Types.Int64, defaultTypeLength, defaultSchemaFieldID)
		case Double:
			fields[i] = schema.NewFloat64Node(col.Name,
				parquet.Repetitions.Required, defaultSchemaFieldID)
		case String:
			fields[i], err = schema.NewPrimitiveNodeLogical(col.Name,
				parquet.Repetitions.Required, schema.StringLogicalType{},
				parquet.Types.ByteArray, defaultTypeLength, defaultSchemaFieldID)
		case Date:
			fields[i], err = schema.NewPrimitiveNodeLogical(col.Name,
				parquet.Repetitions.Required, schema.DateLogicalType{},
				parquet.Types.Int32, defaultTypeLength, defaultSchemaFieldID)
		default:
			return nil, fmt.Errorf("unsupported column type %d for column %s", col.Type, col.Name)
		}
		if err != nil {
			return nil, fmt.Errorf("creating schema node for %s: %w", col.Name, err)
		}
	}
	root, err := schema.NewGroupNode("schema", parquet.Repetitions.Required, fields, defaultSchemaFieldID)
	if err != nil {
		return nil, fmt.Errorf("creating schema root: %w", err)
	}
	return schema.NewSchema(root), nil
}

// unixEpoch is the Unix epoch used for converting date strings to days.
var unixEpoch = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)

// dateToDays converts a "YYYY-MM-DD" date string to the number of days since
// the Unix epoch.
func dateToDays(dateStr string) (int32, error) {
	t, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return 0, fmt.Errorf("parsing date %q: %w", dateStr, err)
	}
	return int32(t.Sub(unixEpoch).Hours() / 24), nil
}

// WriteFiles writes a Parquet file for the given shard with Snappy compression.
// The output file is named {table}.parquet.{shardIdx}.
func (p *parquetFormat) WriteFiles(
	table TableDef, rows []map[string]interface{}, outputDir string, shardIdx int,
) error {
	sch, err := buildParquetSchema(table)
	if err != nil {
		return err
	}

	outPath := filepath.Join(outputDir, fmt.Sprintf("%s.parquet.%d", table.Name, shardIdx))
	f, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("creating %s: %w", outPath, err)
	}
	defer f.Close()

	props := parquet.NewWriterProperties(
		parquet.WithCreatedBy("cockroachdb"),
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	writer := file.NewParquetWriter(f, sch.Root(), file.WithWriterProps(props))
	defer func() {
		// Close is idempotent; the deferred call handles cleanup on error paths
		// while the explicit call below checks the error on success.
		_ = writer.Close()
	}()

	rowGroup := writer.AppendBufferedRowGroup()
	numRows := len(rows)

	for colIdx, col := range table.Columns {
		cw, err := rowGroup.Column(colIdx)
		if err != nil {
			return fmt.Errorf("getting column writer for %s: %w", col.Name, err)
		}

		switch col.Type {
		case Long:
			values := make([]int64, numRows)
			for i, row := range rows {
				values[i] = row[col.Name].(int64)
			}
			w := cw.(*file.Int64ColumnChunkWriter)
			if _, err := w.WriteBatch(values, nil, nil); err != nil {
				return fmt.Errorf("writing int64 column %s: %w", col.Name, err)
			}
		case Double:
			values := make([]float64, numRows)
			for i, row := range rows {
				values[i] = row[col.Name].(float64)
			}
			w := cw.(*file.Float64ColumnChunkWriter)
			if _, err := w.WriteBatch(values, nil, nil); err != nil {
				return fmt.Errorf("writing float64 column %s: %w", col.Name, err)
			}
		case String:
			values := make([]parquet.ByteArray, numRows)
			for i, row := range rows {
				values[i] = parquet.ByteArray(row[col.Name].(string))
			}
			w := cw.(*file.ByteArrayColumnChunkWriter)
			if _, err := w.WriteBatch(values, nil, nil); err != nil {
				return fmt.Errorf("writing string column %s: %w", col.Name, err)
			}
		case Date:
			values := make([]int32, numRows)
			for i, row := range rows {
				days, err := dateToDays(row[col.Name].(string))
				if err != nil {
					return fmt.Errorf("converting date in column %s, row %d: %w", col.Name, i, err)
				}
				values[i] = days
			}
			w := cw.(*file.Int32ColumnChunkWriter)
			if _, err := w.WriteBatch(values, nil, nil); err != nil {
				return fmt.Errorf("writing date column %s: %w", col.Name, err)
			}
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("closing parquet writer for %s: %w", table.Name, err)
	}

	fmt.Printf("  wrote %s (%d records)\n", outPath, numRows)
	return nil
}

// WriteSchema is a no-op for Parquet because the schema is embedded in the
// file footer.
func (p *parquetFormat) WriteSchema(table TableDef, outputDir string) error {
	return nil
}
