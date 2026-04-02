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

// parquetWriter streams batches of rows to a Parquet file. Each call to
// WriteBatch creates a new row group.
type parquetWriter struct {
	table   TableDef
	outPath string
	writer  *file.Writer
	count   int
}

var _ FormatWriter = (*parquetWriter)(nil)

// NewWriter creates a parquetWriter that writes to a Parquet file with Snappy
// compression.
func (p *parquetFormat) NewWriter(
	table TableDef, outputDir string, shardIdx int,
) (FormatWriter, error) {
	sch, err := buildParquetSchema(table)
	if err != nil {
		return nil, err
	}

	outPath := filepath.Join(outputDir, fmt.Sprintf("%s.parquet.%d", table.Name, shardIdx))
	f, err := os.Create(outPath)
	if err != nil {
		return nil, fmt.Errorf("creating %s: %w", outPath, err)
	}

	props := parquet.NewWriterProperties(
		parquet.WithCreatedBy("cockroachdb"),
		parquet.WithCompression(compress.Codecs.Snappy),
	)
	writer := file.NewParquetWriter(f, sch.Root(), file.WithWriterProps(props))

	return &parquetWriter{
		table:   table,
		outPath: outPath,
		writer:  writer,
	}, nil
}

// WriteBatch writes a batch of rows as a single Parquet row group.
func (w *parquetWriter) WriteBatch(rows []map[string]interface{}) error {
	rowGroup := w.writer.AppendBufferedRowGroup()
	numRows := len(rows)

	for colIdx, col := range w.table.Columns {
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
			chunkWriter := cw.(*file.Int64ColumnChunkWriter)
			if _, err := chunkWriter.WriteBatch(values, nil, nil); err != nil {
				return fmt.Errorf("writing int64 column %s: %w", col.Name, err)
			}
		case Double:
			values := make([]float64, numRows)
			for i, row := range rows {
				values[i] = row[col.Name].(float64)
			}
			chunkWriter := cw.(*file.Float64ColumnChunkWriter)
			if _, err := chunkWriter.WriteBatch(values, nil, nil); err != nil {
				return fmt.Errorf("writing float64 column %s: %w", col.Name, err)
			}
		case String:
			values := make([]parquet.ByteArray, numRows)
			for i, row := range rows {
				values[i] = parquet.ByteArray(row[col.Name].(string))
			}
			chunkWriter := cw.(*file.ByteArrayColumnChunkWriter)
			if _, err := chunkWriter.WriteBatch(values, nil, nil); err != nil {
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
			chunkWriter := cw.(*file.Int32ColumnChunkWriter)
			if _, err := chunkWriter.WriteBatch(values, nil, nil); err != nil {
				return fmt.Errorf("writing date column %s: %w", col.Name, err)
			}
		}
	}

	w.count += numRows
	return nil
}

// Close closes the Parquet writer and underlying file, printing a summary.
// The parquet writer's Close method closes the underlying sink (os.File), so
// we must not close it again ourselves.
func (w *parquetWriter) Close() error {
	if err := w.writer.Close(); err != nil {
		return fmt.Errorf("closing parquet writer for %s: %w", w.table.Name, err)
	}
	fmt.Printf("  wrote %s (%d records)\n", w.outPath, w.count)
	return nil
}

// WriteSchema is a no-op for Parquet because the schema is embedded in the
// file footer.
func (p *parquetFormat) WriteSchema(table TableDef, outputDir string) error {
	return nil
}
