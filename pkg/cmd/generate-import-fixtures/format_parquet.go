// Copyright 2026 The Cockroach Authors.
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
		case Text:
			fields[i], err = schema.NewPrimitiveNodeLogical(col.Name,
				parquet.Repetitions.Required, schema.StringLogicalType{},
				parquet.Types.ByteArray, defaultTypeLength, defaultSchemaFieldID)
		case Date:
			fields[i], err = schema.NewPrimitiveNodeLogical(col.Name,
				parquet.Repetitions.Required, schema.DateLogicalType{},
				parquet.Types.Int32, defaultTypeLength, defaultSchemaFieldID)
		default:
			return nil, fmt.Errorf("unsupported column type %s for column %s", col.Type, col.Name)
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
func (w *parquetWriter) WriteBatch(rows [][]any) error {
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
				v, ok := row[colIdx].(int64)
				if !ok {
					return fmt.Errorf("column %s row %d: expected int64, got %T", col.Name, i, row[colIdx])
				}
				values[i] = v
			}
			chunkWriter := cw.(*file.Int64ColumnChunkWriter)
			if _, err := chunkWriter.WriteBatch(values, nil, nil); err != nil {
				return fmt.Errorf("writing int64 column %s: %w", col.Name, err)
			}
		case Double:
			values := make([]float64, numRows)
			for i, row := range rows {
				v, ok := row[colIdx].(float64)
				if !ok {
					return fmt.Errorf("column %s row %d: expected float64, got %T", col.Name, i, row[colIdx])
				}
				values[i] = v
			}
			chunkWriter := cw.(*file.Float64ColumnChunkWriter)
			if _, err := chunkWriter.WriteBatch(values, nil, nil); err != nil {
				return fmt.Errorf("writing float64 column %s: %w", col.Name, err)
			}
		case Text:
			values := make([]parquet.ByteArray, numRows)
			for i, row := range rows {
				v, ok := row[colIdx].(string)
				if !ok {
					return fmt.Errorf("column %s row %d: expected string, got %T", col.Name, i, row[colIdx])
				}
				values[i] = parquet.ByteArray(v)
			}
			chunkWriter := cw.(*file.ByteArrayColumnChunkWriter)
			if _, err := chunkWriter.WriteBatch(values, nil, nil); err != nil {
				return fmt.Errorf("writing string column %s: %w", col.Name, err)
			}
		case Date:
			values := make([]int32, numRows)
			for i, row := range rows {
				t, ok := row[colIdx].(time.Time)
				if !ok {
					return fmt.Errorf("column %s row %d: expected time.Time, got %T", col.Name, i, row[colIdx])
				}
				values[i] = int32(t.Unix() / 86400)
			}
			chunkWriter := cw.(*file.Int32ColumnChunkWriter)
			if _, err := chunkWriter.WriteBatch(values, nil, nil); err != nil {
				return fmt.Errorf("writing date column %s: %w", col.Name, err)
			}
		default:
			return fmt.Errorf("unsupported column type %s for column %s", col.Type, col.Name)
		}
	}

	if err := rowGroup.Close(); err != nil {
		return fmt.Errorf("closing row group for %s: %w", w.table.Name, err)
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
