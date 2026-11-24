// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/linkedin/goavro/v2"
)

// avroFormat implements OutputFormat for AVRO. It produces both OCF
// (Object Container Format) files with snappy compression and binary
// records files for use with IMPORT INTO ... AVRO DATA.
type avroFormat struct{}

var _ OutputFormat = (*avroFormat)(nil)

func (a *avroFormat) Name() string { return "avro" }

// columnTypeToAvro maps a ColumnType to the corresponding AVRO type string.
// Date columns are mapped to AVRO "string" deliberately because CRDB's IMPORT
// reads dates from AVRO string fields.
func columnTypeToAvro(ct ColumnType) (string, error) {
	switch ct {
	case Long:
		return "long", nil
	case Double:
		return "double", nil
	case Text, Date:
		return "string", nil
	default:
		return "", fmt.Errorf("unsupported column type %s", ct)
	}
}

// buildAvroSchema builds an AVRO schema JSON string from a TableDef. Each
// column maps to a non-nullable AVRO field.
func buildAvroSchema(table TableDef) (string, error) {
	fields := make([]map[string]any, len(table.Columns))
	for i, col := range table.Columns {
		avroType, err := columnTypeToAvro(col.Type)
		if err != nil {
			return "", fmt.Errorf("column %s: %w", col.Name, err)
		}
		fields[i] = map[string]any{
			"name": col.Name,
			"type": avroType,
		}
	}
	schema := map[string]any{
		"type":      "record",
		"name":      table.Name,
		"namespace": "tpch",
		"fields":    fields,
	}
	b, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshaling avro schema for %s: %w", table.Name, err)
	}
	return string(b), nil
}

// avroWriter streams batches of rows to both an OCF file and a binary records
// file.
type avroWriter struct {
	table     TableDef
	codec     *goavro.Codec
	ocfWriter *goavro.OCFWriter
	ocfFile   *os.File
	binFile   *os.File
	ocfPath   string
	binPath   string
	count     int
}

var _ FormatWriter = (*avroWriter)(nil)

// NewWriter creates an avroWriter that writes to both an OCF file and a binary
// records file.
func (a *avroFormat) NewWriter(
	table TableDef, outputDir string, shardIdx int,
) (FormatWriter, error) {
	schemaJSON, err := buildAvroSchema(table)
	if err != nil {
		return nil, err
	}
	codec, err := goavro.NewCodec(schemaJSON)
	if err != nil {
		return nil, fmt.Errorf("creating avro codec for %s: %w", table.Name, err)
	}

	ocfPath := filepath.Join(outputDir, fmt.Sprintf("%s.%d.ocf", table.Name, shardIdx))
	ocfFile, err := os.Create(ocfPath)
	if err != nil {
		return nil, fmt.Errorf("creating %s: %w", ocfPath, err)
	}

	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               ocfFile,
		Schema:          schemaJSON,
		CompressionName: goavro.CompressionSnappyLabel,
	})
	if err != nil {
		ocfFile.Close()
		return nil, fmt.Errorf("creating OCF writer for %s: %w", table.Name, err)
	}

	binPath := filepath.Join(outputDir, fmt.Sprintf("%s.%d.bin", table.Name, shardIdx))
	binFile, err := os.Create(binPath)
	if err != nil {
		ocfFile.Close()
		return nil, fmt.Errorf("creating %s: %w", binPath, err)
	}

	return &avroWriter{
		table:     table,
		codec:     codec,
		ocfWriter: ocfWriter,
		ocfFile:   ocfFile,
		binFile:   binFile,
		ocfPath:   ocfPath,
		binPath:   binPath,
	}, nil
}

// rowToMap converts a positional []any row into the map[string]any that goavro
// expects. Date columns (stored as time.Time) are formatted back to
// "YYYY-MM-DD" strings to match the AVRO schema.
func rowToMap(cols []ColumnDef, row []any) map[string]any {
	m := make(map[string]any, len(cols))
	for i, col := range cols {
		v := row[i]
		if col.Type == Date {
			if t, ok := v.(time.Time); ok {
				v = t.Format("2006-01-02")
			}
		}
		m[col.Name] = v
	}
	return m
}

// WriteBatch writes a batch of rows to both the OCF file and the binary records
// file.
func (w *avroWriter) WriteBatch(rows [][]any) error {
	native := make([]any, len(rows))
	for i, row := range rows {
		native[i] = rowToMap(w.table.Columns, row)
	}

	if err := w.ocfWriter.Append(native); err != nil {
		return fmt.Errorf("writing OCF records for %s: %w", w.table.Name, err)
	}

	for i, m := range native {
		binary, err := w.codec.BinaryFromNative(nil, m)
		if err != nil {
			return fmt.Errorf("encoding binary record %d for %s: %w", w.count+i, w.table.Name, err)
		}
		if _, err := w.binFile.Write(binary); err != nil {
			return fmt.Errorf("writing binary record %d for %s: %w", w.count+i, w.table.Name, err)
		}
	}

	w.count += len(rows)
	return nil
}

// Close closes the OCF and binary files, printing a summary.
func (w *avroWriter) Close() error {
	ocfErr := w.ocfFile.Close()
	binErr := w.binFile.Close()
	if err := errors.Join(ocfErr, binErr); err != nil {
		return fmt.Errorf("closing avro files for %s: %w", w.table.Name, err)
	}
	fmt.Printf("  wrote %s (%d records) and %s\n", w.ocfPath, w.count, w.binPath)
	return nil
}

// WriteSchema writes the AVRO schema JSON file for the given table.
func (a *avroFormat) WriteSchema(table TableDef, outputDir string) error {
	schemaJSON, err := buildAvroSchema(table)
	if err != nil {
		return err
	}
	schemaPath := filepath.Join(outputDir, fmt.Sprintf("%s.avsc", table.Name))
	if err := os.WriteFile(schemaPath, []byte(schemaJSON), 0644); err != nil {
		return fmt.Errorf("writing schema %s: %w", schemaPath, err)
	}
	fmt.Printf("  wrote schema %s\n", schemaPath)
	return nil
}
