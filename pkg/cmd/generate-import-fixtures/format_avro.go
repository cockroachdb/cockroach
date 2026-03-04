// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/linkedin/goavro/v2"
)

// avroFormat implements OutputFormat for AVRO. It produces both OCF
// (Object Container Format) files with snappy compression and binary
// records files for use with IMPORT INTO ... AVRO DATA.
type avroFormat struct{}

var _ OutputFormat = (*avroFormat)(nil)

func (a *avroFormat) Name() string { return "avro" }

// columnTypeToAvro maps a ColumnType to the corresponding AVRO type string.
func columnTypeToAvro(ct ColumnType) string {
	switch ct {
	case Long:
		return "long"
	case Double:
		return "double"
	case String, Date:
		return "string"
	default:
		return "string"
	}
}

// buildAvroSchema builds an AVRO schema JSON string from a TableDef. Each
// column maps to a non-nullable AVRO field.
func buildAvroSchema(table TableDef) string {
	fields := make([]map[string]interface{}, len(table.Columns))
	for i, col := range table.Columns {
		fields[i] = map[string]interface{}{
			"name": col.Name,
			"type": columnTypeToAvro(col.Type),
		}
	}
	schema := map[string]interface{}{
		"type":      "record",
		"name":      table.Name,
		"namespace": "tpch",
		"fields":    fields,
	}
	b, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(b)
}

// WriteFiles writes both an OCF file and a binary records file for the given
// shard. The OCF file uses snappy compression and embeds the schema. The binary
// records file contains one binary-encoded AVRO datum per line (newline
// separated), suitable for IMPORT INTO ... WITH data_as_binary_records.
func (a *avroFormat) WriteFiles(
	table TableDef, rows []map[string]interface{}, outputDir string, shardIdx int,
) error {
	schemaJSON := buildAvroSchema(table)
	codec, err := goavro.NewCodec(schemaJSON)
	if err != nil {
		return fmt.Errorf("creating avro codec for %s: %w", table.Name, err)
	}

	// Write OCF file.
	ocfPath := filepath.Join(outputDir, fmt.Sprintf("%s.%d.ocf", table.Name, shardIdx))
	ocfFile, err := os.Create(ocfPath)
	if err != nil {
		return fmt.Errorf("creating %s: %w", ocfPath, err)
	}
	defer ocfFile.Close()

	ocfWriter, err := goavro.NewOCFWriter(goavro.OCFConfig{
		W:               ocfFile,
		Schema:          schemaJSON,
		CompressionName: goavro.CompressionSnappyLabel,
	})
	if err != nil {
		return fmt.Errorf("creating OCF writer for %s: %w", table.Name, err)
	}

	if err := ocfWriter.Append(toNativeSlice(rows)); err != nil {
		return fmt.Errorf("writing OCF records for %s: %w", table.Name, err)
	}

	// Write binary records file.
	binPath := filepath.Join(outputDir, fmt.Sprintf("%s.%d.bin", table.Name, shardIdx))
	binFile, err := os.Create(binPath)
	if err != nil {
		return fmt.Errorf("creating %s: %w", binPath, err)
	}
	defer binFile.Close()

	for i, row := range rows {
		binary, err := codec.BinaryFromNative(nil, row)
		if err != nil {
			return fmt.Errorf("encoding binary record %d for %s: %w", i, table.Name, err)
		}
		if _, err := binFile.Write(binary); err != nil {
			return fmt.Errorf("writing binary record %d for %s: %w", i, table.Name, err)
		}
	}

	fmt.Printf("  wrote %s (%d records) and %s\n", ocfPath, len(rows), binPath)
	return nil
}

// WriteSchema writes the AVRO schema JSON file for the given table.
func (a *avroFormat) WriteSchema(table TableDef, outputDir string) error {
	schemaJSON := buildAvroSchema(table)
	schemaPath := filepath.Join(outputDir, fmt.Sprintf("%s.avsc", table.Name))
	if err := os.WriteFile(schemaPath, []byte(schemaJSON), 0644); err != nil {
		return fmt.Errorf("writing schema %s: %w", schemaPath, err)
	}
	fmt.Printf("  wrote schema %s\n", schemaPath)
	return nil
}

// toNativeSlice converts []map[string]interface{} to []interface{} for the
// goavro OCF writer.
func toNativeSlice(rows []map[string]interface{}) []interface{} {
	out := make([]interface{}, len(rows))
	for i, r := range rows {
		out[i] = r
	}
	return out
}
