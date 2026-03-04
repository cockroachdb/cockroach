// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

// OutputFormat converts parsed TPC-H rows into a specific file format.
type OutputFormat interface {
	// Name returns the format identifier (e.g., "avro").
	Name() string
	// WriteFiles writes the parsed rows to output files. It receives the table
	// definition, the parsed rows, the output directory, and the shard index.
	WriteFiles(table TableDef, rows []map[string]interface{}, outputDir string, shardIdx int) error
	// WriteSchema writes any format-specific schema files to the output
	// directory. Called once per table after all shards are written.
	WriteSchema(table TableDef, outputDir string) error
}
