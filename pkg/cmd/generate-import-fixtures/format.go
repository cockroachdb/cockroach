// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

// OutputFormat converts parsed TPC-H rows into a specific file format.
type OutputFormat interface {
	// Name returns the format identifier (e.g., "avro").
	Name() string
	// NewWriter creates a FormatWriter that streams batches of rows to output
	// files. shardIdx is 1-based. The caller must call Close on the returned
	// writer when done.
	NewWriter(table TableDef, outputDir string, shardIdx int) (FormatWriter, error)
	// WriteSchema writes any format-specific schema files to the output
	// directory. Called once per table after all shards are written.
	WriteSchema(table TableDef, outputDir string) error
}

// FormatWriter streams batches of rows to an output file.
type FormatWriter interface {
	// WriteBatch writes a batch of rows. Each row is a []any whose elements
	// correspond to the columns in the TableDef by index. The caller retains
	// ownership of the slice and may reuse it after WriteBatch returns.
	WriteBatch(rows [][]any) error
	// Close flushes buffered data and closes underlying file(s).
	Close() error
}
