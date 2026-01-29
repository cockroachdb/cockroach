// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"io"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
)

// logParquetWriter writes log entries to a parquet file.
// The schema is designed to match logpb.Entry fields for efficient columnar storage.
type logParquetWriter struct {
	writer    *parquet.Writer
	schemaDef *parquet.SchemaDefinition
	datumBuf  []tree.Datum
}

// logParquetColumnNames defines the column names for the parquet schema.
// These match the fields in logpb.Entry.
var logParquetColumnNames = []string{
	"severity",          // Severity level (INFO, WARNING, ERROR, FATAL)
	"time",              // Nanoseconds since epoch
	"goroutine",         // Goroutine ID
	"file",              // Source file name
	"line",              // Line number in source file
	"message",           // Log message text
	"tags",              // Context tags
	"counter",           // Entry counter for audit
	"redactable",        // Whether message contains redaction markers
	"channel",           // Logging channel (DEV, OPS, HEALTH, etc.)
	"structured_end",    // JSON payload end index (if structured)
	"structured_start",  // JSON payload start index (if structured)
	"stack_trace_start", // Stack trace position (if present)
	"tenant_id",         // Tenant ID
	"tenant_name",       // Tenant name
}

// logParquetColumnTypes defines the SQL types for each column.
// Using appropriate types enables better compression.
var logParquetColumnTypes = []*types.T{
	types.Int,    // severity - small int, high compression with dictionary encoding
	types.Int,    // time - int64 timestamp, delta encoding
	types.Int,    // goroutine - int64
	types.String, // file - string, dictionary encoding helps
	types.Int,    // line - int64
	types.String, // message - string, main content
	types.String, // tags - string
	types.Int,    // counter - uint64
	types.Bool,   // redactable - boolean
	types.Int,    // channel - small int, dictionary encoding
	types.Int,    // structured_end - uint32
	types.Int,    // structured_start - uint32
	types.Int,    // stack_trace_start - uint32
	types.String, // tenant_id - string
	types.String, // tenant_name - string
}

// newLogParquetWriter creates a new parquet writer for log entries.
// It uses ZSTD compression for optimal compression ratio.
func newLogParquetWriter(sink io.Writer) (*logParquetWriter, error) {
	schemaDef, err := parquet.NewSchema(logParquetColumnNames, logParquetColumnTypes)
	if err != nil {
		return nil, err
	}

	writer, err := parquet.NewWriter(
		schemaDef,
		sink,
		parquet.WithCompressionCodec(parquet.CompressionZSTD),
		parquet.WithMaxRowGroupLength(10000), // Flush every 10k rows for memory efficiency
	)
	if err != nil {
		return nil, err
	}

	return &logParquetWriter{
		writer:    writer,
		schemaDef: schemaDef,
		datumBuf:  make([]tree.Datum, len(logParquetColumnNames)),
	}, nil
}

// sanitizeUTF8 ensures the string is valid UTF-8 by replacing invalid
// sequences with the Unicode replacement character (U+FFFD). This is
// necessary because Parquet readers (e.g., DuckDB) require valid UTF-8.
func sanitizeUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, "\uFFFD")
}

// AddEntry writes a log entry to the parquet file.
func (w *logParquetWriter) AddEntry(e logpb.Entry) error {
	// Convert entry fields to datums.
	// String fields are sanitized to ensure valid UTF-8 encoding.
	w.datumBuf[0] = tree.NewDInt(tree.DInt(e.Severity))
	w.datumBuf[1] = tree.NewDInt(tree.DInt(e.Time))
	w.datumBuf[2] = tree.NewDInt(tree.DInt(e.Goroutine))
	w.datumBuf[3] = tree.NewDString(sanitizeUTF8(e.File))
	w.datumBuf[4] = tree.NewDInt(tree.DInt(e.Line))
	w.datumBuf[5] = tree.NewDString(sanitizeUTF8(e.Message))
	w.datumBuf[6] = tree.NewDString(sanitizeUTF8(e.Tags))
	w.datumBuf[7] = tree.NewDInt(tree.DInt(e.Counter))
	w.datumBuf[8] = tree.MakeDBool(tree.DBool(e.Redactable))
	w.datumBuf[9] = tree.NewDInt(tree.DInt(e.Channel))
	w.datumBuf[10] = tree.NewDInt(tree.DInt(e.StructuredEnd))
	w.datumBuf[11] = tree.NewDInt(tree.DInt(e.StructuredStart))
	w.datumBuf[12] = tree.NewDInt(tree.DInt(e.StackTraceStart))
	w.datumBuf[13] = tree.NewDString(sanitizeUTF8(e.TenantID))
	w.datumBuf[14] = tree.NewDString(sanitizeUTF8(e.TenantName))

	return w.writer.AddRow(w.datumBuf)
}

// Flush flushes any buffered data to the sink.
func (w *logParquetWriter) Flush() error {
	return w.writer.Flush()
}

// Close closes the writer and flushes any remaining data.
func (w *logParquetWriter) Close() error {
	return w.writer.Close()
}
