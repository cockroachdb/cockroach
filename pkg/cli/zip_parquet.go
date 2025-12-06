// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// parquetZipCollector collects data during a debug zip operation and writes
// it as parquet files at the end.
type parquetZipCollector struct {
	mu sync.Mutex

	// logs collects log entries from all nodes.
	logs *parquetLogWriter

	// sqlTables collects SQL query results.
	// Key is the table path (e.g., "crdb_internal/cluster_settings")
	sqlTables map[string]*parquetSQLWriter

	// jsonData collects JSON data that will be converted to parquet.
	// Key is the data type (e.g., "ranges", "gossip")
	jsonData map[string]*parquetJSONWriter
}

// newParquetZipCollector creates a new parquet collector for debug zip.
func newParquetZipCollector() (*parquetZipCollector, error) {
	logWriter, err := newParquetLogWriter()
	if err != nil {
		return nil, err
	}
	return &parquetZipCollector{
		logs:      logWriter,
		sqlTables: make(map[string]*parquetSQLWriter),
		jsonData:  make(map[string]*parquetJSONWriter),
	}, nil
}

// AddLogEntry adds a log entry to the collector.
func (c *parquetZipCollector) AddLogEntry(nodeID roachpb.NodeID, entry logpb.Entry) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.logs.AddEntry(nodeID, entry)
}

// GetSQLWriter returns a SQL writer for the given table, creating one if needed.
func (c *parquetZipCollector) GetSQLWriter(tablePath string) *parquetSQLWriter {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w, ok := c.sqlTables[tablePath]; ok {
		return w
	}
	w := newParquetSQLWriter(tablePath)
	c.sqlTables[tablePath] = w
	return w
}

// GetJSONWriter returns a JSON writer for the given data type, creating one if needed.
func (c *parquetZipCollector) GetJSONWriter(dataType string) *parquetJSONWriter {
	c.mu.Lock()
	defer c.mu.Unlock()
	if w, ok := c.jsonData[dataType]; ok {
		return w
	}
	w := newParquetJSONWriter(dataType)
	c.jsonData[dataType] = w
	return w
}

// Flush writes all collected data to the zipper.
func (c *parquetZipCollector) Flush(z *zipper, prefix string, zr *zipReporter) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Flush logs
	if err := c.logs.Flush(z, prefix, zr); err != nil {
		return err
	}

	// Flush SQL tables
	for tablePath, writer := range c.sqlTables {
		if err := writer.Flush(z, prefix, tablePath, zr); err != nil {
			return err
		}
	}

	// Flush JSON data
	for dataType, writer := range c.jsonData {
		if err := writer.Flush(z, prefix, dataType, zr); err != nil {
			return err
		}
	}

	return nil
}

// =============================================================================
// Log Writer
// =============================================================================

// parquetLogWriter collects log entries and writes them to parquet.
type parquetLogWriter struct {
	schema  *parquet.SchemaDefinition
	entries []logRow
}

// logRow represents a single log entry row.
type logRow struct {
	nodeID     int32
	severity   int32
	time       int64
	goroutine  int64
	file       string
	line       int64
	message    string
	tags       string
	counter    int64
	redactable bool
	channel    int32
}

// newParquetLogWriter creates a new parquet log writer.
func newParquetLogWriter() (*parquetLogWriter, error) {
	columnNames := []string{
		"node_id", "severity", "time", "goroutine", "file",
		"line", "message", "tags", "counter", "redactable", "channel",
	}
	columnTypes := []*types.T{
		types.Int4, types.Int4, types.Int, types.Int, types.String,
		types.Int, types.String, types.String, types.Int, types.Bool, types.Int4,
	}

	schema, err := parquet.NewSchema(columnNames, columnTypes)
	if err != nil {
		return nil, errors.Wrap(err, "creating log parquet schema")
	}

	return &parquetLogWriter{
		schema:  schema,
		entries: make([]logRow, 0, 10000),
	}, nil
}

// AddEntry adds a log entry to the buffer.
func (w *parquetLogWriter) AddEntry(nodeID roachpb.NodeID, entry logpb.Entry) error {
	w.entries = append(w.entries, logRow{
		nodeID:     int32(nodeID),
		severity:   int32(entry.Severity),
		time:       entry.Time,
		goroutine:  entry.Goroutine,
		file:       entry.File,
		line:       entry.Line,
		message:    entry.Message,
		tags:       entry.Tags,
		counter:    int64(entry.Counter),
		redactable: entry.Redactable,
		channel:    int32(entry.Channel),
	})
	return nil
}

// Flush writes all buffered entries to the zipper as a parquet file.
func (w *parquetLogWriter) Flush(z *zipper, prefix string, zr *zipReporter) error {
	if len(w.entries) == 0 {
		return nil
	}

	s := zr.start("writing logs parquet file")

	var buf bytes.Buffer
	writer, err := parquet.NewWriter(w.schema, &buf, parquet.WithCompressionCodec(parquet.CompressionZSTD))
	if err != nil {
		return s.fail(errors.Wrap(err, "creating parquet writer"))
	}

	for _, entry := range w.entries {
		datums := []tree.Datum{
			tree.NewDInt(tree.DInt(entry.nodeID)),
			tree.NewDInt(tree.DInt(entry.severity)),
			tree.NewDInt(tree.DInt(entry.time)),
			tree.NewDInt(tree.DInt(entry.goroutine)),
			tree.NewDString(entry.file),
			tree.NewDInt(tree.DInt(entry.line)),
			tree.NewDString(entry.message),
			tree.NewDString(entry.tags),
			tree.NewDInt(tree.DInt(entry.counter)),
			tree.MakeDBool(tree.DBool(entry.redactable)),
			tree.NewDInt(tree.DInt(entry.channel)),
		}
		if err := writer.AddRow(datums); err != nil {
			return s.fail(errors.Wrap(err, "writing parquet row"))
		}
	}

	if err := writer.Close(); err != nil {
		return s.fail(errors.Wrap(err, "closing parquet writer"))
	}

	z.Lock()
	defer z.Unlock()
	name := prefix + "/logs/nodes.parquet"
	zw, err := z.createLocked(name, timeutil.Now())
	if err != nil {
		return s.fail(errors.Wrap(err, "creating zip entry"))
	}
	if _, err := io.Copy(zw, &buf); err != nil {
		return s.fail(errors.Wrap(err, "writing to zip"))
	}

	s.done()
	return nil
}

// =============================================================================
// SQL Table Writer
// =============================================================================

// parquetSQLWriter collects SQL query results and writes them to parquet.
type parquetSQLWriter struct {
	tablePath string
	columns   []string
	rows      [][]driver.Value
	mu        sync.Mutex
}

// newParquetSQLWriter creates a new SQL writer.
func newParquetSQLWriter(tablePath string) *parquetSQLWriter {
	return &parquetSQLWriter{
		tablePath: tablePath,
		rows:      make([][]driver.Value, 0, 1000),
	}
}

// AddRows adds rows from a SQL query result to the buffer.
func (w *parquetSQLWriter) AddRows(columns []string, rows [][]driver.Value) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.columns == nil {
		w.columns = columns
	}
	w.rows = append(w.rows, rows...)
}

// Flush writes all buffered rows to the zipper as a parquet file.
func (w *parquetSQLWriter) Flush(z *zipper, prefix, tablePath string, zr *zipReporter) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.rows) == 0 {
		return nil
	}

	s := zr.start(redact.Sprintf("writing SQL table %s parquet file", tablePath))

	// All SQL data is stored as strings since we don't have type information.
	columnTypes := make([]*types.T, len(w.columns))
	for i := range columnTypes {
		columnTypes[i] = types.String
	}

	schema, err := parquet.NewSchema(w.columns, columnTypes)
	if err != nil {
		return s.fail(errors.Wrap(err, "creating parquet schema"))
	}

	var buf bytes.Buffer
	writer, err := parquet.NewWriter(schema, &buf, parquet.WithCompressionCodec(parquet.CompressionZSTD))
	if err != nil {
		return s.fail(errors.Wrap(err, "creating parquet writer"))
	}

	for _, row := range w.rows {
		datums := make([]tree.Datum, len(row))
		for i, val := range row {
			datums[i] = sqlValueToDatum(val)
		}
		if err := writer.AddRow(datums); err != nil {
			return s.fail(errors.Wrap(err, "writing parquet row"))
		}
	}

	if err := writer.Close(); err != nil {
		return s.fail(errors.Wrap(err, "closing parquet writer"))
	}

	z.Lock()
	defer z.Unlock()
	// Convert table path to parquet file path
	name := prefix + "/" + tablePath + ".parquet"
	zw, err := z.createLocked(name, timeutil.Now())
	if err != nil {
		return s.fail(errors.Wrap(err, "creating zip entry"))
	}
	if _, err := io.Copy(zw, &buf); err != nil {
		return s.fail(errors.Wrap(err, "writing to zip"))
	}

	s.done()
	return nil
}

// sqlValueToDatum converts a SQL driver.Value to a tree.Datum (as string).
func sqlValueToDatum(val driver.Value) tree.Datum {
	if val == nil {
		return tree.DNull
	}
	switch v := val.(type) {
	case string:
		return tree.NewDString(v)
	case []byte:
		return tree.NewDString(string(v))
	case int64:
		return tree.NewDString(fmt.Sprintf("%d", v))
	case float64:
		return tree.NewDString(fmt.Sprintf("%g", v))
	case bool:
		return tree.NewDString(fmt.Sprintf("%t", v))
	default:
		return tree.NewDString(fmt.Sprintf("%v", v))
	}
}

// =============================================================================
// JSON Data Writer
// =============================================================================

// parquetJSONWriter collects JSON data and writes it to parquet.
// JSON objects are flattened to a two-column schema: key and value (as JSON string).
type parquetJSONWriter struct {
	dataType string
	entries  []jsonRow
	mu       sync.Mutex
}

// jsonRow represents a flattened JSON row.
type jsonRow struct {
	nodeID int32  // 0 for cluster-wide data
	key    string // identifier (e.g., range_id, setting_name)
	value  string // JSON-encoded value
}

// newParquetJSONWriter creates a new JSON writer.
func newParquetJSONWriter(dataType string) *parquetJSONWriter {
	return &parquetJSONWriter{
		dataType: dataType,
		entries:  make([]jsonRow, 0, 1000),
	}
}

// AddEntry adds a JSON object to the buffer.
func (w *parquetJSONWriter) AddEntry(nodeID int32, key string, value interface{}) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return errors.Wrap(err, "marshaling JSON")
	}

	w.entries = append(w.entries, jsonRow{
		nodeID: nodeID,
		key:    key,
		value:  string(jsonBytes),
	})
	return nil
}

// AddRawJSON adds a raw JSON string to the buffer.
func (w *parquetJSONWriter) AddRawJSON(nodeID int32, key string, jsonStr string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.entries = append(w.entries, jsonRow{
		nodeID: nodeID,
		key:    key,
		value:  jsonStr,
	})
}

// Flush writes all buffered entries to the zipper as a parquet file.
func (w *parquetJSONWriter) Flush(z *zipper, prefix, dataType string, zr *zipReporter) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.entries) == 0 {
		return nil
	}

	s := zr.start(redact.Sprintf("writing %s parquet file", dataType))

	// Schema: node_id, key, value (JSON)
	columnNames := []string{"node_id", "key", "value"}
	columnTypes := []*types.T{types.Int4, types.String, types.String}

	schema, err := parquet.NewSchema(columnNames, columnTypes)
	if err != nil {
		return s.fail(errors.Wrap(err, "creating parquet schema"))
	}

	var buf bytes.Buffer
	writer, err := parquet.NewWriter(schema, &buf, parquet.WithCompressionCodec(parquet.CompressionZSTD))
	if err != nil {
		return s.fail(errors.Wrap(err, "creating parquet writer"))
	}

	for _, entry := range w.entries {
		datums := []tree.Datum{
			tree.NewDInt(tree.DInt(entry.nodeID)),
			tree.NewDString(entry.key),
			tree.NewDString(entry.value),
		}
		if err := writer.AddRow(datums); err != nil {
			return s.fail(errors.Wrap(err, "writing parquet row"))
		}
	}

	if err := writer.Close(); err != nil {
		return s.fail(errors.Wrap(err, "closing parquet writer"))
	}

	z.Lock()
	defer z.Unlock()
	name := prefix + "/" + dataType + ".parquet"
	zw, err := z.createLocked(name, timeutil.Now())
	if err != nil {
		return s.fail(errors.Wrap(err, "creating zip entry"))
	}
	if _, err := io.Copy(zw, &buf); err != nil {
		return s.fail(errors.Wrap(err, "writing to zip"))
	}

	s.done()
	return nil
}

// =============================================================================
// SQL Query Execution Helper
// =============================================================================

// ExecuteSQLToParquet executes a SQL query and writes results to the parquet collector.
func (c *parquetZipCollector) ExecuteSQLToParquet(
	ctx context.Context,
	conn clisqlclient.Conn,
	tablePath string,
	query string,
) error {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	columns := rows.Columns()
	if len(columns) == 0 {
		return nil
	}

	writer := c.GetSQLWriter(tablePath)

	// Collect all rows
	var allRows [][]driver.Value
	vals := make([]driver.Value, len(columns))
	for {
		if err := rows.Next(vals); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// Copy vals since Next reuses the slice
		rowCopy := make([]driver.Value, len(vals))
		copy(rowCopy, vals)
		allRows = append(allRows, rowCopy)
	}

	if len(allRows) > 0 {
		writer.AddRows(columns, allRows)
	}

	return nil
}

// =============================================================================
// JSON Data Collection Helper
// =============================================================================

// AddJSONData adds JSON data to the collector.
// The data is stored as (node_id, key, json_value) tuples.
func (c *parquetZipCollector) AddJSONData(dataType string, nodeID int32, key string, data interface{}) error {
	writer := c.GetJSONWriter(dataType)
	return writer.AddEntry(nodeID, key, data)
}

// AddJSONBytes adds raw JSON bytes to the collector.
func (c *parquetZipCollector) AddJSONBytes(dataType string, nodeID int32, key string, jsonBytes []byte) {
	writer := c.GetJSONWriter(dataType)
	writer.AddRawJSON(nodeID, key, string(jsonBytes))
}

// =============================================================================
// Integration with dumpTableDataForZip
// =============================================================================

// dumpTableDataForZipParquet is the parquet version of dumpTableDataForZip.
// It executes the query and stores results in the parquet collector.
func (zc *debugZipContext) dumpTableDataForZipParquet(
	ctx context.Context,
	conn clisqlclient.Conn,
	base, table string,
	query string,
) error {
	// Sanitize table name for path
	tablePath := base + "/" + strings.ReplaceAll(sanitizeFilename(table), ".", "_")

	return zc.parquetCollector.ExecuteSQLToParquet(ctx, conn, tablePath, query)
}
