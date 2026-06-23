// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/compress"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/pqarrow"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// viewToRow is a test helper that extracts values from a parquetRowView into []interface{}.
// This allows tests to continue using the simple []interface{} format.
func viewToRow(rowData interface{}) []interface{} {
	view, ok := rowData.(*parquetRowView)
	if !ok {
		panic(fmt.Sprintf("expected *parquetRowView, got %T", rowData))
	}

	rowIdx := view.rowIndex
	row := make([]interface{}, view.numColumns)

	for colIdx := 0; colIdx < view.numColumns; colIdx++ {
		batch := view.batches[colIdx]
		if batch == nil {
			row[colIdx] = nil
			continue
		}

		if batch.isNull[rowIdx] {
			row[colIdx] = nil
			continue
		}

		// Extract typed value
		switch batch.physicalType {
		case parquet.Types.Boolean:
			row[colIdx] = batch.boolValues[rowIdx]
		case parquet.Types.Int32:
			row[colIdx] = batch.int32Values[rowIdx]
		case parquet.Types.Int64:
			row[colIdx] = batch.int64Values[rowIdx]
		case parquet.Types.Float:
			row[colIdx] = batch.float32Values[rowIdx]
		case parquet.Types.Double:
			row[colIdx] = batch.float64Values[rowIdx]
		case parquet.Types.ByteArray:
			row[colIdx] = batch.byteArrayValues[rowIdx]
		case parquet.Types.FixedLenByteArray:
			row[colIdx] = batch.fixedLenByteArrayValues[rowIdx]
		}
	}

	return row
}

// createTestParquetFile creates a simple Parquet file in memory for testing.
func createTestParquetFile(
	t *testing.T, numRows int, compression compress.Compression,
) *fileReader {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)
	rows := make([]parquetRow, numRows)
	for i := range numRows {
		var name, score any
		if i%3 != 0 {
			name = "name_" + string(rune('A'+i%26))
		}
		if i%5 != 0 {
			score = float64(i) * 1.5
		}
		rows[i] = parquetRow{int32(i), name, score, i%2 == 0}
	}
	return makeParquetFile(t, schema, rows, withParquetCompression(compression))
}

func TestParquetRowProducerBasicScanning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Test all supported compression codecs
	testCases := []struct {
		name        string
		compression compress.Compression
	}{
		{"Uncompressed", compress.Codecs.Uncompressed},
		{"Gzip", compress.Codecs.Gzip},
		{"Snappy", compress.Codecs.Snappy},
		{"Zstd", compress.Codecs.Zstd},
		{"Brotli", compress.Codecs.Brotli},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a test Parquet file with 10 rows and specified compression
			fileReader := createTestParquetFile(t, 10, tc.compression)

			producer, err := newParquetRowProducer(fileReader, nil)
			require.NoError(t, err)
			require.NotNil(t, producer)

			// Verify initial state
			require.Equal(t, int64(10), producer.totalRows)
			require.Equal(t, 4, producer.numColumns) // id, name, score, active

			// Scan all rows
			rowCount := 0
			for producer.Scan() {
				row, err := producer.Row()
				require.NoError(t, err)
				require.NotNil(t, row)

				rowData := viewToRow(row)
				require.Equal(t, 4, len(rowData))

				// Verify ID column (always present)
				require.Equal(t, int32(rowCount), rowData[0])

				rowCount++
			}

			require.NoError(t, producer.Err())
			require.Equal(t, 10, rowCount)
			require.Equal(t, float32(1.0), producer.Progress())
		})
	}
}

func TestParquetRowProducerNullHandling(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a test Parquet file
	fileReader := createTestParquetFile(t, 15, compress.Codecs.Uncompressed)

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	rowCount := 0
	for producer.Scan() {
		row, err := producer.Row()
		require.NoError(t, err)

		rowData := viewToRow(row)

		// Check NULL values based on test data pattern
		// name is NULL when rowCount % 3 == 0
		if rowCount%3 == 0 {
			require.Nil(t, rowData[1], "row %d: name should be NULL", rowCount)
		} else {
			require.NotNil(t, rowData[1], "row %d: name should not be NULL", rowCount)
		}

		// score is NULL when rowCount % 5 == 0
		if rowCount%5 == 0 {
			require.Nil(t, rowData[2], "row %d: score should be NULL", rowCount)
		} else {
			require.NotNil(t, rowData[2], "row %d: score should not be NULL", rowCount)
		}

		rowCount++
	}

	require.NoError(t, producer.Err())
	require.Equal(t, 15, rowCount)
}

func TestParquetRowProducerBatching(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a file with more rows than the batch size
	numRows := 250
	fileReader := createTestParquetFile(t, numRows, compress.Codecs.Uncompressed)

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	// Should read in batches of 100 (defaultParquetBatchSize)
	require.Equal(t, int64(100), producer.batchSize)

	// Scan all rows
	rowCount := 0
	for producer.Scan() {
		row, err := producer.Row()
		require.NoError(t, err)
		require.NotNil(t, row)

		rowData := viewToRow(row)
		require.Equal(t, int32(rowCount), rowData[0])

		rowCount++
	}

	require.NoError(t, producer.Err())
	require.Equal(t, numRows, rowCount)
}

func TestParquetRowProducerProgress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	numRows := 100
	fileReader := createTestParquetFile(t, numRows, compress.Codecs.Uncompressed)

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	// Initial progress should be 0
	require.Equal(t, float32(0), producer.Progress())

	// Read half the rows
	for i := 0; i < numRows/2; i++ {
		require.True(t, producer.Scan())
		_, err := producer.Row()
		require.NoError(t, err)
	}

	// Progress should be approximately 0.5
	progress := producer.Progress()
	require.InDelta(t, 0.5, progress, 0.01)

	// Read remaining rows
	for producer.Scan() {
		_, err := producer.Row()
		require.NoError(t, err)
	}

	// Progress should be 1.0
	require.Equal(t, float32(1.0), producer.Progress())
}

func TestParquetRowProducerSingleRow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a Parquet file with just 1 row
	fileReader := createTestParquetFile(t, 1, compress.Codecs.Uncompressed)

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	// Should scan exactly one row
	require.True(t, producer.Scan())
	row, err := producer.Row()
	require.NoError(t, err)
	require.NotNil(t, row)

	// No more rows
	require.False(t, producer.Scan())
	require.NoError(t, producer.Err())
}

func TestParquetRowProducerSkip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	fileReader := createTestParquetFile(t, 10, compress.Codecs.Uncompressed)

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	// Scan and skip first row
	require.True(t, producer.Scan())
	err = producer.Skip()
	require.NoError(t, err)

	// Read second row - should have ID = 1 (not 0)
	require.True(t, producer.Scan())
	row, err := producer.Row()
	require.NoError(t, err)

	rowData := viewToRow(row)
	require.Equal(t, int32(1), rowData[0])
}

// TestParquetRowProducerEmptyRowGroups tests that empty row groups are skipped transparently.
// This can occur with files from PyArrow v9-15 or partitioned datasets.
func TestParquetRowProducerEmptyRowGroups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a Parquet file with structure: [2 rows] [0 rows] [3 rows]
	// This tests that we properly skip the empty row group and read all 5 rows
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "value", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(
		schema,
		&buf,
		parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Uncompressed),
			parquet.WithMaxRowGroupLength(2), // Force multiple row groups
		),
		pqarrow.DefaultWriterProps(),
	)
	require.NoError(t, err)

	allocator := memory.DefaultAllocator

	// Write first row group (2 rows)
	{
		recordBuilder := array.NewRecordBuilder(allocator, schema)
		recordBuilder.Field(0).(*array.Int32Builder).AppendValues([]int32{1, 2}, nil)
		recordBuilder.Field(1).(*array.StringBuilder).AppendValues([]string{"first", "second"}, nil)
		record := recordBuilder.NewRecord()
		defer record.Release()

		err = writer.Write(record)
		require.NoError(t, err)
	}

	// Write empty row group (0 rows) - simulates PyArrow v9-15 bug or empty partition
	{
		recordBuilder := array.NewRecordBuilder(allocator, schema)
		record := recordBuilder.NewRecord()
		defer record.Release()

		err = writer.Write(record)
		require.NoError(t, err)
	}

	// Write third row group (3 rows)
	{
		recordBuilder := array.NewRecordBuilder(allocator, schema)
		recordBuilder.Field(0).(*array.Int32Builder).AppendValues([]int32{3, 4, 5}, nil)
		recordBuilder.Field(1).(*array.StringBuilder).AppendValues([]string{"third", "fourth", "fifth"}, nil)
		record := recordBuilder.NewRecord()
		defer record.Release()

		err = writer.Write(record)
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	// Now read the file and verify all 5 rows are read (empty row group is skipped)
	reader := bytes.NewReader(buf.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	// Verify the file structure and find which row groups are empty
	parquetReader, err := file.NewParquetReader(bytes.NewReader(buf.Bytes()))
	require.NoError(t, err)
	defer func() { _ = parquetReader.Close() }()

	numRowGroups := parquetReader.NumRowGroups()
	require.GreaterOrEqual(t, numRowGroups, 1)

	// Count total rows across all row groups
	var totalRows int64
	hasEmptyRowGroup := false
	for i := 0; i < numRowGroups; i++ {
		rowsInGroup := parquetReader.RowGroup(i).NumRows()
		totalRows += rowsInGroup
		if rowsInGroup == 0 {
			hasEmptyRowGroup = true
		}
	}

	// We expect 5 total rows and at least one empty row group
	require.Equal(t, int64(5), totalRows, "file should contain 5 total rows")
	require.True(t, hasEmptyRowGroup, "file should have at least one empty row group to test the bug fix")

	// Scan through all rows
	rowCount := 0
	expectedIDs := []int32{1, 2, 3, 4, 5}
	expectedValues := []string{"first", "second", "third", "fourth", "fifth"}

	for producer.Scan() {
		rowData, err := producer.Row()
		require.NoError(t, err)

		view := rowData.(*parquetRowView)
		require.Equal(t, 2, view.numColumns)

		// Get ID from first column
		idVal, isNull, err := view.batches[0].GetValueAt(int(view.rowIndex))
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, expectedIDs[rowCount], idVal.(int32))

		// Get value from second column
		valueVal, isNull, err := view.batches[1].GetValueAt(int(view.rowIndex))
		require.NoError(t, err)
		require.False(t, isNull)
		require.Equal(t, []byte(expectedValues[rowCount]), valueVal.([]byte))

		rowCount++
	}

	require.NoError(t, producer.Err())
	require.Equal(t, 5, rowCount, "should read all 5 rows, skipping empty row group")

	// Verify progress tracking is correct
	require.InDelta(t, 1.0, producer.Progress(), 0.01)
}

func TestParquetRowConsumerCreation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a simple import context for testing
	importCtx := &parallelImportContext{
		targetCols: tree.NameList{"id", "name", "score", "active"},
	}

	// Create a mock Parquet file to get the schema
	fileReader := createTestParquetFile(t, 1, compress.Codecs.Uncompressed)
	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	consumer, err := newParquetRowConsumer(importCtx, producer, &importFileContext{}, false)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	// Verify field mapping was created correctly
	require.Equal(t, 4, len(consumer.fieldNameToIdx))
	require.Contains(t, consumer.fieldNameToIdx, "id")
	require.Contains(t, consumer.fieldNameToIdx, "name")
	require.Contains(t, consumer.fieldNameToIdx, "score")
	require.Contains(t, consumer.fieldNameToIdx, "active")
}

// TestParquetAutomaticColumnMapping tests automatic column mapping when no columns are specified.
func TestParquetAutomaticColumnMapping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a mock table descriptor with columns matching the Parquet schema
	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name: "test_table",
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, Nullable: false},
			{Name: "name", ID: 2, Type: types.String, Nullable: true},
			{Name: "score", ID: 3, Type: types.Float, Nullable: true},
			{Name: "active", ID: 4, Type: types.Bool, Nullable: true},
		},
		NextColumnID: 5,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ColumnNames: []string{"id", "name", "score", "active"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4},
			},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
		},
	}).BuildImmutableTable()

	// Create import context with NO target columns (automatic mapping)
	importCtx := &parallelImportContext{
		targetCols: tree.NameList{}, // Empty - should use all visible columns
		tableDesc:  tableDesc,
	}

	// Create a mock Parquet file to get the schema
	fileReader := createTestParquetFile(t, 1, compress.Codecs.Uncompressed)
	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	consumer, err := newParquetRowConsumer(importCtx, producer, &importFileContext{}, false)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	// Verify field mapping was created automatically for all visible columns
	require.Equal(t, 4, len(consumer.fieldNameToIdx))
	require.Contains(t, consumer.fieldNameToIdx, "id")
	require.Contains(t, consumer.fieldNameToIdx, "name")
	require.Contains(t, consumer.fieldNameToIdx, "score")
	require.Contains(t, consumer.fieldNameToIdx, "active")

	// Verify the mapping indices are correct
	require.Equal(t, 0, consumer.fieldNameToIdx["id"])
	require.Equal(t, 1, consumer.fieldNameToIdx["name"])
	require.Equal(t, 2, consumer.fieldNameToIdx["score"])
	require.Equal(t, 3, consumer.fieldNameToIdx["active"])
}

// TestParquetColumnOrderMapping tests that column mapping works correctly when
// target columns are specified in a different order than the table schema.
// This is a regression test for a bug where indices from targetCols were incorrectly
// used to index visibleCols, causing wrong column types to be validated.
func TestParquetColumnOrderMapping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a table with columns in one order: [id, name, age, score]
	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name: "test_table",
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, Nullable: false},
			{Name: "name", ID: 2, Type: types.String, Nullable: true},
			{Name: "age", ID: 3, Type: types.Int, Nullable: true},
			{Name: "score", ID: 4, Type: types.Float, Nullable: true},
		},
		NextColumnID: 5,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ColumnNames: []string{"id", "name", "age", "score"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4},
			},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
		},
	}).BuildImmutableTable()

	// Import with columns in DIFFERENT order: [score, name, id]
	// This tests that we correctly map to visibleCols indices, not targetCols indices
	importCtx := &parallelImportContext{
		targetCols: tree.NameList{"score", "name", "id"},
		tableDesc:  tableDesc,
	}

	// Create Parquet file with columns: score (float), name (string), id (int)
	// The bug would cause:
	// - "score" to be validated against visibleCols[0] = "id" (int) - type mismatch!
	// - "name" to be validated against visibleCols[1] = "name" (string) - accidentally correct
	// - "id" to be validated against visibleCols[2] = "age" (int) - accidentally works
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		},
		nil,
	)

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(
		schema,
		&buf,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed)),
		pqarrow.DefaultWriterProps(),
	)
	require.NoError(t, err)

	// Write one row
	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	recordBuilder.Field(0).(*array.Float64Builder).Append(95.5)
	recordBuilder.Field(1).(*array.StringBuilder).Append("Alice")
	recordBuilder.Field(2).(*array.Int32Builder).Append(42)
	record := recordBuilder.NewRecord()
	defer record.Release()

	err = writer.Write(record)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Create producer
	reader := bytes.NewReader(buf.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	producer, err := newParquetRowProducer(fileReader, importCtx)
	require.NoError(t, err)

	// Create consumer - this should validate column types correctly
	consumer, err := newParquetRowConsumer(importCtx, producer, &importFileContext{}, false)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	// Verify fieldNameToIdx maps to full-table visibleCols indices.
	require.Equal(t, 3, consumer.fieldNameToIdx["score"], "score should map to visibleCols index 3")
	require.Equal(t, 1, consumer.fieldNameToIdx["name"], "name should map to visibleCols index 1")
	require.Equal(t, 0, consumer.fieldNameToIdx["id"], "id should map to visibleCols index 0")

	// Verify colMapping uses target-column indices (position in targetCols),
	// not full-table indices. Target order is [score, name, id].
	require.Equal(t, 0, consumer.colMapping[0], "parquet col 0 (score) -> target index 0")
	require.Equal(t, 1, consumer.colMapping[1], "parquet col 1 (name) -> target index 1")
	require.Equal(t, 2, consumer.colMapping[2], "parquet col 2 (id) -> target index 2")
}

// TestParquetSubsetColumnMapping is a regression test for #169899. When IMPORT
// INTO excludes columns, colMapping must use target-column indices (position in
// the targetCols list) rather than full-table visibleCols indices. Using
// full-table indices caused an out-of-bounds panic in FillDatums because
// DatumRowConverter.Datums is sized to len(targetCols).
func TestParquetSubsetColumnMapping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Table schema shared by all sub-tests: [name, id, age, score]. The
	// primary key is id (not the first column), so test cases can exclude
	// parquet col 0 (name) while still satisfying IMPORT INTO's requirement
	// that targetCols include the primary key.
	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name: "test_table",
		Columns: []descpb.ColumnDescriptor{
			{Name: "name", ID: 2, Type: types.String, Nullable: true},
			{Name: "id", ID: 1, Type: types.Int, Nullable: true},
			{Name: "age", ID: 3, Type: types.Int, Nullable: true},
			{Name: "score", ID: 4, Type: types.Float, Nullable: true},
		},
		NextColumnID: 5,
		Families: []descpb.ColumnFamilyDescriptor{{
			Name:        "primary",
			ColumnNames: []string{"name", "id", "age", "score"},
			ColumnIDs:   []descpb.ColumnID{2, 1, 3, 4},
		}},
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
		},
	}).BuildImmutableTable()

	// makeParquetFile writes a single row [name="Alice", id=1, age=30,
	// score=95.5] using the supplied column names (cases). Type order is
	// fixed: string, int32, int32, float64.
	makeParquetFile := func(t *testing.T, colNames [4]string) *fileReader {
		t.Helper()
		pqSchema := arrow.NewSchema(
			[]arrow.Field{
				{Name: colNames[0], Type: arrow.BinaryTypes.String, Nullable: true},
				{Name: colNames[1], Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				{Name: colNames[2], Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				{Name: colNames[3], Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			},
			nil,
		)

		var buf bytes.Buffer
		writer, err := pqarrow.NewFileWriter(
			pqSchema, &buf,
			parquet.NewWriterProperties(
				parquet.WithCompression(compress.Codecs.Uncompressed)),
			pqarrow.DefaultWriterProps(),
		)
		require.NoError(t, err)

		rb := array.NewRecordBuilder(memory.DefaultAllocator, pqSchema)
		rb.Field(0).(*array.StringBuilder).Append("Alice")
		rb.Field(1).(*array.Int32Builder).Append(1)
		rb.Field(2).(*array.Int32Builder).Append(30)
		rb.Field(3).(*array.Float64Builder).Append(95.5)
		record := rb.NewRecord()
		defer record.Release()

		require.NoError(t, writer.Write(record))
		require.NoError(t, writer.Close())

		reader := bytes.NewReader(buf.Bytes())
		return &fileReader{
			Reader:   reader,
			ReaderAt: reader,
			Seeker:   reader,
			total:    int64(buf.Len()),
		}
	}

	lowerCaseNames := [4]string{"name", "id", "age", "score"}

	// Sample row written by makeParquetFile; expected datums below pick out
	// the targeted subset (lookup is case-insensitive).
	allDatums := map[string]tree.Datum{
		"id":    tree.NewDInt(1),
		"name":  tree.NewDString("Alice"),
		"age":   tree.NewDInt(30),
		"score": tree.NewDFloat(95.5),
	}

	testCases := []struct {
		name string
		// parquetColNames overrides the parquet schema column names; defaults
		// to lowerCaseNames when zero-valued.
		parquetColNames [4]string
		targetCols      tree.NameList
		// Expected colMapping: parquet col index -> target col index (-1 = skipped).
		wantColMapping []int
	}{
		{
			// Excludes parquet col 0 (name, a non-PK column).
			name:           "first column excluded",
			targetCols:     tree.NameList{"id", "age", "score"},
			wantColMapping: []int{-1, 0, 1, 2},
		},
		{
			// Excluding a non-last column is the original panic from #169899.
			name:           "middle column excluded",
			targetCols:     tree.NameList{"name", "id", "score"},
			wantColMapping: []int{0, 1, -1, 2},
		},
		{
			name:           "last column excluded",
			targetCols:     tree.NameList{"name", "id", "age"},
			wantColMapping: []int{0, 1, 2, -1},
		},
		{
			name:           "multiple columns excluded",
			targetCols:     tree.NameList{"id", "score"},
			wantColMapping: []int{-1, 0, -1, 1},
		},
		{
			// Parquet matching is case-insensitive; mixed-case parquet column
			// names must still resolve to the lowercase target columns.
			name:            "mixed case parquet columns",
			parquetColNames: [4]string{"Name", "ID", "Age", "Score"},
			targetCols:      tree.NameList{"id", "age", "score"},
			wantColMapping:  []int{-1, 0, 1, 2},
		},
	}

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	defer row.TestingSetDatumRowConverterBatchSize(2)()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			importCtx := &parallelImportContext{
				targetCols: tc.targetCols,
				tableDesc:  tableDesc,
			}

			colNames := tc.parquetColNames
			if colNames == [4]string{} {
				colNames = lowerCaseNames
			}
			fr := makeParquetFile(t, colNames)
			producer, err := newParquetRowProducer(fr, importCtx)
			require.NoError(t, err)

			consumer, err := newParquetRowConsumer(
				importCtx, producer, &importFileContext{}, false)
			require.NoError(t, err)

			require.Equal(t, tc.wantColMapping, consumer.colMapping)

			// Verify all mapped indices are within the target column bounds.
			// This is the invariant FillDatums relies on: conv.Datums and
			// conv.VisibleColTypes are sized to len(targetCols).
			for _, idx := range consumer.colMapping {
				if idx >= 0 {
					require.Less(t, idx, len(tc.targetCols),
						"colMapping index must be < len(targetCols)")
				}
			}

			// Drive FillDatums end-to-end. This is the call site that
			// originally panicked with an out-of-bounds index when a non-last
			// column was excluded (#169899).
			conv, err := row.NewDatumRowConverter(
				ctx, &semaCtx, tableDesc, tc.targetCols, evalCtx.Copy(),
				nil /* kvCh */, nil /* seqChunkProvider */, nil /* metrics */, nil, /* db */
			)
			require.NoError(t, err)

			require.True(t, producer.Scan(), "expected at least one row")
			rowData, err := producer.Row()
			require.NoError(t, err)
			require.NoError(t, consumer.FillDatums(ctx, rowData, 1, conv))

			require.Len(t, conv.Datums, len(tc.targetCols))
			for i, colName := range tc.targetCols {
				want := allDatums[strings.ToLower(string(colName))]
				require.Equalf(t, want, conv.Datums[i],
					"datum at target index %d (column %q)", i, colName)
			}
		})
	}
}

// TestParquetCaseConflictingColumns tests that we reject tables with columns
// that differ only in case, which is incompatible with Parquet's case-insensitive matching.
func TestParquetCaseConflictingColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a table with columns that differ only in case: "name" and "NAME"
	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name: "test_table",
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, Nullable: false},
			{Name: "name", ID: 2, Type: types.String, Nullable: true}, // lowercase
			{Name: "NAME", ID: 3, Type: types.String, Nullable: true}, // uppercase - conflicts!
		},
		NextColumnID: 4,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ColumnNames: []string{"id", "name", "NAME"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3},
			},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
		},
	}).BuildImmutableTable()

	importCtx := &parallelImportContext{
		targetCols: tree.NameList{},
		tableDesc:  tableDesc,
	}

	// Create a simple Parquet file
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(
		schema,
		&buf,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed)),
		pqarrow.DefaultWriterProps(),
	)
	require.NoError(t, err)

	// Write one row
	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	recordBuilder.Field(0).(*array.Int32Builder).Append(1)
	recordBuilder.Field(1).(*array.StringBuilder).Append("test")
	record := recordBuilder.NewRecord()
	defer record.Release()

	err = writer.Write(record)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Create producer
	reader := bytes.NewReader(buf.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	producer, err := newParquetRowProducer(fileReader, importCtx)
	require.NoError(t, err)

	// Attempt to create consumer - should fail due to case-conflicting columns
	_, err = newParquetRowConsumer(importCtx, producer, &importFileContext{}, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "differ only in case")
	require.Contains(t, err.Error(), "name")
	require.Contains(t, err.Error(), "NAME")
}

// TestParquetCaseInsensitiveMatching tests that case-insensitive matching works
// when there's no collision (e.g., Parquet has "Name", table has "name").
func TestParquetCaseInsensitiveMatching(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a table with lowercase column names
	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name: "test_table",
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, Nullable: false},
			{Name: "name", ID: 2, Type: types.String, Nullable: true}, // lowercase
			{Name: "score", ID: 3, Type: types.Float, Nullable: true}, // lowercase
		},
		NextColumnID: 4,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ColumnNames: []string{"id", "name", "score"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3},
			},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
		},
	}).BuildImmutableTable()

	importCtx := &parallelImportContext{
		targetCols: tree.NameList{},
		tableDesc:  tableDesc,
	}

	// Create a Parquet file with MIXED CASE column names (different from table)
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "ID", Type: arrow.PrimitiveTypes.Int32, Nullable: false},     // uppercase
			{Name: "Name", Type: arrow.BinaryTypes.String, Nullable: true},      // capital N
			{Name: "SCORE", Type: arrow.PrimitiveTypes.Float64, Nullable: true}, // uppercase
		},
		nil,
	)

	var buf bytes.Buffer
	writer, err := pqarrow.NewFileWriter(
		schema,
		&buf,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed)),
		pqarrow.DefaultWriterProps(),
	)
	require.NoError(t, err)

	// Write test data
	recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	recordBuilder.Field(0).(*array.Int32Builder).Append(42)
	recordBuilder.Field(1).(*array.StringBuilder).Append("Alice")
	recordBuilder.Field(2).(*array.Float64Builder).Append(95.5)
	record := recordBuilder.NewRecord()
	defer record.Release()

	err = writer.Write(record)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// Create producer
	reader := bytes.NewReader(buf.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	producer, err := newParquetRowProducer(fileReader, importCtx)
	require.NoError(t, err)

	// Create consumer - should succeed with case-insensitive matching
	consumer, err := newParquetRowConsumer(importCtx, producer, &importFileContext{}, false)
	require.NoError(t, err)
	require.NotNil(t, consumer)

	// Verify the mapping works (all lowercased)
	require.Equal(t, 3, len(consumer.fieldNameToIdx))
	require.Equal(t, 0, consumer.fieldNameToIdx["id"], "ID → id")
	require.Equal(t, 1, consumer.fieldNameToIdx["name"], "Name → name")
	require.Equal(t, 2, consumer.fieldNameToIdx["score"], "SCORE → score")
}

// TestParquetMissingRequiredColumn tests that validation fails when a required column is missing.
func TestParquetMissingRequiredColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a mock table descriptor with a required column not in the Parquet file
	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name: "test_table",
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, Nullable: false},
			{Name: "name", ID: 2, Type: types.String, Nullable: true},
			{Name: "email", ID: 3, Type: types.String, Nullable: false}, // Required but not in Parquet
			{Name: "score", ID: 4, Type: types.Float, Nullable: true},
		},
		NextColumnID: 5,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ColumnNames: []string{"id", "name", "email", "score"},
				ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4},
			},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
		},
	}).BuildImmutableTable()

	// Create import context with automatic mapping
	importCtx := &parallelImportContext{
		targetCols: tree.NameList{}, // Empty - should use all visible columns
		tableDesc:  tableDesc,
	}

	// Create a mock Parquet file (has id, name, score, active - but NOT email)
	fileReader := createTestParquetFile(t, 1, compress.Codecs.Uncompressed)
	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	// Should fail because "email" is required (non-nullable, no default) but missing from Parquet
	consumer, err := newParquetRowConsumer(importCtx, producer, &importFileContext{}, false)
	require.Error(t, err)
	require.Nil(t, consumer)
	require.Contains(t, err.Error(), "email")
	require.Contains(t, err.Error(), "non-nullable, no default")
}

// TestParquetMultipleFloat64Columns tests that multiple float64 columns
// don't interfere with each other due to buffer reuse.
func TestParquetMultipleFloat64Columns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()

	// Mimic the Titanic dataset structure with Age and Fare as float64
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "PassengerId", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "Age", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "Fare", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	ageBuilder := builder.Field(1).(*array.Float64Builder)
	fareBuilder := builder.Field(2).(*array.Float64Builder)

	// PassengerId, Age, Fare
	idBuilder.Append(1)
	ageBuilder.Append(32.0)
	fareBuilder.Append(7.75)

	idBuilder.Append(2)
	ageBuilder.Append(26.0)
	fareBuilder.Append(79.20)

	record := builder.NewRecord()
	defer record.Release()

	// Write to Parquet format
	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	err = writer.Write(record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back using our importer
	reader := bytes.NewReader(buf.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)
	require.NotNil(t, producer)

	// Verify we can read the data correctly
	require.Equal(t, int64(2), producer.totalRows)
	require.Equal(t, 3, producer.numColumns)

	// Scan and verify each row
	rowNum := 0
	for producer.Scan() {
		row, err := producer.Row()
		require.NoError(t, err)

		rowData := viewToRow(row)
		require.Equal(t, 3, len(rowData))

		if rowNum == 0 {
			// Row 1: PassengerId=1, Age=32.0, Fare=7.75
			require.Equal(t, int64(1), rowData[0])
			require.Equal(t, float64(32.0), rowData[1], "Age should be 32.0, not %v", rowData[1])
			require.Equal(t, float64(7.75), rowData[2], "Fare should be 7.75, not %v", rowData[2])
		} else if rowNum == 1 {
			// Row 2: PassengerId=2, Age=26.0, Fare=79.20
			require.Equal(t, int64(2), rowData[0])
			require.Equal(t, float64(26.0), rowData[1], "Age should be 26.0, not %v", rowData[1])
			require.Equal(t, float64(79.20), rowData[2], "Fare should be 79.20, not %v", rowData[2])
		}

		rowNum++
	}

	require.NoError(t, producer.Err())
	require.Equal(t, 2, rowNum)
}

// TestParquetMultipleFloat64ColumnsLargeFile tests buffer reuse across batches
func TestParquetMultipleFloat64ColumnsLargeFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()

	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "PassengerId", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "Age", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "Fare", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int64Builder)
	ageBuilder := builder.Field(1).(*array.Float64Builder)
	fareBuilder := builder.Field(2).(*array.Float64Builder)

	// Create 250 rows to force multiple batches (batch size is 100)
	for i := int64(0); i < 250; i++ {
		idBuilder.Append(i + 1)
		ageBuilder.Append(float64(20 + i))        // Age: 20, 21, 22, ..., 269
		fareBuilder.Append(10.0 + float64(i)*0.5) // Fare: 10.0, 10.5, 11.0, ..., 134.5
	}

	record := builder.NewRecord()
	defer record.Release()

	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	err = writer.Write(record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Read back using our importer
	reader := bytes.NewReader(buf.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)

	// Verify all rows
	rowNum := int64(0)
	for producer.Scan() {
		row, err := producer.Row()
		require.NoError(t, err)

		rowData := viewToRow(row)
		expectedAge := float64(20 + rowNum)
		expectedFare := 10.0 + float64(rowNum)*0.5

		require.Equal(t, rowNum+1, rowData[0], "Row %d: wrong PassengerId", rowNum)
		require.Equal(t, expectedAge, rowData[1], "Row %d: Age should be %.1f, got %v", rowNum, expectedAge, rowData[1])
		require.Equal(t, expectedFare, rowData[2], "Row %d: Fare should be %.2f, got %v", rowNum, expectedFare, rowData[2])

		rowNum++
	}

	require.NoError(t, producer.Err())
	require.Equal(t, int64(250), rowNum)
}

// TestParquetStrictValidation tests strict_validation mode for Parquet imports.
func TestParquetStrictValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()

	// Create Parquet file with extra column not in table
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "extra_column", Type: arrow.BinaryTypes.String, Nullable: true}, // Extra column
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int32Builder)
	nameBuilder := builder.Field(1).(*array.StringBuilder)
	extraBuilder := builder.Field(2).(*array.StringBuilder)

	// Add test data
	idBuilder.Append(1)
	nameBuilder.Append("test")
	extraBuilder.Append("extra_value")

	record := builder.NewRecord()
	defer record.Release()

	// Write to Parquet format
	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	err = writer.Write(record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	// Create table with only id and name columns (missing extra_column)
	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name: "test_table",
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int, Nullable: false},
			{Name: "name", ID: 2, Type: types.String, Nullable: true},
		},
		NextColumnID: 3,
		Families: []descpb.ColumnFamilyDescriptor{
			{
				Name:        "primary",
				ColumnNames: []string{"id", "name"},
				ColumnIDs:   []descpb.ColumnID{1, 2},
			},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
		},
	}).BuildImmutableTable()

	// Test 1: Non-strict mode (default) - should succeed and skip extra_column
	t.Run("NonStrict", func(t *testing.T) {
		importCtx := &parallelImportContext{
			targetCols: tree.NameList{}, // Empty - should use all visible columns
			tableDesc:  tableDesc,
		}

		reader := bytes.NewReader(buf.Bytes())
		fileReader := &fileReader{
			Reader:   reader,
			ReaderAt: reader,
			Seeker:   reader,
			total:    int64(buf.Len()),
		}
		producer, err := newParquetRowProducer(fileReader, nil)
		require.NoError(t, err)

		consumer, err := newParquetRowConsumer(importCtx, producer, &importFileContext{}, false)
		require.NoError(t, err)
		require.NotNil(t, consumer)

		// Verify field mapping - extra_column should not be in mapping
		require.Equal(t, 2, len(consumer.fieldNameToIdx))
		require.Contains(t, consumer.fieldNameToIdx, "id")
		require.Contains(t, consumer.fieldNameToIdx, "name")
		require.NotContains(t, consumer.fieldNameToIdx, "extra_column")
	})

	// Test 2: Strict mode - should error on extra columns
	t.Run("StrictFlagSet", func(t *testing.T) {
		importCtx := &parallelImportContext{
			targetCols: tree.NameList{},
			tableDesc:  tableDesc,
		}

		reader := bytes.NewReader(buf.Bytes())
		fileReader := &fileReader{
			Reader:   reader,
			ReaderAt: reader,
			Seeker:   reader,
			total:    int64(buf.Len()),
		}
		producer, err := newParquetRowProducer(fileReader, nil)
		require.NoError(t, err)

		_, err = newParquetRowConsumer(importCtx, producer, &importFileContext{}, true)
		require.Error(t, err)
		require.Contains(t, err.Error(), "extra_column")
		require.Contains(t, err.Error(), "not in the target table")
	})
}

// TestParquetReadTitanicFile reads the Titanic Parquet file from testdata
func TestParquetReadTitanicFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Open the Titanic data file from testdata
	dir := datapathutils.TestDataPath(t, "parquet")
	f, err := os.Open(filepath.Join(dir, "titanic.parquet"))
	require.NoError(t, err)
	defer f.Close()

	// Get file size for the fileReader
	stat, err := f.Stat()
	require.NoError(t, err)

	// Create fileReader
	fileReader := &fileReader{
		Reader:   f,
		ReaderAt: f,
		Seeker:   f,
		total:    stat.Size(),
	}

	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)
	require.NotNil(t, producer)

	t.Logf("Total rows: %d", producer.totalRows)
	t.Logf("Num columns: %d", producer.numColumns)
	t.Logf("Total row groups: %d", producer.totalRowGroups)

	// Check rows per row group
	for i := 0; i < producer.totalRowGroups; i++ {
		rg := producer.reader.RowGroup(i)
		t.Logf("Row group %d: %d rows", i, rg.NumRows())
	}

	// Print schema
	for i := 0; i < producer.numColumns; i++ {
		col := producer.reader.MetaData().Schema.Column(i)
		t.Logf("Column %d: %s (type: %s, logical: %v, converted: %v)", i, col.Name(), col.PhysicalType(), col.LogicalType(), col.ConvertedType())
	}

	// Read all rows and collect the last 5
	lastRows := make([][]interface{}, 0, 5)
	rowCount := int64(0)

	for producer.Scan() {
		row, err := producer.Row()
		require.NoError(t, err)

		rowData := viewToRow(row)

		// Keep only last 5 rows
		if len(lastRows) >= 5 {
			lastRows = lastRows[1:]
		}
		lastRows = append(lastRows, rowData)
		rowCount++
	}

	require.NoError(t, producer.Err())
	t.Logf("Total rows read: %d", rowCount)

	// Print the last 5 rows with detailed Age/Fare info
	t.Logf("\nLast %d rows:", len(lastRows))
	for i, rowData := range lastRows {
		rowNum := rowCount - int64(len(lastRows)) + int64(i)

		// Specifically check columns 5 (Age) and 9 (Fare) if they exist
		if producer.numColumns > 9 {
			passengerId := rowData[0]
			age := rowData[5]
			fare := rowData[9]
			t.Logf("Row %d: PassengerId=%v, Age=%v, Fare=%v", rowNum, passengerId, age, fare)
		} else {
			t.Logf("Row %d: %v", rowNum, rowData)
		}
	}

	// Expected values based on CSV:
	// Row 890 (PassengerId 891): Age should be 32.0, Fare should be 7.75
	t.Logf("\nExpected for Row 890: Age=32.0, Fare=7.75")
	if len(lastRows) > 0 {
		lastRow := lastRows[len(lastRows)-1]
		age := lastRow[5]
		fare := lastRow[9]
		if age != nil {
			ageVal := age.(float64)
			fareVal := fare.(float64)
			t.Logf("Actual:   Age=%.2f, Fare=%.2f", ageVal, fareVal)
			if ageVal == 7.75 {
				t.Errorf("BUG DETECTED: Age shows Fare value (7.75 instead of 32.0)")
			}
		}
	}
}

// TestParquetRowView tests the parquetRowView structure
func TestParquetRowView(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Create a simple test case with two columns
	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int32Builder)
	nameBuilder := builder.Field(1).(*array.StringBuilder)

	idBuilder.Append(1)
	nameBuilder.Append("Alice")

	idBuilder.Append(2)
	nameBuilder.AppendNull()

	record := builder.NewRecord()
	defer record.Release()

	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	reader := bytes.NewReader(buf.Bytes())
	fr := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(buf.Len()),
	}

	parquetReader, err := file.NewParquetReader(fr)
	require.NoError(t, err)

	rowGroup := parquetReader.RowGroup(0)

	buffers := newParquetBatchBuffers(2)

	// Read both columns
	colReader0, err := rowGroup.Column(0)
	require.NoError(t, err)
	batch0, err := newParquetColumnBatch(colReader0, buffers, 2)
	require.NoError(t, err)
	require.NoError(t, batch0.Read())

	colReader1, err := rowGroup.Column(1)
	require.NoError(t, err)
	batch1, err := newParquetColumnBatch(colReader1, buffers, 2)
	require.NoError(t, err)
	require.NoError(t, batch1.Read())

	// Create views for each row
	batches := []*parquetColumnBatch{batch0, batch1}

	// View for row 0
	view0 := &parquetRowView{
		batches:    batches,
		numColumns: 2,
		rowIndex:   0,
	}

	require.False(t, view0.batches[0].isNull[view0.rowIndex])
	require.Equal(t, int32(1), view0.batches[0].int32Values[view0.rowIndex])

	require.False(t, view0.batches[1].isNull[view0.rowIndex])
	require.Equal(t, []byte("Alice"), view0.batches[1].byteArrayValues[view0.rowIndex])

	// View for row 1
	view1 := &parquetRowView{
		batches:    batches,
		numColumns: 2,
		rowIndex:   1,
	}

	require.False(t, view1.batches[0].isNull[view1.rowIndex])
	require.Equal(t, int32(2), view1.batches[0].int32Values[view1.rowIndex])

	require.True(t, view1.batches[1].isNull[view1.rowIndex])
}

// TestParquetPlainPrimitiveTypes verifies that Parquet files with plain primitive
// types (no LogicalType or ConvertedType annotations) are handled correctly.
func TestParquetPlainPrimitiveTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Open alltypes_plain.parquet which should have plain primitive types
	testFile := "testdata/parquet/alltypes_plain.parquet"
	f, err := os.Open(testFile)
	require.NoError(t, err)
	defer f.Close()

	stat, err := f.Stat()
	require.NoError(t, err)

	fr := &fileReader{
		Reader:   f,
		ReaderAt: f,
		Seeker:   f,
		total:    stat.Size(),
	}

	// Create producer (this will call newParquetRowProducer)
	producer, err := newParquetRowProducer(fr, nil)
	require.NoError(t, err)
	require.NotNil(t, producer)

	// Check the metadata to verify we're testing plain types
	parquetReader, err := file.NewParquetReader(fr)
	require.NoError(t, err)
	defer func() { _ = parquetReader.Close() }()

	parquetSchema := parquetReader.MetaData().Schema
	foundPlainType := false

	t.Log("Column annotations:")
	for i := 0; i < parquetSchema.NumColumns(); i++ {
		col := parquetSchema.Column(i)
		logicalType := col.LogicalType()
		convertedType := col.ConvertedType()

		t.Logf("  %s: LogicalType=%v (nil=%v), ConvertedType=%v",
			col.Name(), logicalType, logicalType == nil, convertedType)

		// Check if this is a plain type (either no logical type, or UnknownLogicalType)
		// Plain types have no semantic annotations, just physical type
		isPlainType := false
		if logicalType == nil {
			isPlainType = true
		} else {
			// Check for UnknownLogicalType which represents plain types
			// Also check string representation for "None"
			switch logicalType.(type) {
			case schema.UnknownLogicalType:
				isPlainType = true
			default:
				// Check if string representation is "None"
				if logicalType.String() == "None" {
					isPlainType = true
				}
			}
		}

		if isPlainType && convertedType == schema.ConvertedTypes.None {
			foundPlainType = true
		}
	}

	require.True(t, foundPlainType,
		"alltypes_plain.parquet should have at least one column with no type annotations")

	// Verify we can read rows (this tests that conversion works with nil logicalType)
	rowCount := 0
	for producer.Scan() && rowCount < 5 {
		row, err := producer.Row()
		require.NoError(t, err)
		require.NotNil(t, row)
		rowCount++
	}

	require.NoError(t, producer.Err())
	require.Greater(t, rowCount, 0, "should read at least some rows")

	t.Logf("Successfully read %d rows with plain primitive types", rowCount)
}

// TestParquetPlainPrimitivesValidation verifies that plain primitive types
// (no LogicalType/ConvertedType) pass schema validation when creating a consumer.
// This tests the validateAndBuildColumnMapping → validateWithLogicalType path.
func TestParquetPlainPrimitivesValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testFile := "testdata/parquet/alltypes_plain.parquet"
	f, err := os.Open(testFile)
	require.NoError(t, err)
	defer f.Close()

	stat, err := f.Stat()
	require.NoError(t, err)

	fr := &fileReader{
		Reader:   f,
		ReaderAt: f,
		Seeker:   f,
		total:    stat.Size(),
	}

	// Create producer
	producer, err := newParquetRowProducer(fr, nil)
	require.NoError(t, err)
	require.NotNil(t, producer)

	// Create a mock table descriptor with columns matching the Parquet file
	// alltypes_plain.parquet has columns: id, bool_col, tinyint_col, smallint_col,
	// int_col, bigint_col, float_col, double_col, date_string_col, string_col, timestamp_col
	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		Name: "test_table",
		Columns: []descpb.ColumnDescriptor{
			{Name: "id", ID: 1, Type: types.Int},
			{Name: "bool_col", ID: 2, Type: types.Bool},
			{Name: "tinyint_col", ID: 3, Type: types.Int},
			{Name: "smallint_col", ID: 4, Type: types.Int},
			{Name: "int_col", ID: 5, Type: types.Int},
			{Name: "bigint_col", ID: 6, Type: types.Int},
			{Name: "float_col", ID: 7, Type: types.Float},
			{Name: "double_col", ID: 8, Type: types.Float},
			{Name: "date_string_col", ID: 9, Type: types.String},
			{Name: "string_col", ID: 10, Type: types.String},
			{Name: "timestamp_col", ID: 11, Type: types.TimestampTZ}, // INT96 timestamp
		},
		Families: []descpb.ColumnFamilyDescriptor{{
			Name:        "primary",
			ColumnNames: []string{"id", "bool_col", "tinyint_col", "smallint_col", "int_col", "bigint_col", "float_col", "double_col", "date_string_col", "string_col", "timestamp_col"},
			ColumnIDs:   []descpb.ColumnID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
		}},
		PrimaryIndex: descpb.IndexDescriptor{
			Name:                "primary",
			ID:                  1,
			Unique:              true,
			KeyColumnNames:      []string{"id"},
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
		},
	}).BuildImmutableTable()

	importCtx := &parallelImportContext{
		tableDesc:  tableDesc,
		targetCols: nil, // Use all columns
	}

	// This should NOT fail even though the Parquet file has plain primitive types (nil LogicalType)
	// The validation logic should handle nil LogicalType and validate based on physical type alone
	consumer, err := newParquetRowConsumer(importCtx, producer, nil, false)
	require.NoError(t, err, "validation should succeed for plain primitive types")
	require.NotNil(t, consumer)

	t.Log("Successfully validated plain primitive types through consumer creation")
}

// TestParquetListSchemaDetection verifies that detectListColumn correctly
// identifies valid LIST columns and rejects unsupported nesting.
func TestParquetListSchemaDetection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()

	t.Run("DetectsListColumn", func(t *testing.T) {
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		}, nil)
		builder := array.NewRecordBuilder(pool, arrowSchema)
		defer builder.Release()

		lb := builder.Field(0).(*array.ListBuilder)
		lb.Append(true)
		lb.ValueBuilder().(*array.StringBuilder).Append("a")

		record := builder.NewRecord()
		defer record.Release()

		buf := new(bytes.Buffer)
		writerProps := parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Uncompressed))
		writer, err := pqarrow.NewFileWriter(
			arrowSchema, buf, writerProps, pqarrow.DefaultWriterProps())
		require.NoError(t, err)
		require.NoError(t, writer.Write(record))
		require.NoError(t, writer.Close())

		reader := bytes.NewReader(buf.Bytes())
		fr := &fileReader{
			Reader: reader, ReaderAt: reader, Seeker: reader,
			total: int64(buf.Len()),
		}
		parquetReader, err := file.NewParquetReader(fr)
		require.NoError(t, err)

		pqSchema := parquetReader.MetaData().Schema
		listInfo, err := detectListColumn(pqSchema, 0)
		require.NoError(t, err)
		require.NotNil(t, listInfo)
		require.Equal(t, "tags", listInfo.columnName)
		require.True(t, listInfo.elementIsOptional)
		require.Equal(t, int16(3), listInfo.maxDefLevel)
		require.Equal(t, int16(3), listInfo.presentElementDefLevel)
		require.Equal(t, 1, listInfo.nestingDepth)
		require.Len(t, listInfo.levels, 1)
		require.Equal(t, int16(1), listInfo.levels[0].repLevel)
		require.Equal(t, int16(0), listInfo.levels[0].nullListDefLevel)
		require.Equal(t, int16(1), listInfo.levels[0].emptyListDefLevel)
	})

	t.Run("FlatColumnNotDetectedAsList", func(t *testing.T) {
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		}, nil)
		builder := array.NewRecordBuilder(pool, arrowSchema)
		defer builder.Release()

		builder.Field(0).(*array.Int32Builder).Append(1)

		record := builder.NewRecord()
		defer record.Release()

		buf := new(bytes.Buffer)
		writerProps := parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Uncompressed))
		writer, err := pqarrow.NewFileWriter(
			arrowSchema, buf, writerProps, pqarrow.DefaultWriterProps())
		require.NoError(t, err)
		require.NoError(t, writer.Write(record))
		require.NoError(t, writer.Close())

		reader := bytes.NewReader(buf.Bytes())
		fr := &fileReader{
			Reader: reader, ReaderAt: reader, Seeker: reader,
			total: int64(buf.Len()),
		}
		parquetReader, err := file.NewParquetReader(fr)
		require.NoError(t, err)

		pqSchema := parquetReader.MetaData().Schema
		listInfo, err := detectListColumn(pqSchema, 0)
		require.NoError(t, err)
		require.Nil(t, listInfo, "flat column should not be detected as LIST")
	})
}

// parquetRow is one row passed to makeParquetFile: one value per column, in
// schema order. Conventions:
//   - nil means the column is NULL (or the LIST itself is NULL).
//   - For a LIST<T> column, pass []any of element values. An empty []any{}
//     is an empty list (not NULL).
//   - Primitive columns require the exact Go type (int32 for Int32, string
//     for String, etc.) — see appendParquetValue for the supported set.
type parquetRow []any

// parquetFileOption configures non-default knobs of makeParquetFile.
type parquetFileOption func(*parquetFileOptions)

type parquetFileOptions struct {
	compression compress.Compression
}

// withParquetCompression overrides the default compression (Uncompressed).
func withParquetCompression(c compress.Compression) parquetFileOption {
	return func(o *parquetFileOptions) { o.compression = c }
}

// makeParquetFile assembles an in-memory parquet file from the given arrow
// schema plus a slice of rows, and returns it wrapped in a *fileReader ready
// to feed the importer. Defaults to uncompressed.
//
// TODO(sravotto): adopt this helper at the other ~15 inline
// pqarrow.NewFileWriter sites in this file and the sibling
// read_import_parquet_list_test.go / read_import_parquet_batch_test.go.
// Extend appendParquetValue and add options (max row group length, ...) as
// those call sites need them.
func makeParquetFile(
	t *testing.T, arrowSchema *arrow.Schema, rows []parquetRow, opts ...parquetFileOption,
) *fileReader {
	t.Helper()
	options := parquetFileOptions{compression: compress.Codecs.Uncompressed}
	for _, o := range opts {
		o(&options)
	}
	buf := new(bytes.Buffer)
	writer, err := pqarrow.NewFileWriter(
		arrowSchema, buf,
		parquet.NewWriterProperties(parquet.WithCompression(options.compression)),
		pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	builder := array.NewRecordBuilder(memory.NewGoAllocator(), arrowSchema)
	defer builder.Release()
	for rowIdx, row := range rows {
		require.Lenf(t, row, len(arrowSchema.Fields()),
			"row %d has %d values but schema has %d columns",
			rowIdx, len(row), len(arrowSchema.Fields()))
		for colIdx, val := range row {
			appendParquetValue(t, rowIdx, colIdx,
				arrowSchema.Field(colIdx).Type, builder.Field(colIdx), val)
		}
	}
	record := builder.NewRecord()
	defer record.Release()
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	reader := bytes.NewReader(buf.Bytes())
	return &fileReader{
		Reader: reader, ReaderAt: reader, Seeker: reader,
		total: int64(buf.Len()),
	}
}

// appendParquetValue appends one Go value to the arrow builder for the given
// column type, applying the parquetRow conventions documented on parquetRow.
// rowIdx and colIdx are only used for error messages.
func appendParquetValue(
	t *testing.T, rowIdx, colIdx int, dt arrow.DataType, b array.Builder, val any,
) {
	t.Helper()
	if val == nil {
		b.AppendNull()
		return
	}
	switch dt := dt.(type) {
	case *arrow.Int32Type:
		v, ok := val.(int32)
		require.Truef(t, ok, "row %d col %d: expected int32, got %T", rowIdx, colIdx, val)
		b.(*array.Int32Builder).Append(v)
	case *arrow.Float64Type:
		v, ok := val.(float64)
		require.Truef(t, ok, "row %d col %d: expected float64, got %T", rowIdx, colIdx, val)
		b.(*array.Float64Builder).Append(v)
	case *arrow.BooleanType:
		v, ok := val.(bool)
		require.Truef(t, ok, "row %d col %d: expected bool, got %T", rowIdx, colIdx, val)
		b.(*array.BooleanBuilder).Append(v)
	case *arrow.StringType:
		v, ok := val.(string)
		require.Truef(t, ok, "row %d col %d: expected string, got %T", rowIdx, colIdx, val)
		b.(*array.StringBuilder).Append(v)
	case *arrow.ListType:
		elems, ok := val.([]any)
		require.Truef(t, ok, "row %d col %d: expected []any for LIST, got %T", rowIdx, colIdx, val)
		lb := b.(*array.ListBuilder)
		lb.Append(true)
		for _, elem := range elems {
			appendParquetValue(t, rowIdx, colIdx, dt.Elem(), lb.ValueBuilder(), elem)
		}
	default:
		t.Fatalf("row %d col %d: unsupported arrow type %T (extend appendParquetValue)",
			rowIdx, colIdx, dt)
	}
}

// TestParquetListEndToEnd drives the full producer/consumer pipeline for LIST
// columns: it scans every row, calls FillDatums, and asserts that the assembled
// datums match what the Parquet file encodes for both ARRAY and JSONB targets.
func TestParquetListEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	defer row.TestingSetDatumRowConverterBatchSize(2)()

	// jsonArr builds a JSONB array datum from primitive Go values, mirroring
	// the elements assembled by parquetElementToJSON.
	jsonArr := func(t *testing.T, elems ...any) tree.Datum {
		t.Helper()
		ab := json.NewArrayBuilder(len(elems))
		for _, e := range elems {
			if e == nil {
				ab.Add(json.NullJSONValue)
				continue
			}
			switch v := e.(type) {
			case string:
				ab.Add(json.FromString(v))
			default:
				t.Fatalf("unsupported jsonArr element type %T", e)
			}
		}
		return tree.NewDJSON(ab.Build())
	}

	// stringArr builds a tree.DArray of strings (with optional nulls).
	stringArr := func(t *testing.T, elems ...any) tree.Datum {
		t.Helper()
		arr := tree.NewDArray(types.String)
		for _, e := range elems {
			if e == nil {
				require.NoError(t, arr.Append(tree.DNull))
				continue
			}
			require.NoError(t, arr.Append(tree.NewDString(e.(string))))
		}
		return arr
	}

	cases := []struct {
		name        string
		tagsColType *types.T
		// expectedTags is keyed by id (1..4) — the value the consumer should
		// produce for the "tags" column on that row.
		expectedTags func(t *testing.T) map[int]tree.Datum
	}{
		{
			name:        "ListToArray",
			tagsColType: types.MakeArray(types.String),
			expectedTags: func(t *testing.T) map[int]tree.Datum {
				return map[int]tree.Datum{
					1: stringArr(t, "a", "b"),
					2: tree.DNull,
					3: stringArr(t),
					4: stringArr(t, "x"),
				}
			},
		},
		{
			name:        "ListToJSON",
			tagsColType: types.Jsonb,
			expectedTags: func(t *testing.T) map[int]tree.Datum {
				return map[int]tree.Datum{
					1: jsonArr(t, "a", "b"),
					2: tree.DNull,
					3: jsonArr(t),
					4: jsonArr(t, "x"),
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fr := makeParquetFile(t, arrow.NewSchema([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
				{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
			}, nil), []parquetRow{
				{int32(1), []any{"a", "b"}},
				{int32(2), nil},
				{int32(3), []any{}},
				{int32(4), []any{"x"}},
			})

			tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
				ID:   1,
				Name: "test_table",
				Columns: []descpb.ColumnDescriptor{
					{ID: 1, Name: "id", Type: types.Int4},
					{ID: 2, Name: "tags", Type: tc.tagsColType, Nullable: true},
				},
				PrimaryIndex: descpb.IndexDescriptor{
					ID: 1, Name: "pk",
					KeyColumnIDs:        []descpb.ColumnID{1},
					KeyColumnNames:      []string{"id"},
					KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				},
			}).BuildImmutableTable()
			targetCols := tree.NameList{"id", "tags"}
			importCtx := &parallelImportContext{
				tableDesc:  tableDesc,
				targetCols: targetCols,
			}

			producer, err := newParquetRowProducer(fr, importCtx)
			require.NoError(t, err)
			consumer, err := newParquetRowConsumer(
				importCtx, producer, &importFileContext{}, false)
			require.NoError(t, err)

			conv, err := row.NewDatumRowConverter(
				ctx, &semaCtx, tableDesc, targetCols, evalCtx.Copy(),
				nil /* kvCh */, nil /* seqChunkProvider */, nil /* metrics */, nil, /* db */
			)
			require.NoError(t, err)

			expected := tc.expectedTags(t)
			seen := 0
			for producer.Scan() {
				rowData, err := producer.Row()
				require.NoError(t, err)
				require.NoError(t, consumer.FillDatums(ctx, rowData, int64(seen), conv))

				require.Len(t, conv.Datums, 2)
				id := int(tree.MustBeDInt(conv.Datums[0]))
				want, ok := expected[id]
				require.True(t, ok, "unexpected id %d", id)
				require.Equalf(t, want, conv.Datums[1], "tags datum for id=%d", id)
				seen++
			}
			require.NoError(t, producer.Err())
			require.Equal(t, len(expected), seen)
		})
	}
}

// TestParquetListOmittedFromTargets verifies that a parquet file containing a
// LIST column can still be imported when that LIST column is not in the
// IMPORT target list. The producer must open the file, skip the LIST column,
// and yield the remaining columns.
func TestParquetListOmittedFromTargets(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	defer row.TestingSetDatumRowConverterBatchSize(2)()

	fr := makeParquetFile(t, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
	}, nil), []parquetRow{
		{int32(1), []any{"a", "b"}},
		{int32(2), nil},
		{int32(3), []any{}},
		{int32(4), []any{"x"}},
	})

	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:   1,
		Name: "test_table",
		Columns: []descpb.ColumnDescriptor{
			{ID: 1, Name: "id", Type: types.Int4},
			{ID: 2, Name: "tags", Type: types.MakeArray(types.String), Nullable: true},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			ID: 1, Name: "pk",
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnNames:      []string{"id"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
		},
	}).BuildImmutableTable()
	// IMPORT INTO t(id) — the user explicitly omits the tags column.
	targetCols := tree.NameList{"id"}
	importCtx := &parallelImportContext{
		tableDesc:  tableDesc,
		targetCols: targetCols,
	}

	producer, err := newParquetRowProducer(fr, importCtx)
	require.NoError(t, err)
	consumer, err := newParquetRowConsumer(
		importCtx, producer, &importFileContext{}, false)
	require.NoError(t, err)

	conv, err := row.NewDatumRowConverter(
		ctx, &semaCtx, tableDesc, targetCols, evalCtx.Copy(),
		nil /* kvCh */, nil /* seqChunkProvider */, nil /* metrics */, nil, /* db */
	)
	require.NoError(t, err)

	expectedIDs := []int{1, 2, 3, 4}
	seen := 0
	for producer.Scan() {
		rowData, err := producer.Row()
		require.NoError(t, err)
		require.NoError(t, consumer.FillDatums(ctx, rowData, int64(seen), conv))

		// The converter still allocates a slot for every visible column, but
		// "tags" was not in targetCols and therefore stays at its default.
		require.Equal(t, expectedIDs[seen], int(tree.MustBeDInt(conv.Datums[0])))
		seen++
	}
	require.NoError(t, producer.Err())
	require.Equal(t, len(expectedIDs), seen)
}

// TestParquetUnsupportedColumnDeferred verifies that detectListColumn errors
// for unsupported parquet structures (nested LIST, MAP) are deferred until
// the user actually imports the offending column. Otherwise, opening a
// parquet file that contains any unsupported column would fail regardless
// of which columns the user asked for.
func TestParquetUnsupportedColumnDeferred(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const fieldID = -1

	// Build a parquet schema with one importable flat column ("id") and one
	// unsupported nested LIST column ("nested"). We can't easily produce a
	// real LIST<LIST<T>> parquet file with the high-level arrow writer, so
	// we exercise determineColumnsToRead directly: it is the function that
	// decides which columns to read and is where the deferred error must
	// surface.
	idLeaf, err := schema.NewPrimitiveNode(
		"id", parquet.Repetitions.Required,
		parquet.Types.Int32, -1, fieldID)
	require.NoError(t, err)

	innerLeaf, err := schema.NewPrimitiveNode(
		"element", parquet.Repetitions.Optional,
		parquet.Types.Int32, -1, fieldID)
	require.NoError(t, err)
	innerRepeated, err := schema.NewGroupNode(
		"list", parquet.Repetitions.Repeated,
		[]schema.Node{innerLeaf}, fieldID)
	require.NoError(t, err)
	innerList, err := schema.NewGroupNodeLogical(
		"element", parquet.Repetitions.Optional,
		[]schema.Node{innerRepeated},
		schema.ListLogicalType{}, fieldID)
	require.NoError(t, err)
	outerRepeated, err := schema.NewGroupNode(
		"list", parquet.Repetitions.Repeated,
		[]schema.Node{innerList}, fieldID)
	require.NoError(t, err)
	nestedList, err := schema.NewGroupNodeLogical(
		"nested_list", parquet.Repetitions.Optional,
		[]schema.Node{outerRepeated},
		schema.ListLogicalType{}, fieldID)
	require.NoError(t, err)

	root, err := schema.NewGroupNode(
		"schema", parquet.Repetitions.Required,
		[]schema.Node{idLeaf, nestedList}, fieldID)
	require.NoError(t, err)
	pqSchema := schema.NewSchema(root)

	// Mirror what newParquetRowProducer does: build columnMetadata for every
	// leaf, deferring detectListColumn errors. The leaf order matches the
	// schema's column layout: 0=id, 1=nested.element.
	columnMetadata := make(map[int]*parquetColumnMetadata)
	for colIdx := range pqSchema.NumColumns() {
		col := pqSchema.Column(colIdx)
		name := pqSchema.ColumnRoot(colIdx).Name()
		listInfo, detectErr := detectListColumn(pqSchema, colIdx)
		switch {
		case detectErr != nil:
			columnMetadata[colIdx] = &parquetColumnMetadata{
				columnName:   name,
				detectionErr: detectErr,
			}
		case listInfo != nil:
			columnMetadata[colIdx] = &parquetColumnMetadata{
				columnName: name,
				isList:     true,
				listInfo:   listInfo,
			}
		default:
			columnMetadata[colIdx] = &parquetColumnMetadata{
				columnName:    name,
				logicalType:   deriveLogicalType(col),
				convertedType: col.ConvertedType(),
			}
		}
	}
	require.Error(t, columnMetadata[1].detectionErr,
		"nested LIST should produce a deferred error in metadata")

	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:   1,
		Name: "t",
		Columns: []descpb.ColumnDescriptor{
			{ID: 1, Name: "id", Type: types.Int4},
			{ID: 2, Name: "nested_list", Type: types.MakeArray(types.MakeArray(types.Int4)), Nullable: true},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			ID: 1, Name: "pk",
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnNames:      []string{"id"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
		},
	}).BuildImmutableTable()

	t.Run("OmittingUnsupportedColumnSucceeds", func(t *testing.T) {
		importCtx := &parallelImportContext{
			tableDesc:  tableDesc,
			targetCols: tree.NameList{"id"},
		}
		cols, err := determineColumnsToRead(importCtx, pqSchema, columnMetadata)
		require.NoError(t, err)
		require.Equal(t, []int{0}, cols)
	})

	t.Run("ImportingUnsupportedColumnFails", func(t *testing.T) {
		importCtx := &parallelImportContext{
			tableDesc:  tableDesc,
			targetCols: tree.NameList{"id", "nested_list"},
		}
		_, err := determineColumnsToRead(importCtx, pqSchema, columnMetadata)
		require.Error(t, err)
		require.ErrorContains(t, err, `column "nested_list"`)
		require.ErrorContains(t, err, "nested LIST")
	})
}

// TestParquetMultipleListColumns drives the full producer/consumer pipeline
// against a file with two LIST columns. Each LIST is read by its own stateful
// reader, and we verify both columns produce the expected per-row datums.
func TestParquetMultipleListColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := eval.MakeTestingEvalContext(st)
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	defer row.TestingSetDatumRowConverterBatchSize(2)()

	stringArr := func(t *testing.T, elems ...string) tree.Datum {
		t.Helper()
		arr := tree.NewDArray(types.String)
		for _, e := range elems {
			require.NoError(t, arr.Append(tree.NewDString(e)))
		}
		return arr
	}
	intArr := func(t *testing.T, elems ...int) tree.Datum {
		t.Helper()
		arr := tree.NewDArray(types.Int4)
		for _, e := range elems {
			require.NoError(t, arr.Append(tree.NewDInt(tree.DInt(e))))
		}
		return arr
	}

	type expected struct {
		tags   tree.Datum
		scores tree.Datum
	}
	want := map[int]expected{
		1: {tags: stringArr(t, "a", "b"), scores: intArr(t, 10, 20, 30)},
		2: {tags: tree.DNull, scores: intArr(t)},
		3: {tags: stringArr(t), scores: tree.DNull},
		4: {tags: stringArr(t, "x"), scores: intArr(t, 99)},
	}

	fr := makeParquetFile(t, arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
		{Name: "scores", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
	}, nil), []parquetRow{
		{int32(1), []any{"a", "b"}, []any{int32(10), int32(20), int32(30)}},
		{int32(2), nil, []any{}},
		{int32(3), []any{}, nil},
		{int32(4), []any{"x"}, []any{int32(99)}},
	})

	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
		ID:   1,
		Name: "test_table",
		Columns: []descpb.ColumnDescriptor{
			{ID: 1, Name: "id", Type: types.Int4},
			{ID: 2, Name: "tags", Type: types.MakeArray(types.String), Nullable: true},
			{ID: 3, Name: "scores", Type: types.MakeArray(types.Int4), Nullable: true},
		},
		PrimaryIndex: descpb.IndexDescriptor{
			ID: 1, Name: "pk",
			KeyColumnIDs:        []descpb.ColumnID{1},
			KeyColumnNames:      []string{"id"},
			KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
		},
	}).BuildImmutableTable()
	targetCols := tree.NameList{"id", "tags", "scores"}
	importCtx := &parallelImportContext{
		tableDesc:  tableDesc,
		targetCols: targetCols,
	}

	producer, err := newParquetRowProducer(fr, importCtx)
	require.NoError(t, err)
	consumer, err := newParquetRowConsumer(
		importCtx, producer, &importFileContext{}, false)
	require.NoError(t, err)

	conv, err := row.NewDatumRowConverter(
		ctx, &semaCtx, tableDesc, targetCols, evalCtx.Copy(),
		nil /* kvCh */, nil /* seqChunkProvider */, nil /* metrics */, nil, /* db */
	)
	require.NoError(t, err)

	seen := 0
	for producer.Scan() {
		rowData, err := producer.Row()
		require.NoError(t, err)
		require.NoError(t, consumer.FillDatums(ctx, rowData, int64(seen), conv))

		require.Len(t, conv.Datums, 3)
		id := int(tree.MustBeDInt(conv.Datums[0]))
		exp, ok := want[id]
		require.True(t, ok, "unexpected id %d", id)
		require.Equalf(t, exp.tags, conv.Datums[1], "tags datum for id=%d", id)
		require.Equalf(t, exp.scores, conv.Datums[2], "scores datum for id=%d", id)
		seen++
	}
	require.NoError(t, producer.Err())
	require.Equal(t, len(want), seen)
}

// TestParquetListMultiBatch verifies that LIST column data is correctly
// preserved across multiple fillBuffer calls, ensuring level entries
// consumed by a chunk read but beyond the batch boundary are not
// discarded.
func TestParquetListMultiBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()

	// Create a file with 300 rows (batch size is 100, so 3 fillBuffer calls).
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "scores", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(pool, arrowSchema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int32Builder)
	lb := builder.Field(1).(*array.ListBuilder)
	vb := lb.ValueBuilder().(*array.Int32Builder)

	numRows := 300
	for i := range numRows {
		idBuilder.Append(int32(i))
		if i%10 == 0 {
			// Every 10th row: null list.
			lb.AppendNull()
		} else if i%10 == 1 {
			// Every 10th+1 row: empty list.
			lb.Append(true)
		} else {
			// Other rows: [i*100, i*100+1, i*100+2].
			lb.Append(true)
			vb.Append(int32(i * 100))
			vb.Append(int32(i*100 + 1))
			vb.Append(int32(i*100 + 2))
		}
	}

	record := builder.NewRecord()
	defer record.Release()

	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Uncompressed))
	writer, err := pqarrow.NewFileWriter(
		arrowSchema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	reader := bytes.NewReader(buf.Bytes())
	fr := &fileReader{
		Reader: reader, ReaderAt: reader, Seeker: reader,
		total: int64(buf.Len()),
	}

	// Read through the full producer pipeline (no importCtx = test mode).
	producer, err := newParquetRowProducer(fr, nil)
	require.NoError(t, err)

	rowIdx := 0
	for producer.Scan() {
		rowData, err := producer.Row()
		require.NoError(t, err)

		view := rowData.(*parquetRowView)
		// Find the LIST column batch (column with isList=true).
		for colIdx := range view.numColumns {
			batch := view.batches[colIdx]
			if batch == nil || !batch.isList {
				continue
			}

			val, isNull, err := batch.GetValueAt(int(view.rowIndex))
			require.NoError(t, err, "row %d", rowIdx)

			if rowIdx%10 == 0 {
				// Null list.
				require.True(t, isNull, "row %d should be null", rowIdx)
			} else if rowIdx%10 == 1 {
				// Empty list.
				require.False(t, isNull, "row %d should not be null", rowIdx)
				elements := val.([]any)
				require.Len(t, elements, 0, "row %d should be empty list", rowIdx)
			} else {
				// 3-element list.
				require.False(t, isNull, "row %d should not be null", rowIdx)
				elements := val.([]any)
				require.Len(t, elements, 3, "row %d", rowIdx)
				require.Equal(t, int32(rowIdx*100), elements[0], "row %d elem 0", rowIdx)
				require.Equal(t, int32(rowIdx*100+1), elements[1], "row %d elem 1", rowIdx)
				require.Equal(t, int32(rowIdx*100+2), elements[2], "row %d elem 2", rowIdx)
			}
		}
		rowIdx++
	}
	require.NoError(t, producer.Err())
	require.Equal(t, numRows, rowIdx, "should read all rows")
}

// TestParquetListAcrossRowGroups verifies that the producer correctly resets
// LIST reader state on a row-group boundary. The test forces multiple row
// groups via WithMaxRowGroupLength and drives the full Scan/Row pipeline.
func TestParquetListAcrossRowGroups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "vals", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(pool, arrowSchema)
	defer builder.Release()
	idBuilder := builder.Field(0).(*array.Int32Builder)
	lb := builder.Field(1).(*array.ListBuilder)
	vb := lb.ValueBuilder().(*array.Int32Builder)

	const numRows = 25
	for i := range numRows {
		idBuilder.Append(int32(i))
		lb.Append(true)
		vb.Append(int32(i))
		vb.Append(int32(i + 1))
	}
	record := builder.NewRecord()
	defer record.Release()

	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(
		parquet.WithCompression(compress.Codecs.Uncompressed),
		parquet.WithMaxRowGroupLength(5),
	)
	writer, err := pqarrow.NewFileWriter(
		arrowSchema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())

	reader := bytes.NewReader(buf.Bytes())
	fr := &fileReader{
		Reader: reader, ReaderAt: reader, Seeker: reader,
		total: int64(buf.Len()),
	}

	producer, err := newParquetRowProducer(fr, nil)
	require.NoError(t, err)
	require.Greater(t, producer.totalRowGroups, 1,
		"test fixture should produce multiple row groups")

	rowIdx := 0
	for producer.Scan() {
		rowData, err := producer.Row()
		require.NoError(t, err)
		view := rowData.(*parquetRowView)
		for colIdx := range view.numColumns {
			batch := view.batches[colIdx]
			if batch == nil || !batch.isList {
				continue
			}
			val, isNull, err := batch.GetValueAt(int(view.rowIndex))
			require.NoError(t, err, "row %d", rowIdx)
			require.False(t, isNull, "row %d should not be null", rowIdx)
			elements := val.([]any)
			require.Len(t, elements, 2, "row %d", rowIdx)
			require.Equal(t, int32(rowIdx), elements[0], "row %d elem 0", rowIdx)
			require.Equal(t, int32(rowIdx+1), elements[1], "row %d elem 1", rowIdx)
		}
		rowIdx++
	}
	require.NoError(t, producer.Err())
	require.Equal(t, numRows, rowIdx)
}

// TestParquetElementToJSON pins the JSON output of parquetElementToJSON for
// every supported physical type and the recognized logical-type annotations.
// The assertions are coupled to tree.AsJSON's per-datum formatting, so a
// change to that formatter (e.g. number layout, bytea encoding) will surface
// here.
func TestParquetElementToJSON(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	plainMeta := func() *parquetColumnMetadata { return &parquetColumnMetadata{} }
	withLogical := func(lt schema.LogicalType) *parquetColumnMetadata {
		return &parquetColumnMetadata{logicalType: lt}
	}
	uuidBytes := []byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0,
		0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}
	// Parquet INTERVAL: 12 bytes = uint32 months + uint32 days + uint32 millis,
	// each little-endian. Pin to 1 month, 2 days, 3000 ms.
	intervalBytes := []byte{
		0x01, 0x00, 0x00, 0x00,
		0x02, 0x00, 0x00, 0x00,
		0xb8, 0x0b, 0x00, 0x00,
	}

	tests := []struct {
		name        string
		elem        any
		meta        *parquetColumnMetadata
		expectedStr string
	}{
		{name: "bool", elem: true, meta: plainMeta(), expectedStr: "true"},
		{name: "int32", elem: int32(7), meta: plainMeta(), expectedStr: "7"},
		{name: "int64", elem: int64(123456789012), meta: plainMeta(), expectedStr: "123456789012"},
		{name: "float32", elem: float32(1.5), meta: plainMeta(), expectedStr: "1.5"},
		{name: "float64", elem: 2.25, meta: plainMeta(), expectedStr: "2.25"},
		{
			name: "byte array no logical type",
			elem: []byte("hello"), meta: plainMeta(),
			expectedStr: `"hello"`,
		},
		{
			name:        "byte array string logical type",
			elem:        []byte("world"),
			meta:        withLogical(schema.StringLogicalType{}),
			expectedStr: `"world"`,
		},
		{
			name:        "byte array enum logical type",
			elem:        []byte("RED"),
			meta:        withLogical(schema.EnumLogicalType{}),
			expectedStr: `"RED"`,
		},
		{
			name:        "byte array JSON logical type",
			elem:        []byte(`{"a":1}`),
			meta:        withLogical(schema.JSONLogicalType{}),
			expectedStr: `{"a": 1}`,
		},
		{
			name: "int32 date logical type",
			// Days since 1970-01-01: 2023-02-01 = 19389.
			elem:        int32(19389),
			meta:        withLogical(schema.DateLogicalType{}),
			expectedStr: `"2023-02-01"`,
		},
		{
			name:        "fixed len byte array UUID logical type",
			elem:        parquet.FixedLenByteArray(uuidBytes),
			meta:        withLogical(schema.UUIDLogicalType{}),
			expectedStr: `"12345678-9abc-def0-1234-56789abcdef0"`,
		},
		{
			name:        "fixed len byte array interval logical type",
			elem:        parquet.FixedLenByteArray(intervalBytes),
			meta:        withLogical(schema.IntervalLogicalType{}),
			expectedStr: `"1 mon 2 days 00:00:03"`,
		},
		{
			name: "fixed len byte array no logical type",
			// Unannotated FLBA falls through to types.Bytes, producing the
			// standard "\x..." hex encoding used by CRDB's bytea_output=hex.
			elem:        parquet.FixedLenByteArray([]byte{0xde, 0xad, 0xbe, 0xef}),
			meta:        plainMeta(),
			expectedStr: `"\\xdeadbeef"`,
		},
		{
			name: "int96 timestamp",
			// INT96 = nanos within day (8 bytes, little-endian) + Julian day
			// number (4 bytes, little-endian). 2451545 = 0x256859 is the JD
			// for 2000-01-01.
			elem:        parquet.Int96{0, 0, 0, 0, 0, 0, 0, 0, 0x59, 0x68, 0x25, 0x00},
			meta:        plainMeta(),
			expectedStr: `"2000-01-01T00:00:00Z"`,
		},
		{
			name: "int32 decimal non-zero scale",
			// 12345 with scale 2 = 123.45.
			elem:        int32(12345),
			meta:        withLogical(schema.NewDecimalLogicalType(5, 2)),
			expectedStr: `123.45`,
		},
		{
			name: "int64 timestamp micros",
			// 1675209600000000 micros = 2023-02-01T00:00:00Z.
			elem:        int64(1675209600000000),
			meta:        withLogical(schema.NewTimestampLogicalType(true, schema.TimeUnitMicros)),
			expectedStr: `"2023-02-01T00:00:00Z"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			j, err := parquetElementToJSON(tc.elem, tc.meta)
			require.NoError(t, err)
			require.Equal(t, tc.expectedStr, j.String())
		})
	}

	t.Run("unsupported type fails assertion", func(t *testing.T) {
		_, err := parquetElementToJSON(struct{}{}, plainMeta())
		require.ErrorContains(t, err, "unsupported element type")
		require.True(t, errors.HasAssertionFailure(err),
			"unsupported element type should be reported as an assertion failure")
	})
}

// TestAssembleListDatum covers assembleListDatum directly: interleaved NULL
// elements for both ARRAY and JSONB targets, and the per-element error wraps
// emitted when a conversion or append fails.
func TestAssembleListDatum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	plainMeta := &parquetColumnMetadata{}

	t.Run("array with null elements", func(t *testing.T) {
		got, err := assembleListDatum(
			[]any{[]byte("a"), nil, []byte("b")},
			types.MakeArray(types.String), plainMeta)
		require.NoError(t, err)

		want := tree.NewDArray(types.String)
		require.NoError(t, want.Append(tree.NewDString("a")))
		require.NoError(t, want.Append(tree.DNull))
		require.NoError(t, want.Append(tree.NewDString("b")))
		require.Equal(t, want, got)
	})

	t.Run("array empty list", func(t *testing.T) {
		got, err := assembleListDatum(
			[]any{}, types.MakeArray(types.Int), plainMeta)
		require.NoError(t, err)
		require.Equal(t, tree.NewDArray(types.Int), got)
	})

	t.Run("array string passes through invalid utf-8", func(t *testing.T) {
		// No UTF-8 validation on this path; matches the flat-path importer.
		got, err := assembleListDatum(
			[]any{[]byte{0xff}}, types.MakeArray(types.String), plainMeta)
		require.NoError(t, err)

		want := tree.NewDArray(types.String)
		require.NoError(t, want.Append(tree.NewDString("\xff")))
		require.Equal(t, want, got)
	})

	t.Run("array conversion error wraps with element index", func(t *testing.T) {
		// struct{} has no Parquet physical type, so convertWithLogicalType
		// rejects it; the wrap must name the offending element index.
		_, err := assembleListDatum(
			[]any{int32(1), struct{}{}},
			types.MakeArray(types.Int), plainMeta)
		require.Error(t, err)
		require.Contains(t, err.Error(), "converting LIST element 1")
		require.Contains(t, err.Error(), "unsupported Parquet value type")
	})

	t.Run("array append type mismatch wraps with element index", func(t *testing.T) {
		// []byte with an INT target produces a DString via the byte-array
		// fallback path; appending that DString to a DArray(Int) fails the
		// type check in arr.Append, which is the wrap we want to assert.
		_, err := assembleListDatum(
			[]any{int32(1), []byte("not-an-int")},
			types.MakeArray(types.Int), plainMeta)
		require.Error(t, err)
		require.Contains(t, err.Error(), "appending LIST element 1")
	})

	t.Run("json with null elements", func(t *testing.T) {
		got, err := assembleListDatum(
			[]any{[]byte("a"), nil, []byte("b")}, types.Jsonb, plainMeta)
		require.NoError(t, err)

		ab := json.NewArrayBuilder(3)
		ab.Add(json.FromString("a"))
		ab.Add(json.NullJSONValue)
		ab.Add(json.FromString("b"))
		require.Equal(t, tree.NewDJSON(ab.Build()), got)
	})

	t.Run("json empty list", func(t *testing.T) {
		got, err := assembleListDatum(
			[]any{}, types.Jsonb, plainMeta)
		require.NoError(t, err)
		require.Equal(t, tree.NewDJSON(json.NewArrayBuilder(0).Build()), got)
	})

	t.Run("json with mixed nil and empty bytes", func(t *testing.T) {
		// Asymmetric with the ARRAY<JSONB> case above: inside a JSONB list,
		// []byte{} becomes an empty JSON string rather than collapsing to
		// null.
		got, err := assembleListDatum(
			[]any{nil, []byte{}}, types.Jsonb, plainMeta)
		require.NoError(t, err)

		ab := json.NewArrayBuilder(2)
		ab.Add(json.NullJSONValue)
		ab.Add(json.FromString(""))
		require.Equal(t, tree.NewDJSON(ab.Build()), got)
	})

	t.Run("json passes through invalid utf-8", func(t *testing.T) {
		// Same as the ARRAY<STRING> case: the bytes ride through into the
		// JSON string unchanged.
		got, err := assembleListDatum(
			[]any{[]byte{0xff}}, types.Jsonb, plainMeta)
		require.NoError(t, err)

		ab := json.NewArrayBuilder(1)
		ab.Add(json.FromString("\xff"))
		require.Equal(t, tree.NewDJSON(ab.Build()), got)
	})

	t.Run("json conversion error wraps with element index", func(t *testing.T) {
		_, err := assembleListDatum(
			[]any{int32(1), struct{}{}}, types.Jsonb, plainMeta)
		require.Error(t, err)
		require.Contains(t, err.Error(), "converting LIST element 1 to JSON")
		require.Contains(t, err.Error(), "unsupported element type")
	})

	t.Run("unsupported target type fails assertion", func(t *testing.T) {
		// LIST targeting a scalar type should never make it past schema
		// validation; if it does, assembleListDatum trips an assertion.
		_, err := assembleListDatum([]any{int32(1)}, types.Int, plainMeta)
		require.ErrorContains(t, err, "LIST columns can only target ARRAY or JSONB")
		require.True(t, errors.HasAssertionFailure(err))
	})

	t.Run("non-slice value fails assertion", func(t *testing.T) {
		// The producer is contractually obligated to hand assembleListDatum a
		// []any; any other shape is a programmer error.
		_, err := assembleListDatum("not a slice", types.MakeArray(types.String), plainMeta)
		require.ErrorContains(t, err, "expected []any for LIST column")
		require.True(t, errors.HasAssertionFailure(err))
	})
}

// TestParquetListValidationRejects verifies that the LIST branch of
// validateAndBuildColumnMapping rejects non-ARRAY/JSONB targets and arrays
// whose element type is incompatible with the Parquet element's physical type,
// surfacing the error at file-open time rather than mid-import.
func TestParquetListValidationRejects(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Build a Parquet file with a single LIST<int32> column named "vals".
	pool := memory.NewGoAllocator()
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "vals", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
	}, nil)
	builder := array.NewRecordBuilder(pool, arrowSchema)
	defer builder.Release()
	lb := builder.Field(0).(*array.ListBuilder)
	vb := lb.ValueBuilder().(*array.Int32Builder)
	lb.Append(true)
	vb.Append(1)
	record := builder.NewRecord()
	defer record.Release()

	buf := new(bytes.Buffer)
	writer, err := pqarrow.NewFileWriter(
		arrowSchema, buf,
		parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed)),
		pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, writer.Write(record))
	require.NoError(t, writer.Close())
	parquetBytes := buf.Bytes()

	newImportCtxWithType := func(colType *types.T) *parallelImportContext {
		tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{
			Name: "t",
			Columns: []descpb.ColumnDescriptor{
				{ID: 1, Name: "id", Type: types.Int},
				{ID: 2, Name: "vals", Type: colType, Nullable: true},
			},
			NextColumnID: 3,
			PrimaryIndex: descpb.IndexDescriptor{
				ID: 1, Name: "pk",
				KeyColumnIDs:        []descpb.ColumnID{1},
				KeyColumnNames:      []string{"id"},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			},
		}).BuildImmutableTable()
		return &parallelImportContext{
			tableDesc:  tableDesc,
			targetCols: tree.NameList{"vals"},
		}
	}

	tests := []struct {
		name         string
		colType      *types.T
		expectedErr  string
		expectedHelp string // optional substring for the wrapping context
	}{
		{
			name:         "scalar int target",
			colType:      types.Int,
			expectedErr:  "Parquet LIST requires an ARRAY or JSONB target type",
			expectedHelp: `column "vals"`,
		},
		{
			name:         "string array target for int list",
			colType:      types.MakeArray(types.String),
			expectedErr:  "int32 type can only be converted to INT or DECIMAL, got StringFamily",
			expectedHelp: `column "vals" (LIST element type)`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reader := bytes.NewReader(parquetBytes)
			fr := &fileReader{
				Reader: reader, ReaderAt: reader, Seeker: reader,
				total: int64(len(parquetBytes)),
			}
			importCtx := newImportCtxWithType(tc.colType)
			producer, err := newParquetRowProducer(fr, importCtx)
			require.NoError(t, err)
			_, err = newParquetRowConsumer(importCtx, producer, &importFileContext{}, false)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedErr)
			if tc.expectedHelp != "" {
				require.Contains(t, err.Error(), tc.expectedHelp)
			}
		})
	}
}

// TestParquetConsumerListRouting verifies that parquetRowConsumer.FillDatums
// dispatches LIST batches through assembleListDatum and produces the expected
// DArray or DJSON datum, depending on the target column type. The producer
// half of the LIST pipeline is mocked by fabricating parquetColumnBatch
// values directly, so the test exercises the consumer routing in isolation.
func TestParquetConsumerListRouting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Three Parquet columns: id (flat int32), tags (LIST<string>),
	// scores (LIST<int32>). The first targets an INT column, the second an
	// ARRAY(STRING) column, the third a JSONB column. This shape covers
	// both LIST destinations plus the flat fallthrough.
	listMeta := func() *parquetColumnMetadata {
		return &parquetColumnMetadata{isList: true}
	}
	flatMeta := func() *parquetColumnMetadata { return &parquetColumnMetadata{} }
	consumer := &parquetRowConsumer{
		columnMetadata: map[int]*parquetColumnMetadata{
			0: flatMeta(),
			1: listMeta(),
			2: listMeta(),
		},
		colMapping: []int{0, 1, 2},
	}

	// Two rows. Row 0: id=10, tags=["a","b"], scores=[1,2]. Row 1: id=20,
	// tags=NULL, scores=[].
	idBatch := &parquetColumnBatch{
		physicalType: parquet.Types.Int32,
		rowCount:     2,
		isNull:       []bool{false, false},
		int32Values:  []int32{10, 20},
	}
	tagsBatch := &parquetColumnBatch{
		isList:   true,
		rowCount: 2,
		isNull:   []bool{false, true},
		listRowValues: [][]any{
			{[]byte("a"), []byte("b")},
			nil,
		},
	}
	scoresBatch := &parquetColumnBatch{
		isList:   true,
		rowCount: 2,
		isNull:   []bool{false, false},
		listRowValues: [][]any{
			{int32(1), int32(2)},
			{},
		},
	}

	conv := &row.DatumRowConverter{
		VisibleColTypes: []*types.T{
			types.Int,
			types.MakeArray(types.String),
			types.Jsonb,
		},
		Datums: make([]tree.Datum, 3),
	}

	// Row 0.
	view0 := &parquetRowView{
		batches:    []*parquetColumnBatch{idBatch, tagsBatch, scoresBatch},
		numColumns: 3,
		rowIndex:   0,
	}
	require.NoError(t, consumer.FillDatums(ctx, view0, 0, conv))

	wantTags := tree.NewDArray(types.String)
	require.NoError(t, wantTags.Append(tree.NewDString("a")))
	require.NoError(t, wantTags.Append(tree.NewDString("b")))
	wantScoresAb := json.NewArrayBuilder(2)
	wantScoresAb.Add(json.FromInt(1))
	wantScoresAb.Add(json.FromInt(2))
	wantScores := tree.NewDJSON(wantScoresAb.Build())

	require.Equal(t, tree.NewDInt(10), conv.Datums[0])
	require.Equal(t, wantTags, conv.Datums[1])
	require.Equal(t, wantScores, conv.Datums[2])

	// Row 1: null list and empty list.
	conv.Datums = make([]tree.Datum, 3)
	view1 := &parquetRowView{
		batches:    []*parquetColumnBatch{idBatch, tagsBatch, scoresBatch},
		numColumns: 3,
		rowIndex:   1,
	}
	require.NoError(t, consumer.FillDatums(ctx, view1, 1, conv))

	require.Equal(t, tree.NewDInt(20), conv.Datums[0])
	require.Equal(t, tree.DNull, conv.Datums[1], "null LIST should map to DNull")
	require.Equal(t, tree.NewDJSON(json.NewArrayBuilder(0).Build()), conv.Datums[2],
		"empty LIST should map to empty JSONB array")
}
