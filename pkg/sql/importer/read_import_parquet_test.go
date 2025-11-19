// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/compress"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/pqarrow"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

// createTestParquetFile creates a simple Parquet file in memory for testing
func createTestParquetFile(
	t *testing.T, numRows int, compression compress.Compression,
) *bytes.Buffer {
	// Handle empty file case - return nil, tests should check for this
	if numRows == 0 {
		return nil
	}

	pool := memory.NewGoAllocator()

	// Define schema with various types
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		},
		nil,
	)

	// Build record with test data
	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	idBuilder := builder.Field(0).(*array.Int32Builder)
	nameBuilder := builder.Field(1).(*array.StringBuilder)
	scoreBuilder := builder.Field(2).(*array.Float64Builder)
	activeBuilder := builder.Field(3).(*array.BooleanBuilder)

	for i := 0; i < numRows; i++ {
		idBuilder.Append(int32(i))
		if i%3 == 0 {
			nameBuilder.AppendNull()
		} else {
			nameBuilder.Append("name_" + string(rune('A'+i%26)))
		}
		if i%5 == 0 {
			scoreBuilder.AppendNull()
		} else {
			scoreBuilder.Append(float64(i) * 1.5)
		}
		activeBuilder.Append(i%2 == 0)
	}

	record := builder.NewRecord()
	defer record.Release()

	// Write to Parquet format with specified compression
	buf := new(bytes.Buffer)
	writerProps := parquet.NewWriterProperties(parquet.WithCompression(compression))
	writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
	require.NoError(t, err)

	err = writer.Write(record)
	require.NoError(t, err)

	err = writer.Close()
	require.NoError(t, err)

	return buf
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
			parquetData := createTestParquetFile(t, 10, tc.compression)

			// Create fileReader from buffer - use the same bytes.Reader instance for all interfaces
			reader := bytes.NewReader(parquetData.Bytes())
			fileReader := &fileReader{
				Reader:   reader,
				ReaderAt: reader,
				Seeker:   reader,
				total:    int64(parquetData.Len()),
			}

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
	parquetData := createTestParquetFile(t, 15, compress.Codecs.Uncompressed)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

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
	parquetData := createTestParquetFile(t, numRows, compress.Codecs.Uncompressed)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

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
	parquetData := createTestParquetFile(t, numRows, compress.Codecs.Uncompressed)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

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
	parquetData := createTestParquetFile(t, 1, compress.Codecs.Uncompressed)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

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

	parquetData := createTestParquetFile(t, 10, compress.Codecs.Uncompressed)

	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}

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
	parquetData := createTestParquetFile(t, 1, compress.Codecs.Uncompressed)
	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}
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
	parquetData := createTestParquetFile(t, 1, compress.Codecs.Uncompressed)
	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}
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

	// Verify the mapping is correct:
	// - "score" should map to visibleCols[3] (index 3 in table schema)
	// - "name" should map to visibleCols[1] (index 1 in table schema)
	// - "id" should map to visibleCols[0] (index 0 in table schema)
	require.Equal(t, 3, consumer.fieldNameToIdx["score"], "score should map to visibleCols index 3")
	require.Equal(t, 1, consumer.fieldNameToIdx["name"], "name should map to visibleCols index 1")
	require.Equal(t, 0, consumer.fieldNameToIdx["id"], "id should map to visibleCols index 0")
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
	parquetData := createTestParquetFile(t, 1, compress.Codecs.Uncompressed)
	reader := bytes.NewReader(parquetData.Bytes())
	fileReader := &fileReader{
		Reader:   reader,
		ReaderAt: reader,
		Seeker:   reader,
		total:    int64(parquetData.Len()),
	}
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
		require.False(t, consumer.strict)

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
