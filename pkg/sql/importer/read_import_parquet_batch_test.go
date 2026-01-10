// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"bytes"
	"os"
	"testing"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/compress"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/pqarrow"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestParquetColumnBatchAllTypes tests batch reading for all Parquet physical types
func TestParquetColumnBatchAllTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name              string
		arrowType         arrow.DataType
		parquetType       parquet.Type
		buildValues       func(*array.RecordBuilder)
		validateBool      func(*testing.T, *parquetColumnBatch)
		validateInt32     func(*testing.T, *parquetColumnBatch)
		validateInt64     func(*testing.T, *parquetColumnBatch)
		validateInt96     func(*testing.T, *parquetColumnBatch)
		validateFloat32   func(*testing.T, *parquetColumnBatch)
		validateFloat64   func(*testing.T, *parquetColumnBatch)
		validateByteArray func(*testing.T, *parquetColumnBatch)
		validateFixedLen  func(*testing.T, *parquetColumnBatch)
	}{
		{
			name:        "Boolean",
			arrowType:   arrow.FixedWidthTypes.Boolean,
			parquetType: parquet.Types.Boolean,
			buildValues: func(builder *array.RecordBuilder) {
				b := builder.Field(0).(*array.BooleanBuilder)
				b.Append(true)
				b.AppendNull()
				b.Append(false)
				b.Append(true)
			},
			validateBool: func(t *testing.T, batch *parquetColumnBatch) {
				require.False(t, batch.isNull[0])
				require.True(t, batch.boolValues[0])
				require.True(t, batch.isNull[1])
				require.False(t, batch.isNull[2])
				require.False(t, batch.boolValues[2])
				require.False(t, batch.isNull[3])
				require.True(t, batch.boolValues[3])
			},
		},
		{
			name:        "Int32",
			arrowType:   arrow.PrimitiveTypes.Int32,
			parquetType: parquet.Types.Int32,
			buildValues: func(builder *array.RecordBuilder) {
				b := builder.Field(0).(*array.Int32Builder)
				b.Append(42)
				b.Append(100)
				b.AppendNull()
				b.Append(-5)
			},
			validateInt32: func(t *testing.T, batch *parquetColumnBatch) {
				require.False(t, batch.isNull[0])
				require.Equal(t, int32(42), batch.int32Values[0])
				require.False(t, batch.isNull[1])
				require.Equal(t, int32(100), batch.int32Values[1])
				require.True(t, batch.isNull[2])
				require.False(t, batch.isNull[3])
				require.Equal(t, int32(-5), batch.int32Values[3])
			},
		},
		{
			name:        "Int64",
			arrowType:   arrow.PrimitiveTypes.Int64,
			parquetType: parquet.Types.Int64,
			buildValues: func(builder *array.RecordBuilder) {
				b := builder.Field(0).(*array.Int64Builder)
				b.Append(9223372036854775807) // Max int64
				b.AppendNull()
				b.Append(-9223372036854775808) // Min int64
				b.Append(12345)
			},
			validateInt64: func(t *testing.T, batch *parquetColumnBatch) {
				require.False(t, batch.isNull[0])
				require.Equal(t, int64(9223372036854775807), batch.int64Values[0])
				require.True(t, batch.isNull[1])
				require.False(t, batch.isNull[2])
				require.Equal(t, int64(-9223372036854775808), batch.int64Values[2])
				require.False(t, batch.isNull[3])
				require.Equal(t, int64(12345), batch.int64Values[3])
			},
		},
		{
			name:        "Float",
			arrowType:   arrow.PrimitiveTypes.Float32,
			parquetType: parquet.Types.Float,
			buildValues: func(builder *array.RecordBuilder) {
				b := builder.Field(0).(*array.Float32Builder)
				b.Append(3.14)
				b.AppendNull()
				b.Append(-2.71)
				b.Append(0.0)
			},
			validateFloat32: func(t *testing.T, batch *parquetColumnBatch) {
				require.False(t, batch.isNull[0])
				require.InDelta(t, 3.14, batch.float32Values[0], 0.01)
				require.True(t, batch.isNull[1])
				require.False(t, batch.isNull[2])
				require.InDelta(t, -2.71, batch.float32Values[2], 0.01)
				require.False(t, batch.isNull[3])
				require.InDelta(t, 0.0, batch.float32Values[3], 0.01)
			},
		},
		{
			name:        "Double",
			arrowType:   arrow.PrimitiveTypes.Float64,
			parquetType: parquet.Types.Double,
			buildValues: func(builder *array.RecordBuilder) {
				b := builder.Field(0).(*array.Float64Builder)
				b.Append(3.14159)
				b.Append(2.71828)
				b.AppendNull()
				b.Append(-1.41421)
			},
			validateFloat64: func(t *testing.T, batch *parquetColumnBatch) {
				require.False(t, batch.isNull[0])
				require.InDelta(t, 3.14159, batch.float64Values[0], 0.00001)
				require.False(t, batch.isNull[1])
				require.InDelta(t, 2.71828, batch.float64Values[1], 0.00001)
				require.True(t, batch.isNull[2])
				require.False(t, batch.isNull[3])
				require.InDelta(t, -1.41421, batch.float64Values[3], 0.00001)
			},
		},
		{
			name:        "ByteArray",
			arrowType:   arrow.BinaryTypes.String,
			parquetType: parquet.Types.ByteArray,
			buildValues: func(builder *array.RecordBuilder) {
				b := builder.Field(0).(*array.StringBuilder)
				b.Append("hello")
				b.AppendNull()
				b.Append("world")
				b.Append("")
			},
			validateByteArray: func(t *testing.T, batch *parquetColumnBatch) {
				require.False(t, batch.isNull[0])
				require.Equal(t, []byte("hello"), batch.byteArrayValues[0])
				require.True(t, batch.isNull[1])
				require.False(t, batch.isNull[2])
				require.Equal(t, []byte("world"), batch.byteArrayValues[2])
				require.False(t, batch.isNull[3])
				require.Equal(t, []byte(""), batch.byteArrayValues[3])
			},
		},
		{
			name:        "FixedLenByteArray",
			arrowType:   &arrow.FixedSizeBinaryType{ByteWidth: 16},
			parquetType: parquet.Types.FixedLenByteArray,
			buildValues: func(builder *array.RecordBuilder) {
				b := builder.Field(0).(*array.FixedSizeBinaryBuilder)
				// UUID-like values (16 bytes)
				b.Append([]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10})
				b.AppendNull()
				b.Append([]byte{0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8, 0xf7, 0xf6, 0xf5, 0xf4, 0xf3, 0xf2, 0xf1, 0xf0})
				b.Append([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
			},
			validateFixedLen: func(t *testing.T, batch *parquetColumnBatch) {
				require.False(t, batch.isNull[0])
				require.Equal(t, parquet.FixedLenByteArray{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10}, batch.fixedLenByteArrayValues[0])
				require.True(t, batch.isNull[1])
				require.False(t, batch.isNull[2])
				require.Equal(t, parquet.FixedLenByteArray{0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8, 0xf7, 0xf6, 0xf5, 0xf4, 0xf3, 0xf2, 0xf1, 0xf0}, batch.fixedLenByteArrayValues[2])
				require.False(t, batch.isNull[3])
				require.Equal(t, parquet.FixedLenByteArray{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, batch.fixedLenByteArrayValues[3])
			},
		},
		// Note: INT96 is not tested here because Arrow's pqarrow library doesn't support
		// writing INT96 columns (it's deprecated). INT96 support is validated in
		// TestParquetColumnBatchRealFiles using real Parquet files that contain INT96 timestamps.
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewGoAllocator()
			schema := arrow.NewSchema(
				[]arrow.Field{
					{Name: "col", Type: tc.arrowType, Nullable: true},
				},
				nil,
			)

			builder := array.NewRecordBuilder(pool, schema)
			defer builder.Release()

			tc.buildValues(builder)

			record := builder.NewRecord()
			defer record.Release()

			buf := new(bytes.Buffer)
			writerProps := parquet.NewWriterProperties(parquet.WithCompression(compress.Codecs.Uncompressed))
			writer, err := pqarrow.NewFileWriter(schema, buf, writerProps, pqarrow.DefaultWriterProps())
			require.NoError(t, err)
			require.NoError(t, writer.Write(record))
			require.NoError(t, writer.Close())

			// Read the Parquet file
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
			colReader, err := rowGroup.Column(0)
			require.NoError(t, err)

			buffers := newParquetBatchBuffers(4)
			batch, err := newParquetColumnBatch(colReader, buffers, 4)
			require.NoError(t, err)

			require.Equal(t, tc.parquetType, batch.physicalType)
			require.Equal(t, int64(4), batch.rowCount)

			err = batch.Read()
			require.NoError(t, err)

			// Validate based on type
			if tc.validateBool != nil {
				tc.validateBool(t, batch)
			}
			if tc.validateInt32 != nil {
				tc.validateInt32(t, batch)
			}
			if tc.validateInt64 != nil {
				tc.validateInt64(t, batch)
			}
			if tc.validateInt96 != nil {
				tc.validateInt96(t, batch)
			}
			if tc.validateFloat32 != nil {
				tc.validateFloat32(t, batch)
			}
			if tc.validateFloat64 != nil {
				tc.validateFloat64(t, batch)
			}
			if tc.validateByteArray != nil {
				tc.validateByteArray(t, batch)
			}
			if tc.validateFixedLen != nil {
				tc.validateFixedLen(t, batch)
			}
		})
	}
}

// TestParquetColumnBatchNonNullable tests reading a non-nullable column
func TestParquetColumnBatchNonNullable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		},
		nil,
	)

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	int32Builder := builder.Field(0).(*array.Int32Builder)
	int32Builder.Append(1)
	int32Builder.Append(2)
	int32Builder.Append(3)

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
	colReader, err := rowGroup.Column(0)
	require.NoError(t, err)

	buffers := newParquetBatchBuffers(3)
	batch, err := newParquetColumnBatch(colReader, buffers, 3)
	require.NoError(t, err)

	// Non-nullable column should have maxDefLevel == 0
	require.Equal(t, int16(0), batch.maxDefLevel)

	err = batch.Read()
	require.NoError(t, err)

	// All values should be non-null
	for i := 0; i < 3; i++ {
		require.False(t, batch.isNull[i])
	}

	require.Equal(t, int32(1), batch.int32Values[0])
	require.Equal(t, int32(2), batch.int32Values[1])
	require.Equal(t, int32(3), batch.int32Values[2])
}

// TestParquetColumnBatchRealFiles tests reading real Parquet files from the
// apache/parquet-testing repository. This validates that our implementation
// handles real-world files with various encodings, compressions, and edge cases.
func TestParquetColumnBatchRealFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		filename     string
		expectedRows int64
		expectedCols int
	}{
		{
			filename:     "alltypes_plain.parquet",
			expectedRows: 8,
			expectedCols: 11,
		},
		{
			filename:     "alltypes_plain.snappy.parquet",
			expectedRows: 2,
			expectedCols: 11,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.filename, func(t *testing.T) {
			// Open the test file
			testFile := "testdata/parquet/" + tc.filename
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

			parquetReader, err := file.NewParquetReader(fr)
			require.NoError(t, err)

			// Validate metadata
			schema := parquetReader.MetaData().Schema
			require.Equal(t, tc.expectedCols, schema.NumColumns())

			totalRows := int64(0)
			for rg := 0; rg < parquetReader.NumRowGroups(); rg++ {
				rowGroup := parquetReader.RowGroup(rg)
				totalRows += rowGroup.NumRows()

				// Try to read each column in this row group
				buffers := newParquetBatchBuffers(rowGroup.NumRows())
				for col := 0; col < schema.NumColumns(); col++ {
					colReader, err := rowGroup.Column(col)
					require.NoError(t, err)

					batch, err := newParquetColumnBatch(colReader, buffers, rowGroup.NumRows())
					require.NoError(t, err)

					err = batch.Read()
					require.NoError(t, err)
					require.Equal(t, rowGroup.NumRows(), batch.rowCount)
				}
			}

			require.Equal(t, tc.expectedRows, totalRows)
		})
	}
}
