// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/compress"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/pqarrow"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestParquetListColumnReader tests the parquetListColumnReader which reads
// Parquet LIST columns using definition/repetition levels.
// It creates Parquet files with LIST<type> columns using Arrow's ListBuilder,
// then reads them through parquetListColumnReader and verifies per-row
// element arrays are correctly reconstructed from flat definition/repetition levels.
func TestParquetListColumnReader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	pool := memory.NewGoAllocator()

	// Helper to create a Parquet file from an Arrow schema and record, then
	// read column 0 as a LIST batch.
	readListBatch := func(
		t *testing.T,
		arrowSchema *arrow.Schema,
		record arrow.Record,
		numRows int64,
	) *parquetColumnBatch {
		t.Helper()
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

		// Detect the LIST column.
		pqSchema := parquetReader.MetaData().Schema
		listInfo, err := detectListColumn(pqSchema, 0)
		require.NoError(t, err)
		require.NotNil(t, listInfo, "column 0 should be detected as LIST")

		// Read the LIST batch using the stateful reader.
		rowGroup := parquetReader.RowGroup(0)
		colReader, err := rowGroup.Column(0)
		require.NoError(t, err)

		listReader, err := newParquetListColumnReader(colReader, listInfo)
		require.NoError(t, err)
		batch, err := listReader.ReadBatch(numRows)
		require.NoError(t, err)
		require.True(t, batch.isList)
		require.Equal(t, numRows, batch.rowCount)
		return batch
	}

	// ListStates covers all list states in a single record: multi-element,
	// null list, empty list, single element, and null elements within a list.
	t.Run("ListStates", func(t *testing.T) {
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "col", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64), Nullable: true},
		}, nil)
		builder := array.NewRecordBuilder(pool, arrowSchema)
		defer builder.Release()

		lb := builder.Field(0).(*array.ListBuilder)
		vb := lb.ValueBuilder().(*array.Int64Builder)

		// Row 0: [10, 20, 30]
		lb.Append(true)
		vb.Append(10)
		vb.Append(20)
		vb.Append(30)

		// Row 1: null list
		lb.AppendNull()

		// Row 2: [] (empty list)
		lb.Append(true)

		// Row 3: [42]
		lb.Append(true)
		vb.Append(42)

		// Row 4: [1, null, 3] (null elements within list)
		lb.Append(true)
		vb.Append(1)
		vb.AppendNull()
		vb.Append(3)

		// Row 5: [null] (single null element)
		lb.Append(true)
		vb.AppendNull()

		record := builder.NewRecord()
		defer record.Release()

		batch := readListBatch(t, arrowSchema, record, 6)

		// Row 0: [10, 20, 30]
		val, isNull, err := batch.GetValueAt(0)
		require.NoError(t, err)
		require.False(t, isNull)
		elements := val.([]any)
		require.Len(t, elements, 3)
		require.Equal(t, int64(10), elements[0])
		require.Equal(t, int64(20), elements[1])
		require.Equal(t, int64(30), elements[2])

		// Row 1: null list
		_, isNull, err = batch.GetValueAt(1)
		require.NoError(t, err)
		require.True(t, isNull)

		// Row 2: empty list
		val, isNull, err = batch.GetValueAt(2)
		require.NoError(t, err)
		require.False(t, isNull)
		elements = val.([]any)
		require.Len(t, elements, 0)

		// Row 3: [42]
		val, isNull, err = batch.GetValueAt(3)
		require.NoError(t, err)
		require.False(t, isNull)
		elements = val.([]any)
		require.Len(t, elements, 1)
		require.Equal(t, int64(42), elements[0])

		// Row 4: [1, null, 3]
		val, isNull, err = batch.GetValueAt(4)
		require.NoError(t, err)
		require.False(t, isNull)
		elements = val.([]any)
		require.Len(t, elements, 3)
		require.Equal(t, int64(1), elements[0])
		require.Nil(t, elements[1])
		require.Equal(t, int64(3), elements[2])

		// Row 5: [null]
		val, isNull, err = batch.GetValueAt(5)
		require.NoError(t, err)
		require.False(t, isNull)
		elements = val.([]any)
		require.Len(t, elements, 1)
		require.Nil(t, elements[0])
	})

	// PhysicalTypes verifies that each supported Parquet physical type
	// round-trips correctly through the LIST column reader.
	t.Run("PhysicalTypes", func(t *testing.T) {
		tests := []struct {
			name         string
			arrowType    arrow.DataType
			appendValues func(lb *array.ListBuilder)
			expected     []any
		}{
			{
				name:      "Bool",
				arrowType: arrow.FixedWidthTypes.Boolean,
				appendValues: func(lb *array.ListBuilder) {
					vb := lb.ValueBuilder().(*array.BooleanBuilder)
					lb.Append(true)
					vb.Append(true)
					vb.Append(false)
					vb.Append(true)
				},
				expected: []any{true, false, true},
			},
			{
				name:      "Int32",
				arrowType: arrow.PrimitiveTypes.Int32,
				appendValues: func(lb *array.ListBuilder) {
					vb := lb.ValueBuilder().(*array.Int32Builder)
					lb.Append(true)
					vb.Append(10)
					vb.Append(20)
				},
				expected: []any{int32(10), int32(20)},
			},
			{
				name:      "Int64",
				arrowType: arrow.PrimitiveTypes.Int64,
				appendValues: func(lb *array.ListBuilder) {
					vb := lb.ValueBuilder().(*array.Int64Builder)
					lb.Append(true)
					vb.Append(100)
					vb.Append(200)
				},
				expected: []any{int64(100), int64(200)},
			},
			{
				name:      "Float32",
				arrowType: arrow.PrimitiveTypes.Float32,
				appendValues: func(lb *array.ListBuilder) {
					vb := lb.ValueBuilder().(*array.Float32Builder)
					lb.Append(true)
					vb.Append(1.5)
					vb.Append(2.5)
				},
				expected: []any{float32(1.5), float32(2.5)},
			},
			{
				name:      "Float64",
				arrowType: arrow.PrimitiveTypes.Float64,
				appendValues: func(lb *array.ListBuilder) {
					vb := lb.ValueBuilder().(*array.Float64Builder)
					lb.Append(true)
					vb.Append(1.5)
					vb.Append(2.5)
				},
				expected: []any{float64(1.5), float64(2.5)},
			},
			{
				name:      "String",
				arrowType: arrow.BinaryTypes.String,
				appendValues: func(lb *array.ListBuilder) {
					vb := lb.ValueBuilder().(*array.StringBuilder)
					lb.Append(true)
					vb.Append("hello")
					vb.Append("world")
				},
				expected: []any{[]byte("hello"), []byte("world")},
			},
			{
				name:      "FixedLenByteArray",
				arrowType: &arrow.FixedSizeBinaryType{ByteWidth: 4},
				appendValues: func(lb *array.ListBuilder) {
					vb := lb.ValueBuilder().(*array.FixedSizeBinaryBuilder)
					lb.Append(true)
					vb.Append([]byte{1, 2, 3, 4})
					vb.Append([]byte{5, 6, 7, 8})
				},
				expected: []any{
					parquet.FixedLenByteArray([]byte{1, 2, 3, 4}),
					parquet.FixedLenByteArray([]byte{5, 6, 7, 8}),
				},
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				arrowSchema := arrow.NewSchema([]arrow.Field{
					{Name: "col", Type: arrow.ListOf(tc.arrowType), Nullable: true},
				}, nil)
				builder := array.NewRecordBuilder(pool, arrowSchema)
				defer builder.Release()

				lb := builder.Field(0).(*array.ListBuilder)
				tc.appendValues(lb)

				record := builder.NewRecord()
				defer record.Release()

				batch := readListBatch(t, arrowSchema, record, 1)

				val, isNull, err := batch.GetValueAt(0)
				require.NoError(t, err)
				require.False(t, isNull)
				elements := val.([]any)
				require.Len(t, elements, len(tc.expected))
				for i, exp := range tc.expected {
					require.Equal(t, exp, elements[i], "element %d", i)
				}
			})
		}
	})

	// Int96 requires raw Parquet writing since Arrow doesn't produce Int96
	// LIST columns natively.
	t.Run("PhysicalTypes/Int96", func(t *testing.T) {
		fieldID := int32(-1)
		leaf, err := schema.NewPrimitiveNode(
			"element", parquet.Repetitions.Optional,
			parquet.Types.Int96, -1, fieldID)
		require.NoError(t, err)

		repeatedGroup, err := schema.NewGroupNode(
			"list", parquet.Repetitions.Repeated,
			[]schema.Node{leaf}, fieldID)
		require.NoError(t, err)

		listGroup, err := schema.NewGroupNodeLogical(
			"col", parquet.Repetitions.Optional,
			[]schema.Node{repeatedGroup},
			schema.ListLogicalType{}, fieldID)
		require.NoError(t, err)

		root, err := schema.NewGroupNode(
			"schema", parquet.Repetitions.Required,
			[]schema.Node{listGroup}, fieldID)
		require.NoError(t, err)

		buf := new(bytes.Buffer)
		writer := file.NewParquetWriter(buf, root)
		rgWriter := writer.AppendRowGroup()
		colWriter, err := rgWriter.NextColumn()
		require.NoError(t, err)

		int96Writer := colWriter.(*file.Int96ColumnChunkWriter)
		val1 := parquet.Int96{1, 2, 3}
		val2 := parquet.Int96{4, 5, 6}
		_, err = int96Writer.WriteBatch(
			[]parquet.Int96{val1, val2},
			[]int16{3, 3}, // both elements present
			[]int16{0, 1}, // rep=0: new row, rep=1: continuation
		)
		require.NoError(t, err)
		require.NoError(t, rgWriter.Close())
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
		require.Equal(t, parquet.Types.Int96, listInfo.elementPhysicalType)

		rowGroup := parquetReader.RowGroup(0)
		colReader, err := rowGroup.Column(0)
		require.NoError(t, err)

		listReader, err := newParquetListColumnReader(colReader, listInfo)
		require.NoError(t, err)
		batch, err := listReader.ReadBatch(1)
		require.NoError(t, err)

		val, isNull, err := batch.GetValueAt(0)
		require.NoError(t, err)
		require.False(t, isNull)
		elements := val.([]any)
		require.Len(t, elements, 2)
		require.Equal(t, val1, elements[0])
		require.Equal(t, val2, elements[1])
	})

	// ManyRowsAcrossChunks exercises multi-chunk reading for different physical
	// types. 500 rows * 3 elements = 1500 level entries, which exceeds the
	// internal chunk size of 1024. ByteArray and FixedLenByteArray are
	// particularly interesting because they copy buffer contents to avoid
	// reuse corruption.
	t.Run("ManyRowsAcrossChunks", func(t *testing.T) {
		t.Run("Int32", func(t *testing.T) {
			arrowSchema := arrow.NewSchema([]arrow.Field{
				{Name: "nums", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
			}, nil)
			builder := array.NewRecordBuilder(pool, arrowSchema)
			defer builder.Release()

			lb := builder.Field(0).(*array.ListBuilder)
			vb := lb.ValueBuilder().(*array.Int32Builder)

			numRows := int64(500)
			for i := int64(0); i < numRows; i++ {
				lb.Append(true)
				vb.Append(int32(i * 10))
				vb.Append(int32(i*10 + 1))
				vb.Append(int32(i*10 + 2))
			}

			record := builder.NewRecord()
			defer record.Release()

			batch := readListBatch(t, arrowSchema, record, numRows)

			for i := int64(0); i < numRows; i++ {
				val, isNull, err := batch.GetValueAt(int(i))
				require.NoError(t, err)
				require.False(t, isNull, "row %d should not be null", i)
				elements := val.([]any)
				require.Len(t, elements, 3, "row %d", i)
				require.Equal(t, int32(i*10), elements[0], "row %d elem 0", i)
				require.Equal(t, int32(i*10+1), elements[1], "row %d elem 1", i)
				require.Equal(t, int32(i*10+2), elements[2], "row %d elem 2", i)
			}
		})

		t.Run("ByteArray", func(t *testing.T) {
			arrowSchema := arrow.NewSchema([]arrow.Field{
				{Name: "strs", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
			}, nil)
			builder := array.NewRecordBuilder(pool, arrowSchema)
			defer builder.Release()

			lb := builder.Field(0).(*array.ListBuilder)
			vb := lb.ValueBuilder().(*array.StringBuilder)

			numRows := int64(500)
			for i := int64(0); i < numRows; i++ {
				lb.Append(true)
				vb.Append(fmt.Sprintf("a_%d", i*10))
				vb.Append(fmt.Sprintf("b_%d", i*10+1))
				vb.Append(fmt.Sprintf("c_%d", i*10+2))
			}

			record := builder.NewRecord()
			defer record.Release()

			batch := readListBatch(t, arrowSchema, record, numRows)

			for i := int64(0); i < numRows; i++ {
				val, isNull, err := batch.GetValueAt(int(i))
				require.NoError(t, err)
				require.False(t, isNull, "row %d should not be null", i)
				elements := val.([]any)
				require.Len(t, elements, 3, "row %d", i)
				require.Equal(t, []byte(fmt.Sprintf("a_%d", i*10)), elements[0], "row %d elem 0", i)
				require.Equal(t, []byte(fmt.Sprintf("b_%d", i*10+1)), elements[1], "row %d elem 1", i)
				require.Equal(t, []byte(fmt.Sprintf("c_%d", i*10+2)), elements[2], "row %d elem 2", i)
			}
		})

		t.Run("FixedLenByteArray", func(t *testing.T) {
			arrowSchema := arrow.NewSchema([]arrow.Field{
				{Name: "fixed", Type: arrow.ListOf(&arrow.FixedSizeBinaryType{ByteWidth: 4}), Nullable: true},
			}, nil)
			builder := array.NewRecordBuilder(pool, arrowSchema)
			defer builder.Release()

			lb := builder.Field(0).(*array.ListBuilder)
			vb := lb.ValueBuilder().(*array.FixedSizeBinaryBuilder)

			numRows := int64(500)
			for i := int64(0); i < numRows; i++ {
				lb.Append(true)
				for j := 0; j < 3; j++ {
					v := int32(i*10 + int64(j))
					vb.Append([]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
				}
			}

			record := builder.NewRecord()
			defer record.Release()

			batch := readListBatch(t, arrowSchema, record, numRows)

			for i := int64(0); i < numRows; i++ {
				val, isNull, err := batch.GetValueAt(int(i))
				require.NoError(t, err)
				require.False(t, isNull, "row %d should not be null", i)
				elements := val.([]any)
				require.Len(t, elements, 3, "row %d", i)
				for j := 0; j < 3; j++ {
					v := int32(i*10 + int64(j))
					expected := parquet.FixedLenByteArray([]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)})
					require.Equal(t, expected, elements[j], "row %d elem %d", i, j)
				}
			}
		})
	})

	// Verifies that the final row is finalized when the total number of level
	// entries is an exact multiple of listColumnChunkSize (1024).
	t.Run("ExactChunkSizeMultiple", func(t *testing.T) {
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "nums", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
		}, nil)
		builder := array.NewRecordBuilder(pool, schema)
		defer builder.Release()

		lb := builder.Field(0).(*array.ListBuilder)
		vb := lb.ValueBuilder().(*array.Int32Builder)

		// 1024 rows with 1 element each = exactly 1024 level entries.
		numRows := int64(listColumnChunkSize)
		for i := int64(0); i < numRows; i++ {
			lb.Append(true)
			vb.Append(int32(i))
		}

		record := builder.NewRecord()
		defer record.Release()

		batch := readListBatch(t, schema, record, numRows)

		for i := int64(0); i < numRows; i++ {
			val, isNull, err := batch.GetValueAt(int(i))
			require.NoError(t, err)
			require.False(t, isNull, "row %d should not be null", i)
			elements := val.([]any)
			require.Len(t, elements, 1, "row %d", i)
			require.Equal(t, int32(i), elements[0], "row %d", i)
		}
	})

	t.Run("PartialRowAcrossChunkBoundary", func(t *testing.T) {
		// A single row with more elements than listColumnChunkSize (1024)
		// forces the row to be split across multiple internal read chunks.
		// The reader must buffer the partial row and reassemble it correctly.
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "big_list", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
		}, nil)
		builder := array.NewRecordBuilder(pool, schema)
		defer builder.Release()

		lb := builder.Field(0).(*array.ListBuilder)
		vb := lb.ValueBuilder().(*array.Int32Builder)

		numElements := 2*listColumnChunkSize + 100
		lb.Append(true)
		for i := range numElements {
			vb.Append(int32(i))
		}

		record := builder.NewRecord()
		defer record.Release()

		batch := readListBatch(t, schema, record, 1)

		val, isNull, err := batch.GetValueAt(0)
		require.NoError(t, err)
		require.False(t, isNull)
		elements := val.([]any)
		require.Len(t, elements, numElements)
		for i := range numElements {
			require.Equal(t, int32(i), elements[i], "element %d", i)
		}
	})

	t.Run("OverflowAcrossMultipleBatches", func(t *testing.T) {
		// Tests that overflow state is correctly preserved across multiple
		// ReadBatch calls on the same reader. 500 rows read in batches of
		// 100 exercises the overflow drain path 4 times.
		schema := arrow.NewSchema([]arrow.Field{
			{Name: "vals", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
		}, nil)
		builder := array.NewRecordBuilder(pool, schema)
		defer builder.Release()

		lb := builder.Field(0).(*array.ListBuilder)
		vb := lb.ValueBuilder().(*array.Int32Builder)

		numRows := int64(500)
		for i := int64(0); i < numRows; i++ {
			lb.Append(true)
			vb.Append(int32(i * 10))
			vb.Append(int32(i*10 + 1))
			vb.Append(int32(i*10 + 2))
		}

		record := builder.NewRecord()
		defer record.Release()

		buf := new(bytes.Buffer)
		writerProps := parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Uncompressed))
		writer, err := pqarrow.NewFileWriter(
			schema, buf, writerProps, pqarrow.DefaultWriterProps())
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

		rowGroup := parquetReader.RowGroup(0)
		colReader, err := rowGroup.Column(0)
		require.NoError(t, err)

		listReader, err := newParquetListColumnReader(colReader, listInfo)
		require.NoError(t, err)

		// Read in 5 batches of 100 rows each.
		batchSize := int64(100)
		for batchIdx := int64(0); batchIdx < 5; batchIdx++ {
			batch, err := listReader.ReadBatch(batchSize)
			require.NoError(t, err, "batch %d", batchIdx)
			require.Equal(t, batchSize, batch.rowCount, "batch %d", batchIdx)

			for rowInBatch := int64(0); rowInBatch < batchSize; rowInBatch++ {
				globalRow := batchIdx*batchSize + rowInBatch
				val, isNull, err := batch.GetValueAt(int(rowInBatch))
				require.NoError(t, err, "row %d", globalRow)
				require.False(t, isNull, "row %d should not be null", globalRow)
				elements := val.([]any)
				require.Len(t, elements, 3, "row %d", globalRow)
				require.Equal(t, int32(globalRow*10), elements[0], "row %d elem 0", globalRow)
				require.Equal(t, int32(globalRow*10+1), elements[1], "row %d elem 1", globalRow)
				require.Equal(t, int32(globalRow*10+2), elements[2], "row %d elem 2", globalRow)
			}
		}
	})

	t.Run("RequiredListOptionalElement", func(t *testing.T) {
		// A required (non-nullable) list with optional elements. The list
		// itself cannot be null, but elements within it can be.
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "ids", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: false},
		}, nil)
		builder := array.NewRecordBuilder(pool, arrowSchema)
		defer builder.Release()

		lb := builder.Field(0).(*array.ListBuilder)
		vb := lb.ValueBuilder().(*array.Int32Builder)

		// Row 0: [10, 20]
		lb.Append(true)
		vb.Append(10)
		vb.Append(20)

		// Row 1: [] (empty list, valid for required list)
		lb.Append(true)

		// Row 2: [null, 30]
		lb.Append(true)
		vb.AppendNull()
		vb.Append(30)

		record := builder.NewRecord()
		defer record.Release()

		batch := readListBatch(t, arrowSchema, record, 3)

		// Row 0: [10, 20]
		val, isNull, err := batch.GetValueAt(0)
		require.NoError(t, err)
		require.False(t, isNull)
		elements := val.([]any)
		require.Len(t, elements, 2)
		require.Equal(t, int32(10), elements[0])
		require.Equal(t, int32(20), elements[1])

		// Row 1: empty list
		val, isNull, err = batch.GetValueAt(1)
		require.NoError(t, err)
		require.False(t, isNull)
		elements = val.([]any)
		require.Len(t, elements, 0)

		// Row 2: [null, 30]
		val, isNull, err = batch.GetValueAt(2)
		require.NoError(t, err)
		require.False(t, isNull)
		elements = val.([]any)
		require.Len(t, elements, 2)
		require.Nil(t, elements[0])
		require.Equal(t, int32(30), elements[1])
	})

	t.Run("RequiredListRequiredElement", func(t *testing.T) {
		// A required list with required (non-nullable) elements.
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "ids", Type: arrow.ListOfNonNullable(arrow.PrimitiveTypes.Int32), Nullable: false},
		}, nil)
		builder := array.NewRecordBuilder(pool, arrowSchema)
		defer builder.Release()

		lb := builder.Field(0).(*array.ListBuilder)
		vb := lb.ValueBuilder().(*array.Int32Builder)

		// Row 0: [1, 2, 3]
		lb.Append(true)
		vb.Append(1)
		vb.Append(2)
		vb.Append(3)

		// Row 1: []
		lb.Append(true)

		// Row 2: [42]
		lb.Append(true)
		vb.Append(42)

		record := builder.NewRecord()
		defer record.Release()

		batch := readListBatch(t, arrowSchema, record, 3)

		// Row 0: [1, 2, 3]
		val, isNull, err := batch.GetValueAt(0)
		require.NoError(t, err)
		require.False(t, isNull)
		elements := val.([]any)
		require.Len(t, elements, 3)
		require.Equal(t, int32(1), elements[0])
		require.Equal(t, int32(2), elements[1])
		require.Equal(t, int32(3), elements[2])

		// Row 1: empty list
		val, isNull, err = batch.GetValueAt(1)
		require.NoError(t, err)
		require.False(t, isNull)
		elements = val.([]any)
		require.Len(t, elements, 0)

		// Row 2: [42]
		val, isNull, err = batch.GetValueAt(2)
		require.NoError(t, err)
		require.False(t, isNull)
		elements = val.([]any)
		require.Len(t, elements, 1)
		require.Equal(t, int32(42), elements[0])
	})

	t.Run("MultiRowGroup", func(t *testing.T) {
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "vals", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
		}, nil)

		// Write 3 row groups with 4 rows each (12 rows total).
		buf := new(bytes.Buffer)
		writerProps := parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Uncompressed))
		writer, err := pqarrow.NewFileWriter(
			arrowSchema, buf, writerProps, pqarrow.DefaultWriterProps())
		require.NoError(t, err)

		rowsPerGroup := 4
		numGroups := 3
		for g := range numGroups {
			recBuilder := array.NewRecordBuilder(pool, arrowSchema)
			lb := recBuilder.Field(0).(*array.ListBuilder)
			vb := lb.ValueBuilder().(*array.Int32Builder)
			for r := range rowsPerGroup {
				globalRow := g*rowsPerGroup + r
				lb.Append(true)
				vb.Append(int32(globalRow * 100))
				vb.Append(int32(globalRow*100 + 1))
			}
			rec := recBuilder.NewRecord()
			require.NoError(t, writer.Write(rec))
			rec.Release()
			recBuilder.Release()
		}
		require.NoError(t, writer.Close())

		reader := bytes.NewReader(buf.Bytes())
		fr := &fileReader{
			Reader: reader, ReaderAt: reader, Seeker: reader,
			total: int64(buf.Len()),
		}
		parquetReader, err := file.NewParquetReader(fr)
		require.NoError(t, err)
		require.Equal(t, numGroups, parquetReader.NumRowGroups())

		pqSchema := parquetReader.MetaData().Schema
		listInfo, err := detectListColumn(pqSchema, 0)
		require.NoError(t, err)
		require.NotNil(t, listInfo)

		for g := range numGroups {
			rowGroup := parquetReader.RowGroup(g)
			colReader, err := rowGroup.Column(0)
			require.NoError(t, err)

			listReader, err := newParquetListColumnReader(colReader, listInfo)
			require.NoError(t, err)
			batch, err := listReader.ReadBatch(int64(rowsPerGroup))
			require.NoError(t, err)

			for r := range rowsPerGroup {
				globalRow := g*rowsPerGroup + r
				val, isNull, err := batch.GetValueAt(r)
				require.NoError(t, err, "row group %d row %d", g, r)
				require.False(t, isNull, "row group %d row %d", g, r)
				elements := val.([]any)
				require.Len(t, elements, 2, "row group %d row %d", g, r)
				require.Equal(t, int32(globalRow*100), elements[0],
					"row group %d row %d elem 0", g, r)
				require.Equal(t, int32(globalRow*100+1), elements[1],
					"row group %d row %d elem 1", g, r)
			}
		}
	})

	t.Run("LargeRowAcrossRowGroups", func(t *testing.T) {
		// Three row groups: the first and last have small rows, the middle
		// has a single row with 2500 elements. This tests that a row whose
		// element count far exceeds the row group's row budget stays intact
		// in its own row group and reads back correctly alongside normal rows.
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "vals", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
		}, nil)

		buf := new(bytes.Buffer)
		writerProps := parquet.NewWriterProperties(
			parquet.WithCompression(compress.Codecs.Uncompressed))
		writer, err := pqarrow.NewFileWriter(
			arrowSchema, buf, writerProps, pqarrow.DefaultWriterProps())
		require.NoError(t, err)

		// Row group 0: 3 small rows with 2 elements each.
		b0 := array.NewRecordBuilder(pool, arrowSchema)
		lb0 := b0.Field(0).(*array.ListBuilder)
		vb0 := lb0.ValueBuilder().(*array.Int32Builder)
		for r := range 3 {
			lb0.Append(true)
			vb0.Append(int32(r * 10))
			vb0.Append(int32(r*10 + 1))
		}
		rec0 := b0.NewRecord()
		require.NoError(t, writer.Write(rec0))
		rec0.Release()
		b0.Release()

		// Row group 1: 1 large row with 2500 elements.
		b1 := array.NewRecordBuilder(pool, arrowSchema)
		lb1 := b1.Field(0).(*array.ListBuilder)
		vb1 := lb1.ValueBuilder().(*array.Int32Builder)
		numElements := 2500
		lb1.Append(true)
		for i := range numElements {
			vb1.Append(int32(i))
		}
		rec1 := b1.NewRecord()
		require.NoError(t, writer.Write(rec1))
		rec1.Release()
		b1.Release()

		// Row group 2: 2 small rows.
		b2 := array.NewRecordBuilder(pool, arrowSchema)
		lb2 := b2.Field(0).(*array.ListBuilder)
		vb2 := lb2.ValueBuilder().(*array.Int32Builder)
		for r := range 2 {
			lb2.Append(true)
			vb2.Append(int32((r + 100) * 10))
		}
		rec2 := b2.NewRecord()
		require.NoError(t, writer.Write(rec2))
		rec2.Release()
		b2.Release()

		require.NoError(t, writer.Close())

		reader := bytes.NewReader(buf.Bytes())
		fr := &fileReader{
			Reader: reader, ReaderAt: reader, Seeker: reader,
			total: int64(buf.Len()),
		}
		parquetReader, err := file.NewParquetReader(fr)
		require.NoError(t, err)
		require.Equal(t, 3, parquetReader.NumRowGroups())

		pqSchema := parquetReader.MetaData().Schema
		listInfo, err := detectListColumn(pqSchema, 0)
		require.NoError(t, err)
		require.NotNil(t, listInfo)

		// Read row group 0: 3 small rows.
		rg0 := parquetReader.RowGroup(0)
		col0, err := rg0.Column(0)
		require.NoError(t, err)
		lr0, err := newParquetListColumnReader(col0, listInfo)
		require.NoError(t, err)
		batch0, err := lr0.ReadBatch(3)
		require.NoError(t, err)
		for r := range 3 {
			val, isNull, err := batch0.GetValueAt(r)
			require.NoError(t, err)
			require.False(t, isNull, "rg0 row %d", r)
			elements := val.([]any)
			require.Len(t, elements, 2, "rg0 row %d", r)
			require.Equal(t, int32(r*10), elements[0], "rg0 row %d", r)
			require.Equal(t, int32(r*10+1), elements[1], "rg0 row %d", r)
		}

		// Read row group 1: 1 large row with 2500 elements.
		rg1 := parquetReader.RowGroup(1)
		col1, err := rg1.Column(0)
		require.NoError(t, err)
		lr1, err := newParquetListColumnReader(col1, listInfo)
		require.NoError(t, err)
		batch1, err := lr1.ReadBatch(1)
		require.NoError(t, err)
		val, isNull, err := batch1.GetValueAt(0)
		require.NoError(t, err)
		require.False(t, isNull)
		elements := val.([]any)
		require.Len(t, elements, numElements)
		for i := range numElements {
			require.Equal(t, int32(i), elements[i], "large row elem %d", i)
		}

		// Read row group 2: 2 small rows.
		rg2 := parquetReader.RowGroup(2)
		col2, err := rg2.Column(0)
		require.NoError(t, err)
		lr2, err := newParquetListColumnReader(col2, listInfo)
		require.NoError(t, err)
		batch2, err := lr2.ReadBatch(2)
		require.NoError(t, err)
		for r := range 2 {
			val, isNull, err := batch2.GetValueAt(r)
			require.NoError(t, err)
			require.False(t, isNull, "rg2 row %d", r)
			elements := val.([]any)
			require.Len(t, elements, 1, "rg2 row %d", r)
			require.Equal(t, int32((r+100)*10), elements[0], "rg2 row %d", r)
		}
	})

	t.Run("SmallBatchMidRowResume", func(t *testing.T) {
		// 6 rows with ~300 elements each (1800 total level entries).
		// ReadBatch(2) three times exercises the partial-row save/resume path:
		// chunk size is 1024, so a chunk boundary falls mid-row, and the
		// next ReadBatch call must resume the partial row correctly.
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "big", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
		}, nil)
		builder := array.NewRecordBuilder(pool, arrowSchema)
		defer builder.Release()

		lb := builder.Field(0).(*array.ListBuilder)
		vb := lb.ValueBuilder().(*array.Int32Builder)

		totalRows := 6
		elementsPerRow := 300
		for r := range totalRows {
			lb.Append(true)
			for e := range elementsPerRow {
				vb.Append(int32(r*1000 + e))
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
		parquetReader, err := file.NewParquetReader(fr)
		require.NoError(t, err)

		pqSchema := parquetReader.MetaData().Schema
		listInfo, err := detectListColumn(pqSchema, 0)
		require.NoError(t, err)
		require.NotNil(t, listInfo)

		rowGroup := parquetReader.RowGroup(0)
		colReader, err := rowGroup.Column(0)
		require.NoError(t, err)

		listReader, err := newParquetListColumnReader(colReader, listInfo)
		require.NoError(t, err)

		batchSize := int64(2)
		for batchIdx := range 3 {
			batch, err := listReader.ReadBatch(batchSize)
			require.NoError(t, err, "batch %d", batchIdx)
			require.Equal(t, batchSize, batch.rowCount, "batch %d", batchIdx)

			for rowInBatch := range int(batchSize) {
				globalRow := batchIdx*int(batchSize) + rowInBatch
				val, isNull, err := batch.GetValueAt(rowInBatch)
				require.NoError(t, err, "row %d", globalRow)
				require.False(t, isNull, "row %d should not be null", globalRow)
				elements := val.([]any)
				require.Len(t, elements, elementsPerRow, "row %d", globalRow)
				for e := range elementsPerRow {
					require.Equal(t, int32(globalRow*1000+e), elements[e],
						"row %d elem %d", globalRow, e)
				}
			}
		}
	})

	t.Run("ReadBatchExceedsAvailableRows", func(t *testing.T) {
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "vals", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
		}, nil)
		builder := array.NewRecordBuilder(pool, arrowSchema)
		defer builder.Release()

		lb := builder.Field(0).(*array.ListBuilder)
		vb := lb.ValueBuilder().(*array.Int32Builder)

		for r := range 3 {
			lb.Append(true)
			vb.Append(int32(r))
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
		parquetReader, err := file.NewParquetReader(fr)
		require.NoError(t, err)

		pqSchema := parquetReader.MetaData().Schema
		listInfo, err := detectListColumn(pqSchema, 0)
		require.NoError(t, err)

		rowGroup := parquetReader.RowGroup(0)
		colReader, err := rowGroup.Column(0)
		require.NoError(t, err)

		listReader, err := newParquetListColumnReader(colReader, listInfo)
		require.NoError(t, err)

		_, err = listReader.ReadBatch(5)
		require.Error(t, err)
		require.ErrorContains(t, err, "expected 5 rows")
		require.ErrorContains(t, err, "only 3 were available")
	})
}

// makeListSchema builds a standard 3-level Parquet LIST schema:
//
//	root (required) -> listGroup (listRep, LIST) -> repeated group -> leaf (elemRep, elemType)
//
// Returns the schema ready for detectListColumn.
func makeListSchema(
	t *testing.T,
	columnName string,
	listRep parquet.Repetition,
	elemRep parquet.Repetition,
	elemType parquet.Type,
) *schema.Schema {
	t.Helper()
	fieldID := int32(-1)

	leaf, err := schema.NewPrimitiveNode("element", elemRep, elemType, -1, fieldID)
	require.NoError(t, err)

	repeatedGroup, err := schema.NewGroupNode(
		"list", parquet.Repetitions.Repeated, []schema.Node{leaf}, fieldID)
	require.NoError(t, err)

	listGroup, err := schema.NewGroupNodeLogical(
		columnName, listRep, []schema.Node{repeatedGroup},
		schema.ListLogicalType{}, fieldID)
	require.NoError(t, err)

	root, err := schema.NewGroupNode(
		"schema", parquet.Repetitions.Required, []schema.Node{listGroup}, fieldID)
	require.NoError(t, err)

	return schema.NewSchema(root)
}

// TestDetectListColumn tests the detectListColumn function with manually
// constructed Parquet schemas, covering both valid LIST schemas and
// unsupported/invalid structures that should return errors.
func TestDetectListColumn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	fieldID := int32(-1)

	// ValidSchemas covers all combinations of optional/required list and element.
	t.Run("ValidSchemas", func(t *testing.T) {
		tests := []struct {
			name                  string
			listRep               parquet.Repetition
			elemRep               parquet.Repetition
			elemType              parquet.Type
			expectedElemOptional  bool
			expectedNullDefLevel  int16
			expectedEmptyDefLevel int16
		}{
			{
				name:                  "OptionalListOptionalElement",
				listRep:               parquet.Repetitions.Optional,
				elemRep:               parquet.Repetitions.Optional,
				elemType:              parquet.Types.Int32,
				expectedElemOptional:  true,
				expectedNullDefLevel:  0,
				expectedEmptyDefLevel: 1,
			},
			{
				name:                  "OptionalListRequiredElement",
				listRep:               parquet.Repetitions.Optional,
				elemRep:               parquet.Repetitions.Required,
				elemType:              parquet.Types.Int64,
				expectedElemOptional:  false,
				expectedNullDefLevel:  0,
				expectedEmptyDefLevel: 1,
			},
			{
				name:                  "RequiredListOptionalElement",
				listRep:               parquet.Repetitions.Required,
				elemRep:               parquet.Repetitions.Optional,
				elemType:              parquet.Types.Int64,
				expectedElemOptional:  true,
				expectedNullDefLevel:  -1,
				expectedEmptyDefLevel: 0,
			},
			{
				name:                  "RequiredListRequiredElement",
				listRep:               parquet.Repetitions.Required,
				elemRep:               parquet.Repetitions.Required,
				elemType:              parquet.Types.Int32,
				expectedElemOptional:  false,
				expectedNullDefLevel:  -1,
				expectedEmptyDefLevel: 0,
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				pqSchema := makeListSchema(t, "col", tc.listRep, tc.elemRep, tc.elemType)
				info, err := detectListColumn(pqSchema, 0)
				require.NoError(t, err)
				require.NotNil(t, info)

				require.Equal(t, "col", info.columnName)
				require.Equal(t, tc.elemType, info.elementPhysicalType)
				require.Equal(t, 1, info.nestingDepth)
				require.Equal(t, tc.expectedElemOptional, info.elementIsOptional)
				require.Len(t, info.levels, 1)
				require.Equal(t, tc.expectedNullDefLevel, info.levels[0].nullListDefLevel)
				require.Equal(t, tc.expectedEmptyDefLevel, info.levels[0].emptyListDefLevel)
			})
		}
	})

	t.Run("FlatColumn", func(t *testing.T) {
		// A flat column (no repetition) should return nil, nil.
		leaf, err := schema.NewPrimitiveNode(
			"flat_col", parquet.Repetitions.Optional,
			parquet.Types.Int32, -1, fieldID)
		require.NoError(t, err)

		root, err := schema.NewGroupNode(
			"schema", parquet.Repetitions.Required,
			[]schema.Node{leaf}, fieldID)
		require.NoError(t, err)

		pqSchema := schema.NewSchema(root)
		info, err := detectListColumn(pqSchema, 0)
		require.NoError(t, err)
		require.Nil(t, info, "flat column should not be detected as LIST")
	})

	t.Run("NestedListRejected", func(t *testing.T) {
		// LIST(LIST(INT32)): should be rejected as unsupported.
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

		outerList, err := schema.NewGroupNodeLogical(
			"nested", parquet.Repetitions.Optional,
			[]schema.Node{outerRepeated},
			schema.ListLogicalType{}, fieldID)
		require.NoError(t, err)

		root, err := schema.NewGroupNode(
			"schema", parquet.Repetitions.Required,
			[]schema.Node{outerList}, fieldID)
		require.NoError(t, err)

		pqSchema := schema.NewSchema(root)
		_, err = detectListColumn(pqSchema, 0)
		require.Error(t, err)
		require.ErrorContains(t, err, "nested LIST")
	})

	t.Run("RepeatedParentNotListGroup", func(t *testing.T) {
		// A repeated group whose parent does not have LIST annotation.
		leaf, err := schema.NewPrimitiveNode(
			"element", parquet.Repetitions.Optional,
			parquet.Types.Int32, -1, fieldID)
		require.NoError(t, err)

		repeatedGroup, err := schema.NewGroupNode(
			"list", parquet.Repetitions.Repeated,
			[]schema.Node{leaf}, fieldID)
		require.NoError(t, err)

		nonListGroup, err := schema.NewGroupNode(
			"not_a_list", parquet.Repetitions.Optional,
			[]schema.Node{repeatedGroup}, fieldID)
		require.NoError(t, err)

		root, err := schema.NewGroupNode(
			"schema", parquet.Repetitions.Required,
			[]schema.Node{nonListGroup}, fieldID)
		require.NoError(t, err)

		pqSchema := schema.NewSchema(root)
		_, err = detectListColumn(pqSchema, 0)
		require.Error(t, err)
		require.ErrorContains(t, err, "grandparent is not a LIST group")
	})

	t.Run("ConvertedTypeListDetected", func(t *testing.T) {
		// LIST columns annotated with ConvertedType (legacy) should be detected.
		arrowSchema := arrow.NewSchema([]arrow.Field{
			{Name: "tags", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
		}, nil)
		pool := memory.NewGoAllocator()
		builder := array.NewRecordBuilder(pool, arrowSchema)
		defer builder.Release()

		lb := builder.Field(0).(*array.ListBuilder)
		vb := lb.ValueBuilder().(*array.Int32Builder)
		lb.Append(true)
		vb.Append(1)

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
		info, err := detectListColumn(pqSchema, 0)
		require.NoError(t, err)
		require.NotNil(t, info)
		require.Equal(t, "tags", info.columnName)
	})

	t.Run("MapColumnRejected", func(t *testing.T) {
		// MAP columns have a similar repeated-group structure to LIST but
		// with MAP logical type. detectListColumn should reject them with
		// a MAP-specific error rather than a generic one.

		makeMapSchema := func(
			t *testing.T, useLogicalType bool,
		) *schema.Schema {
			t.Helper()
			keyNode, err := schema.NewPrimitiveNode(
				"key", parquet.Repetitions.Required,
				parquet.Types.ByteArray, -1, fieldID)
			require.NoError(t, err)

			valueNode, err := schema.NewPrimitiveNode(
				"value", parquet.Repetitions.Optional,
				parquet.Types.Int32, -1, fieldID)
			require.NoError(t, err)

			keyValueGroup, err := schema.NewGroupNode(
				"key_value", parquet.Repetitions.Repeated,
				[]schema.Node{keyNode, valueNode}, fieldID)
			require.NoError(t, err)

			var mapGroup *schema.GroupNode
			if useLogicalType {
				mapGroup, err = schema.NewGroupNodeLogical(
					"my_map", parquet.Repetitions.Optional,
					[]schema.Node{keyValueGroup},
					schema.MapLogicalType{}, fieldID)
			} else {
				mapGroup, err = schema.NewGroupNodeConverted(
					"my_map", parquet.Repetitions.Optional,
					[]schema.Node{keyValueGroup},
					schema.ConvertedTypes.MapKeyValue, fieldID)
			}
			require.NoError(t, err)

			root, err := schema.NewGroupNode(
				"schema", parquet.Repetitions.Required,
				[]schema.Node{mapGroup}, fieldID)
			require.NoError(t, err)
			return schema.NewSchema(root)
		}

		t.Run("LogicalType", func(t *testing.T) {
			pqSchema := makeMapSchema(t, true /* useLogicalType */)
			for _, colIdx := range []int{0, 1} {
				_, err := detectListColumn(pqSchema, colIdx)
				require.Errorf(t, err, "column %d should error", colIdx)
				require.ErrorContainsf(t, err, "MAP", "column %d should mention MAP", colIdx)
			}
		})

		t.Run("ConvertedType", func(t *testing.T) {
			pqSchema := makeMapSchema(t, false /* useLogicalType */)
			for _, colIdx := range []int{0, 1} {
				_, err := detectListColumn(pqSchema, colIdx)
				require.Errorf(t, err, "column %d should error", colIdx)
				require.ErrorContainsf(t, err, "MAP", "column %d should mention MAP", colIdx)
			}
		})
	})
}
