// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/cockroachdb/errors"
)

// parquetReadBatchFunc reads a batch of values from a Parquet column reader.
// Returns (numRowsRead, numValuesRead, error).
// numRowsRead includes NULLs, numValuesRead excludes NULLs.
type parquetReadBatchFunc func() (numRowsRead int64, numValuesRead int, err error)

// parquetCopyValueFunc copies a single value from the read buffer to the batch.
// rowIdx is the destination index in the batch (including NULLs).
// valIdx is the source index in the read buffer (excluding NULLs).
type parquetCopyValueFunc func(rowIdx int, valIdx int)

// parquetColumnBatch stores a batch of values for a single column in typed form.
// This avoids boxing values into interface{} until absolutely necessary.
//
// The batch supports all Parquet physical types and handles NULL values correctly.
// Values are stored in type-specific slices (e.g., boolValues, int32Values) based
// on the column's physical type. The isNull array tracks which rows are NULL.
//
// Design: We use separate slices for each type rather than []interface{} to:
// 1. Avoid heap allocations from boxing primitive values
// 2. Enable better cache locality during batch processing
// 3. Make it easier for the consumer to access typed values efficiently
type parquetColumnBatch struct {
	physicalType parquet.Type
	rowCount     int64 // Number of rows in this batch
	maxDefLevel  int16 // Maximum definition level for this column (for NULL handling)

	// Definition levels for this batch - determines which rows are NULL
	defLevels []int16

	// Functions for reading and copying values (set up in constructor)
	readBatch parquetReadBatchFunc
	copyValue parquetCopyValueFunc

	// Only one of these slices will be populated, based on physicalType.
	// All slices have length rowCount (expanded to include nulls).
	boolValues              []bool
	int32Values             []int32
	int64Values             []int64
	int96Values             []parquet.Int96 // Deprecated timestamp format
	float32Values           []float32
	float64Values           []float64
	byteArrayValues         [][]byte // Copied bytes
	fixedLenByteArrayValues []parquet.FixedLenByteArray

	// Null tracking - one entry per row
	isNull []bool // [rowIdx] -> is this row null?
}

// parquetBatchBuffers holds reusable buffers for reading Parquet values.
// These buffers are allocated once per row group to avoid repeated allocations.
type parquetBatchBuffers struct {
	boolBuf    []bool
	int32Buf   []int32
	int64Buf   []int64
	float32Buf []float32
	float64Buf []float64
	defLevels  []int16 // Definition levels (for NULL handling)
	repLevels  []int16 // Repetition levels (for arrays)
}

// newParquetBatchBuffers creates buffers sized for the given batch size.
func newParquetBatchBuffers(batchSize int64) *parquetBatchBuffers {
	return &parquetBatchBuffers{
		boolBuf:    make([]bool, batchSize),
		int32Buf:   make([]int32, batchSize),
		int64Buf:   make([]int64, batchSize),
		float32Buf: make([]float32, batchSize),
		float64Buf: make([]float64, batchSize),
		defLevels:  make([]int16, batchSize),
		repLevels:  make([]int16, batchSize),
	}
}

// newParquetColumnBatch creates a new column batch with allocated buffers and
// configured reader/copier functions based on the physical type.
//
// The batch reads values from the Parquet column reader using the provided buffers.
// For nullable columns, it uses definition levels to determine which rows are NULL.
func newParquetColumnBatch(
	colReader file.ColumnChunkReader, buffers *parquetBatchBuffers, rowCount int64,
) (*parquetColumnBatch, error) {
	descriptor := colReader.Descriptor()
	maxDefLevel := descriptor.MaxDefinitionLevel()
	physicalType := colReader.Type()

	b := &parquetColumnBatch{
		physicalType: physicalType,
		rowCount:     rowCount,
		maxDefLevel:  maxDefLevel,
		defLevels:    make([]int16, rowCount),
		isNull:       make([]bool, rowCount),
	}

	// Set up typed buffer and reader/copier functions based on physical type
	switch physicalType {
	case parquet.Types.Boolean:
		b.boolValues = make([]bool, rowCount)
		b.readBatch = func() (int64, int, error) {
			return colReader.(*file.BooleanColumnChunkReader).ReadBatch(rowCount, buffers.boolBuf, b.defLevels, buffers.repLevels)
		}
		b.copyValue = func(rowIdx, valIdx int) {
			b.boolValues[rowIdx] = buffers.boolBuf[valIdx]
		}

	case parquet.Types.Int32:
		b.int32Values = make([]int32, rowCount)
		b.readBatch = func() (int64, int, error) {
			return colReader.(*file.Int32ColumnChunkReader).ReadBatch(rowCount, buffers.int32Buf, b.defLevels, buffers.repLevels)
		}
		b.copyValue = func(rowIdx, valIdx int) {
			b.int32Values[rowIdx] = buffers.int32Buf[valIdx]
		}

	case parquet.Types.Int64:
		b.int64Values = make([]int64, rowCount)
		b.readBatch = func() (int64, int, error) {
			return colReader.(*file.Int64ColumnChunkReader).ReadBatch(rowCount, buffers.int64Buf, b.defLevels, buffers.repLevels)
		}
		b.copyValue = func(rowIdx, valIdx int) {
			b.int64Values[rowIdx] = buffers.int64Buf[valIdx]
		}

	case parquet.Types.Int96:
		// INT96 is deprecated but still widely used for timestamps in Spark-generated files.
		// Format: 12 bytes = 8 bytes nanoseconds since midnight + 4 bytes Julian day number.
		// We read it as Int96 and will convert to timestamp during data processing.
		int96Buf := make([]parquet.Int96, rowCount)
		b.int96Values = make([]parquet.Int96, rowCount)
		b.readBatch = func() (int64, int, error) {
			return colReader.(*file.Int96ColumnChunkReader).ReadBatch(rowCount, int96Buf, b.defLevels, buffers.repLevels)
		}
		b.copyValue = func(rowIdx, valIdx int) {
			// Copy the Int96 value
			b.int96Values[rowIdx] = int96Buf[valIdx]
		}

	case parquet.Types.Float:
		b.float32Values = make([]float32, rowCount)
		b.readBatch = func() (int64, int, error) {
			return colReader.(*file.Float32ColumnChunkReader).ReadBatch(rowCount, buffers.float32Buf, b.defLevels, buffers.repLevels)
		}
		b.copyValue = func(rowIdx, valIdx int) {
			b.float32Values[rowIdx] = buffers.float32Buf[valIdx]
		}

	case parquet.Types.Double:
		b.float64Values = make([]float64, rowCount)
		b.readBatch = func() (int64, int, error) {
			return colReader.(*file.Float64ColumnChunkReader).ReadBatch(rowCount, buffers.float64Buf, b.defLevels, buffers.repLevels)
		}
		b.copyValue = func(rowIdx, valIdx int) {
			b.float64Values[rowIdx] = buffers.float64Buf[valIdx]
		}

	case parquet.Types.ByteArray:
		byteArrayBuf := make([]parquet.ByteArray, rowCount)
		b.byteArrayValues = make([][]byte, rowCount)
		b.readBatch = func() (int64, int, error) {
			return colReader.(*file.ByteArrayColumnChunkReader).ReadBatch(rowCount, byteArrayBuf, b.defLevels, buffers.repLevels)
		}
		b.copyValue = func(rowIdx, valIdx int) {
			// Copy bytes since the buffer may be reused
			copied := make([]byte, len(byteArrayBuf[valIdx]))
			copy(copied, byteArrayBuf[valIdx])
			b.byteArrayValues[rowIdx] = copied
		}

	case parquet.Types.FixedLenByteArray:
		fixedBuf := make([]parquet.FixedLenByteArray, rowCount)
		b.fixedLenByteArrayValues = make([]parquet.FixedLenByteArray, rowCount)
		b.readBatch = func() (int64, int, error) {
			return colReader.(*file.FixedLenByteArrayColumnChunkReader).ReadBatch(rowCount, fixedBuf, b.defLevels, buffers.repLevels)
		}
		b.copyValue = func(rowIdx, valIdx int) {
			// Copy bytes since the buffer may be reused
			copied := make([]byte, len(fixedBuf[valIdx]))
			copy(copied, fixedBuf[valIdx])
			b.fixedLenByteArrayValues[rowIdx] = parquet.FixedLenByteArray(copied)
		}

	default:
		return nil, errors.Newf(
			"unsupported Parquet physical type: %s", physicalType.String())
	}

	return b, nil
}

// Read populates the batch by reading from the Parquet column reader.
// It expands compacted values into full array with null markers.
//
// For nullable columns, valuesRead < numRead because NULLs don't have values in the buffer.
// The reader and copier functions are already configured in the constructor.
//
// Example: If we read 5 rows with definition levels [1, 0, 1, 1, 0] and maxDefLevel=1:
//   - Rows 1, 3, 4 have values (valuesRead=3)
//   - Rows 2, 5 are NULL
//   - The batch will have isNull=[false, true, false, false, true]
//   - Values at indices 1,3,4 will be copied from the read buffer
func (b *parquetColumnBatch) Read() error {
	numRead, valuesRead, err := b.readBatch()
	if err != nil {
		return err
	}
	if numRead != b.rowCount {
		return errors.Newf("expected %d rows, got %d", b.rowCount, numRead)
	}
	valIdx := 0
	for i := int64(0); i < numRead; i++ {
		if b.maxDefLevel > 0 && b.defLevels[i] < b.maxDefLevel {
			b.isNull[i] = true
		} else {
			if valIdx >= valuesRead {
				return errors.Newf("values index %d exceeds valuesRead %d", valIdx, valuesRead)
			}
			b.isNull[i] = false
			b.copyValue(int(i), valIdx)
			valIdx++
		}
	}
	return nil
}
