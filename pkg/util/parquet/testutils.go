// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parquet

import (
	"math"
	"os"
	"testing"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ReadFileAndVerifyDatums reads the parquetFile and first asserts its metadata
// matches the metadata from the writer. Then, it reads the file and asserts
// that it's data matches writtenDatums.
func ReadFileAndVerifyDatums(
	t *testing.T,
	parquetFile string,
	numRows int,
	numCols int,
	writer *Writer,
	writtenDatums [][]tree.Datum,
) {
	f, err := os.Open(parquetFile)
	require.NoError(t, err)

	reader, err := file.NewParquetReader(f)
	require.NoError(t, err)

	assert.Equal(t, reader.NumRows(), int64(numRows))
	assert.Equal(t, reader.MetaData().Schema.NumColumns(), numCols)

	numRowGroups := int(math.Ceil(float64(numRows) / float64(writer.cfg.maxRowGroupLength)))
	assert.EqualValues(t, numRowGroups, reader.NumRowGroups())

	readDatums := make([][]tree.Datum, numRows)
	for i := 0; i < numRows; i++ {
		readDatums[i] = make([]tree.Datum, numCols)
	}

	startingRowIdx := 0
	for rg := 0; rg < reader.NumRowGroups(); rg++ {
		rgr := reader.RowGroup(rg)
		rowsInRowGroup := rgr.NumRows()

		for colIdx := 0; colIdx < numCols; colIdx++ {
			col, err := rgr.Column(colIdx)
			require.NoError(t, err)

			dec := writer.sch.cols[colIdx].decoder
			typ := writer.sch.cols[colIdx].typ

			// Based on how we define schemas, we can detect an array by seeing if the
			// primitive col reader has a max repetition level of 1. See comments above
			// arrayEntryRepLevel for more info.
			isArray := col.Descriptor().MaxRepetitionLevel() == 1

			switch col.Type() {
			case parquet.Types.Boolean:
				colDatums, read, err := readBatch(col, make([]bool, 1), dec, typ, isArray)
				require.NoError(t, err)
				require.Equal(t, rowsInRowGroup, read)
				decodeValuesIntoDatumsHelper(colDatums, readDatums, colIdx, startingRowIdx)
			case parquet.Types.Int32:
				colDatums, read, err := readBatch(col, make([]int32, 1), dec, typ, isArray)
				require.NoError(t, err)
				require.Equal(t, rowsInRowGroup, read)
				decodeValuesIntoDatumsHelper(colDatums, readDatums, colIdx, startingRowIdx)
			case parquet.Types.Int64:
				colDatums, read, err := readBatch(col, make([]int64, 1), dec, typ, isArray)
				require.NoError(t, err)
				require.Equal(t, rowsInRowGroup, read)
				decodeValuesIntoDatumsHelper(colDatums, readDatums, colIdx, startingRowIdx)
			case parquet.Types.Int96:
				panic("unimplemented")
			case parquet.Types.Float:
				panic("unimplemented")
			case parquet.Types.Double:
				panic("unimplemented")
			case parquet.Types.ByteArray:
				colDatums, read, err := readBatch(col, make([]parquet.ByteArray, 1), dec, typ, isArray)
				require.NoError(t, err)
				require.Equal(t, rowsInRowGroup, read)
				decodeValuesIntoDatumsHelper(colDatums, readDatums, colIdx, startingRowIdx)
			case parquet.Types.FixedLenByteArray:
				colDatums, read, err := readBatch(col, make([]parquet.FixedLenByteArray, 1), dec, typ, isArray)
				require.NoError(t, err)
				require.Equal(t, rowsInRowGroup, read)
				decodeValuesIntoDatumsHelper(colDatums, readDatums, colIdx, startingRowIdx)
			}
		}
		startingRowIdx += int(rowsInRowGroup)
	}
	require.NoError(t, reader.Close())

	for i := 0; i < numRows; i++ {
		for j := 0; j < numCols; j++ {
			validateDatum(t, writtenDatums[i][j], readDatums[i][j])
		}
	}
}

type batchReader[T parquetDatatypes] interface {
	ReadBatch(batchSize int64, values []T, defLvls []int16, repLvls []int16) (total int64, valuesRead int, err error)
}

// readBatch reads all the datums in a row group for a column.
func readBatch[T parquetDatatypes](
	r file.ColumnChunkReader, valueAlloc []T, dec decoder, typ *types.T, isArray bool,
) (tree.Datums, int64, error) {
	br, ok := r.(batchReader[T])
	if !ok {
		return nil, 0, errors.AssertionFailedf("expected batchReader for type %T, but found %T instead", valueAlloc, r)
	}

	result := make([]tree.Datum, 0)
	defLevels := [1]int16{}
	repLevels := [1]int16{}

	for {
		numRowsRead, _, err := br.ReadBatch(1, valueAlloc, defLevels[:], repLevels[:])
		if err != nil {
			return nil, 0, err
		}
		if numRowsRead == 0 {
			break
		}

		if isArray {
			// Replevel 0 indicates the start of a new array.
			if repLevels[0] == 0 {
				// Replevel 0, Deflevel 0 represents a NULL array.
				if defLevels[0] == 0 {
					result = append(result, tree.DNull)
					continue
				}
				arrDatum := tree.NewDArray(typ)
				result = append(result, arrDatum)
				// Replevel 0, Deflevel 1 represents an array which is empty.
				if defLevels[0] == 1 {
					continue
				}
			}
			currentArrayDatum := result[len(result)-1].(*tree.DArray)
			// Deflevel 2 represents a null value in an array.
			if defLevels[0] == 2 {
				currentArrayDatum.Array = append(currentArrayDatum.Array, tree.DNull)
				continue
			}
			// Deflevel 3 represents a non-null datum in an array.
			d, err := decode(dec, valueAlloc[0])
			if err != nil {
				return nil, 0, err
			}
			currentArrayDatum.Array = append(currentArrayDatum.Array, d)
		} else {
			// Deflevel 0 represents a null value
			// Deflevel 1 represents a non-null value
			d := tree.DNull
			if defLevels[0] != 0 {
				d, err = decode(dec, valueAlloc[0])
				if err != nil {
					return nil, 0, err
				}
			}
			result = append(result, d)
		}
	}

	return result, int64(len(result)), nil
}

func decodeValuesIntoDatumsHelper(
	colDatums []tree.Datum, datumRows [][]tree.Datum, colIdx int, startingRowIdx int,
) {
	for rowOffset, datum := range colDatums {
		datumRows[startingRowIdx+rowOffset][colIdx] = datum
	}
}

// validateDatum validates that the "contents" of the expected datum matches the
// contents of the actual datum. For example, when validating two arrays, we
// only compare the datums in the arrays. We do not compare CRDB-specific
// metadata fields such as (tree.DArray).HasNulls or (tree.DArray).HasNonNulls.
//
// The reason for this special comparison that the parquet format is presently
// used in an export use case, so we only need to test roundtripability with
// data end users see. We do not need to check roundtripability of internal CRDB data.
func validateDatum(t *testing.T, expected tree.Datum, actual tree.Datum) {
	switch expected.ResolvedType().Family() {
	case types.ArrayFamily:
		arr1 := expected.(*tree.DArray).Array
		arr2 := actual.(*tree.DArray).Array
		require.Equal(t, len(arr1), len(arr2))
		for i := 0; i < len(arr1); i++ {
			validateDatum(t, arr1[i], arr2[i])
		}
	default:
		require.Equal(t, expected, actual)
	}
}
