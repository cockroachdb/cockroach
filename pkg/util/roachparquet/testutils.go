// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachparquet

import (
	"os"
	"testing"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ReadFileAndVerifyDatums reads the parquetFile and first asserts its metadata matches
// numRows, numCols, and numRowGroups. Then, it reads the file and asserts that it's data
// matches writtenDatums.
func ReadFileAndVerifyDatums(
	t *testing.T,
	parquetFile string,
	numRows int,
	numCols int,
	numRowGroups int,
	sch *SchemaDefinition,
	writtenDatums [][]tree.Datum,
) {
	f, err := os.Open(parquetFile)
	require.NoError(t, err)

	reader, err := file.NewParquetReader(f)
	require.NoError(t, err)

	assert.Equal(t, reader.NumRows(), int64(numRows))
	assert.Equal(t, reader.MetaData().Schema.NumColumns(), numCols)
	// If this version ever changes, it should be included in a release note.
	// The version is written to the file, but it's possible that an end user
	// may overlook the version when trying to read it.
	assert.Equal(t, reader.MetaData().Version().String(), "v2.6")
	assert.EqualValues(t, numRowGroups, reader.NumRowGroups())

	readDatums := make([][]tree.Datum, numRows)
	for i := 0; i < numRows; i++ {
		readDatums[i] = make([]tree.Datum, numCols)
	}

	startingRowIdx := 0
	for rg := 0; rg < reader.NumRowGroups(); rg++ {
		rgr := reader.RowGroup(rg)
		rowsInRowGroup := rgr.NumRows()
		defLevels := make([]int16, rowsInRowGroup)

		for colIdx := 0; colIdx < numCols; colIdx++ {
			col, err := rgr.Column(colIdx)
			require.NoError(t, err)

			dec := sch.cols[colIdx].decoder

			switch col.Type() {
			case parquet.Types.Boolean:
				values := make([]bool, rowsInRowGroup)
				readBatchHelper(t, col, rowsInRowGroup, values, defLevels)
				decodeValuesIntoDatumsHelper(t, readDatums, colIdx, startingRowIdx, dec, values, defLevels)
			case parquet.Types.Int32:
				values := make([]int32, numRows)
				readBatchHelper(t, col, rowsInRowGroup, values, defLevels)
				decodeValuesIntoDatumsHelper(t, readDatums, colIdx, startingRowIdx, dec, values, defLevels)
			case parquet.Types.Int64:
				values := make([]int64, rowsInRowGroup)
				readBatchHelper(t, col, rowsInRowGroup, values, defLevels)
				decodeValuesIntoDatumsHelper(t, readDatums, colIdx, startingRowIdx, dec, values, defLevels)
			case parquet.Types.Int96:
				panic("unimplemented")
			case parquet.Types.Float:
				panic("unimplemented")
			case parquet.Types.Double:
				panic("unimplemented")
			case parquet.Types.ByteArray:
				values := make([]parquet.ByteArray, rowsInRowGroup)
				readBatchHelper(t, col, rowsInRowGroup, values, defLevels)
				decodeValuesIntoDatumsHelper(t, readDatums, colIdx, startingRowIdx, dec, values, defLevels)
			case parquet.Types.FixedLenByteArray:
				values := make([]parquet.FixedLenByteArray, rowsInRowGroup)
				readBatchHelper(t, col, rowsInRowGroup, values, defLevels)
				decodeValuesIntoDatumsHelper(t, readDatums, colIdx, startingRowIdx, dec, values, defLevels)
			}
		}
		startingRowIdx += int(rowsInRowGroup)
	}
	require.NoError(t, reader.Close())

	for i := 0; i < numRows; i++ {
		for j := 0; j < numCols; j++ {
			assert.Equal(t, writtenDatums[i][j], readDatums[i][j])
		}
	}
}

func readBatchHelper[T parquetDatatypes](
	t *testing.T, r file.ColumnChunkReader, expectedrowsInRowGroup int64, values []T, defLvls []int16,
) {
	numRead, err := readBatch(r, expectedrowsInRowGroup, values, defLvls)
	require.NoError(t, err)
	require.Equal(t, numRead, expectedrowsInRowGroup)
}

type batchReader[T parquetDatatypes] interface {
	ReadBatch(batchSize int64, values []T, defLvls []int16, repLvls []int16) (total int64, valuesRead int, err error)
}

func readBatch[T parquetDatatypes](
	r file.ColumnChunkReader, batchSize int64, values []T, defLvls []int16,
) (int64, error) {
	br, ok := r.(batchReader[T])
	if !ok {
		return 0, errors.AssertionFailedf("expected batchReader of type %T, but found %T instead", values, r)
	}
	numRowsRead, _, err := br.ReadBatch(batchSize, values, defLvls, nil)
	return numRowsRead, err
}

func decodeValuesIntoDatumsHelper[T parquetDatatypes](
	t *testing.T,
	datums [][]tree.Datum,
	colIdx int,
	startingRowIdx int,
	dec decoder,
	values []T,
	defLevels []int16,
) {
	var err error
	// If the defLevel of a datum is 0, parquet will not write it to the column.
	// Use valueReadIdx to only read from the front of the values array, where datums are defined.
	valueReadIdx := 0
	for rowOffset, defLevel := range defLevels {
		d := tree.DNull
		if defLevel != 0 {
			d, err = decode(dec, values[valueReadIdx])
			require.NoError(t, err)
			valueReadIdx++
		}
		datums[startingRowIdx+rowOffset][colIdx] = d
	}
}
