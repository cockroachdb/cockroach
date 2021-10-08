// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package randgen

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// RandDatumEncoding returns a random DatumEncoding value.
func RandDatumEncoding(rng *rand.Rand) descpb.DatumEncoding {
	return descpb.DatumEncoding(rng.Intn(len(descpb.DatumEncoding_value)))
}

// RandEncDatum generates a random EncDatum (of a random type).
func RandEncDatum(rng *rand.Rand) (rowenc.EncDatum, *types.T) {
	typ := RandEncodableType(rng)
	datum := RandDatum(rng, typ, true /* nullOk */)
	return rowenc.DatumToEncDatum(typ, datum), typ
}

// RandSortingEncDatumSlice generates a slice of random EncDatum values of the
// same random type which is key-encodable.
func RandSortingEncDatumSlice(rng *rand.Rand, numVals int) ([]rowenc.EncDatum, *types.T) {
	typ := RandSortingType(rng)
	vals := make([]rowenc.EncDatum, numVals)
	for i := range vals {
		vals[i] = rowenc.DatumToEncDatum(typ, RandDatum(rng, typ, true))
	}
	return vals, typ
}

// RandSortingEncDatumSlices generates EncDatum slices, each slice with values of the same
// random type which is key-encodable.
func RandSortingEncDatumSlices(
	rng *rand.Rand, numSets, numValsPerSet int,
) ([][]rowenc.EncDatum, []*types.T) {
	vals := make([][]rowenc.EncDatum, numSets)
	types := make([]*types.T, numSets)
	for i := range vals {
		val, typ := RandSortingEncDatumSlice(rng, numValsPerSet)
		vals[i], types[i] = val, typ
	}
	return vals, types
}

// RandEncDatumRowOfTypes generates a slice of random EncDatum values for the
// corresponding type in types.
func RandEncDatumRowOfTypes(rng *rand.Rand, types []*types.T) rowenc.EncDatumRow {
	vals := make([]rowenc.EncDatum, len(types))
	for i := range types {
		vals[i] = rowenc.DatumToEncDatum(types[i], RandDatum(rng, types[i], true))
	}
	return vals
}

// RandEncDatumRows generates EncDatumRows where all rows follow the same random
// []ColumnType structure.
func RandEncDatumRows(rng *rand.Rand, numRows, numCols int) (rowenc.EncDatumRows, []*types.T) {
	types := RandEncodableColumnTypes(rng, numCols)
	return RandEncDatumRowsOfTypes(rng, numRows, types), types
}

// RandEncDatumRowsOfTypes generates EncDatumRows, each row with values of the
// corresponding type in types.
func RandEncDatumRowsOfTypes(rng *rand.Rand, numRows int, types []*types.T) rowenc.EncDatumRows {
	vals := make(rowenc.EncDatumRows, numRows)
	for i := range vals {
		vals[i] = RandEncDatumRowOfTypes(rng, types)
	}
	return vals
}

// IntEncDatum returns an EncDatum representation of DInt(i).
func IntEncDatum(i int) rowenc.EncDatum {
	return rowenc.EncDatum{Datum: tree.NewDInt(tree.DInt(i))}
}

// NullEncDatum returns and EncDatum representation of tree.DNull.
func NullEncDatum() rowenc.EncDatum {
	return rowenc.EncDatum{Datum: tree.DNull}
}

// GenEncDatumRowsInt converts rows of ints to rows of EncDatum DInts.
// If an int is negative, the corresponding value is NULL.
func GenEncDatumRowsInt(inputRows [][]int) rowenc.EncDatumRows {
	rows := make(rowenc.EncDatumRows, len(inputRows))
	for i, inputRow := range inputRows {
		for _, x := range inputRow {
			if x < 0 {
				rows[i] = append(rows[i], NullEncDatum())
			} else {
				rows[i] = append(rows[i], IntEncDatum(x))
			}
		}
	}
	return rows
}

// MakeIntRows constructs a numRows x numCols table where rows[i][j] = i + j.
func MakeIntRows(numRows, numCols int) rowenc.EncDatumRows {
	rows := make(rowenc.EncDatumRows, numRows)
	for i := range rows {
		rows[i] = make(rowenc.EncDatumRow, numCols)
		for j := 0; j < numCols; j++ {
			rows[i][j] = IntEncDatum(i + j)
		}
	}
	return rows
}

// MakeRandIntRows constructs a numRows x numCols table where the values are random.
func MakeRandIntRows(rng *rand.Rand, numRows int, numCols int) rowenc.EncDatumRows {
	rows := make(rowenc.EncDatumRows, numRows)
	for i := range rows {
		rows[i] = make(rowenc.EncDatumRow, numCols)
		for j := 0; j < numCols; j++ {
			rows[i][j] = IntEncDatum(rng.Int())
		}
	}
	return rows
}

// MakeRandIntRowsInRange constructs a numRows * numCols table where the values
// are random integers in the range [0, maxNum).
func MakeRandIntRowsInRange(
	rng *rand.Rand, numRows int, numCols int, maxNum int, nullProbability float64,
) rowenc.EncDatumRows {
	rows := make(rowenc.EncDatumRows, numRows)
	for i := range rows {
		rows[i] = make(rowenc.EncDatumRow, numCols)
		for j := 0; j < numCols; j++ {
			rows[i][j] = IntEncDatum(rng.Intn(maxNum))
			if rng.Float64() < nullProbability {
				rows[i][j] = NullEncDatum()
			}
		}
	}
	return rows
}

// MakeRepeatedIntRows constructs a numRows x numCols table where blocks of n
// consecutive rows have the same value.
func MakeRepeatedIntRows(n int, numRows int, numCols int) rowenc.EncDatumRows {
	rows := make(rowenc.EncDatumRows, numRows)
	for i := range rows {
		rows[i] = make(rowenc.EncDatumRow, numCols)
		for j := 0; j < numCols; j++ {
			rows[i][j] = IntEncDatum(i/n + j)
		}
	}
	return rows
}
