// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randgen

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// RandDatumEncoding returns a random DatumEncoding value.
func RandDatumEncoding(rng *rand.Rand) catenumpb.DatumEncoding {
	return catenumpb.DatumEncoding(rng.Intn(len(catenumpb.DatumEncoding_value)))
}

// RandEncDatum generates a random EncDatum (of a random type).
func RandEncDatum(rng *rand.Rand) (rowenc.EncDatum, *types.T) {
	typ := RandType(rng)
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
	types := RandColumnTypes(rng, numCols)
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

// DatumEncoding_NONE is only for tests. It is not a valid encoding. It is used
// to instruct random datum generators to skip encoding the returned datum(s).
const DatumEncoding_NONE catenumpb.DatumEncoding = -1

func shouldEncode(enc catenumpb.DatumEncoding) bool {
	return enc != DatumEncoding_NONE
}

// GenEncDatumRowsInt converts rows of ints to rows of EncDatum DInts.
// If an int is negative, the corresponding value is NULL.
func GenEncDatumRowsInt(inputRows [][]int, enc catenumpb.DatumEncoding) rowenc.EncDatumRows {
	var a tree.DatumAlloc
	rows := make(rowenc.EncDatumRows, len(inputRows))
	for i, inputRow := range inputRows {
		for _, x := range inputRow {
			var d rowenc.EncDatum
			if x < 0 {
				d = NullEncDatum()
			} else {
				d = IntEncDatum(x)
			}
			if shouldEncode(enc) {
				b, err := d.Encode(types.Int, &a, enc, nil /* appendTo */)
				if err != nil {
					panic(err)
				}
				d = rowenc.EncDatumFromEncoded(enc, b)
			}
			rows[i] = append(rows[i], d)
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

// GenEncDatumRowsFloat converts rows of strings to rows of EncDatum DFloats.
func GenEncDatumRowsFloat(inputRows [][]string, enc catenumpb.DatumEncoding) rowenc.EncDatumRows {
	var a tree.DatumAlloc
	rows := make(rowenc.EncDatumRows, len(inputRows))
	for i, inputRow := range inputRows {
		for _, x := range inputRow {
			f, err := tree.ParseDFloat(x)
			if err != nil {
				panic(errors.AssertionFailedf("could not parse float: %v", x))
			}
			d := rowenc.EncDatum{Datum: f}
			if shouldEncode(enc) {
				b, err := d.Encode(types.Float, &a, enc, nil /* appendTo */)
				if err != nil {
					panic(err)
				}
				d = rowenc.EncDatumFromEncoded(enc, b)
			}
			rows[i] = append(rows[i], d)
		}
	}
	return rows
}

// GenEncDatumRowsString converts rows of strings to rows of EncDatum Strings.
// If a string is empty, the corresponding value is NULL.
func GenEncDatumRowsString(inputRows [][]string, enc catenumpb.DatumEncoding) rowenc.EncDatumRows {
	var a tree.DatumAlloc
	rows := make(rowenc.EncDatumRows, len(inputRows))
	for i, inputRow := range inputRows {
		for _, x := range inputRow {
			var d rowenc.EncDatum
			if x == "" {
				d = NullEncDatum()
			} else {
				d = stringEncDatum(x)
			}
			if shouldEncode(enc) {
				b, err := d.Encode(types.String, &a, enc, nil /* appendTo */)
				if err != nil {
					panic(err)
				}
				d = rowenc.EncDatumFromEncoded(enc, b)
			}
			rows[i] = append(rows[i], d)
		}
	}
	return rows
}

// GenEncDatumRowsCollatedString converts rows of strings to rows of EncDatum
// collated Strings.
// If a string is empty, the corresponding value is NULL.
func GenEncDatumRowsCollatedString(
	inputRows [][]string, typ *types.T, enc catenumpb.DatumEncoding,
) rowenc.EncDatumRows {
	var a tree.DatumAlloc
	rows := make(rowenc.EncDatumRows, len(inputRows))
	for i, inputRow := range inputRows {
		for _, x := range inputRow {
			var d rowenc.EncDatum
			if x == "" {
				d = NullEncDatum()
			} else {
				d = collatedStringEncDatum(x, typ.Locale())
			}
			if shouldEncode(enc) {
				b, err := d.Encode(typ, &a, enc, nil /* appendTo */)
				if err != nil {
					panic(err)
				}
				d = rowenc.EncDatumFromEncoded(enc, b)
			}
			rows[i] = append(rows[i], d)
		}
	}
	return rows
}

// stringEncDatum returns an EncDatum representation of a string.
func stringEncDatum(s string) rowenc.EncDatum {
	return rowenc.EncDatum{Datum: tree.NewDString(s)}
}

// collatedStringEncDatum returns an EncDatum representation of a string.
func collatedStringEncDatum(s string, locale string) rowenc.EncDatum {
	d, err := tree.NewDCollatedString(s, locale, &tree.CollationEnvironment{})
	if err != nil {
		panic(err)
	}
	return rowenc.EncDatum{Datum: d}
}

func bytesEncDatum(b []byte) rowenc.EncDatum {
	return rowenc.EncDatum{Datum: tree.NewDBytes(tree.DBytes(b))}
}

// GenEncDatumRowsBytes converts rows of bytes to rows of EncDatum value encoded
// bytes.
func GenEncDatumRowsBytes(inputRows [][][]byte, enc catenumpb.DatumEncoding) rowenc.EncDatumRows {
	rows := make(rowenc.EncDatumRows, len(inputRows))
	for i, inputRow := range inputRows {
		for _, x := range inputRow {
			d := bytesEncDatum(x)
			if shouldEncode(enc) {
				d = rowenc.EncDatumFromEncoded(catenumpb.DatumEncoding_VALUE, x)
			}
			rows[i] = append(rows[i], d)
		}
	}
	return rows
}
