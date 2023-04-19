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
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

type colSchema struct {
	columnNames []string
	columnTypes []*types.T
}

func newColSchema(numCols int) *colSchema {
	return &colSchema{
		columnNames: make([]string, numCols),
		columnTypes: make([]*types.T, numCols),
	}
}

// TODO (jayant): once all types are supported, we can use randgen.SeedTypes
// instead of this array.
var supportedTypes = []*types.T{
	types.Int,
	types.Bool,
	types.String,
	types.Decimal,
	types.Uuid,
	types.Timestamp,
}

func init() {
	// Include all array types which are arrays of the scalar types above.
	var arrayTypes []*types.T
	for oid := range types.ArrayOids {
		arrayTyp := types.OidToType[oid]
		for _, typ := range supportedTypes {
			if arrayTyp.InternalType.ArrayContents == typ {
				arrayTypes = append(arrayTypes, arrayTyp)
			}
		}
	}
	supportedTypes = append(supportedTypes, arrayTypes...)
}

func makeRandDatums(numRows int, sch *colSchema, rng *rand.Rand) [][]tree.Datum {
	datums := make([][]tree.Datum, numRows)
	for i := 0; i < numRows; i++ {
		datums[i] = make([]tree.Datum, len(sch.columnTypes))
		for j := 0; j < len(sch.columnTypes); j++ {
			datums[i][j] = randgen.RandDatum(rng, sch.columnTypes[j], true)
		}
	}
	return datums
}

func makeRandSchema(numCols int, rng *rand.Rand) *colSchema {
	sch := newColSchema(numCols)
	for i := 0; i < numCols; i++ {
		sch.columnTypes[i] = supportedTypes[rng.Intn(len(supportedTypes))]
		sch.columnNames[i] = fmt.Sprintf("%s%d", sch.columnTypes[i].Name(), i)
	}
	return sch
}

func TestRandomDatums(t *testing.T) {
	seed := rand.NewSource(timeutil.Now().UnixNano())
	rng := rand.New(seed)
	t.Logf("random seed %d", seed.Int63())

	numRows := 64
	numCols := 128
	maxRowGroupSize := int64(8)

	sch := makeRandSchema(numCols, rng)
	datums := makeRandDatums(numRows, sch, rng)

	fileName := "TestRandomDatums.parquet"
	f, err := os.CreateTemp("", fileName)
	require.NoError(t, err)

	schemaDef, err := NewSchema(sch.columnNames, sch.columnTypes)
	require.NoError(t, err)

	writer, err := NewWriter(schemaDef, f, WithMaxRowGroupLength(maxRowGroupSize))
	require.NoError(t, err)

	for _, row := range datums {
		err = writer.AddRow(row)
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	ReadFileAndVerifyDatums(t, f.Name(), numRows, numCols, writer, datums)
}

func TestBasicDatums(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sch    *colSchema
		datums func() ([][]tree.Datum, error)
	}{
		{
			name: "bool",
			sch: &colSchema{
				columnTypes: []*types.T{types.Bool, types.Bool, types.Bool},
				columnNames: []string{"a", "b", "c"},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{tree.DBoolFalse, tree.DBoolTrue, tree.DNull},
				}, nil
			},
		},
		{
			name: "string",
			sch: &colSchema{
				columnTypes: []*types.T{types.String, types.String, types.String},
				columnNames: []string{"a", "b", "c"},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{tree.NewDString("a"), tree.NewDString(""), tree.DNull}}, nil
			},
		},
		{
			name: "timestamp",
			sch: &colSchema{
				columnTypes: []*types.T{types.Timestamp, types.Timestamp, types.Timestamp},
				columnNames: []string{"a", "b", "c"},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{
						tree.MustMakeDTimestamp(timeutil.Now(), time.Microsecond),
						tree.MustMakeDTimestamp(timeutil.Now(), time.Microsecond),
						tree.DNull,
					},
				}, nil
			},
		},
		{
			name: "int",
			sch: &colSchema{
				columnTypes: []*types.T{types.Int4, types.Int, types.Int, types.Int2, types.Int2},
				columnNames: []string{"a", "b", "c", "d", "e"},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{tree.NewDInt(1 << 16), tree.NewDInt(1 << 32),
						tree.NewDInt(-1 * (1 << 32)), tree.NewDInt(12), tree.DNull},
				}, nil
			},
		},
		{
			name: "decimal",
			sch: &colSchema{
				columnTypes: []*types.T{types.Decimal, types.Decimal, types.Decimal, types.Decimal},
				columnNames: []string{"a", "b", "c", "d"},
			},
			datums: func() ([][]tree.Datum, error) {
				var err error
				datums := make([]tree.Datum, 4)
				if datums[0], err = tree.ParseDDecimal("-1.222"); err != nil {
					return nil, err
				}
				if datums[1], err = tree.ParseDDecimal("-inf"); err != nil {
					return nil, err
				}
				if datums[2], err = tree.ParseDDecimal("inf"); err != nil {
					return nil, err
				}
				if datums[3], err = tree.ParseDDecimal("nan"); err != nil {
					return nil, err
				}
				return [][]tree.Datum{datums}, nil
			},
		},
		{
			name: "uuid",
			sch: &colSchema{
				columnTypes: []*types.T{types.Uuid, types.Uuid},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				uid, err := uuid.FromString("acde070d-8c4c-4f0d-9d8a-162843c10333")
				if err != nil {
					return nil, err
				}
				return [][]tree.Datum{
					{tree.NewDUuid(tree.DUuid{UUID: uid}), tree.DNull},
				}, nil
			},
		},
		{
			name: "array",
			sch: &colSchema{
				columnTypes: []*types.T{types.IntArray, types.IntArray},
				columnNames: []string{"a", "b"},
			},
			datums: func() ([][]tree.Datum, error) {
				da := tree.NewDArray(types.Int)
				da.Array = tree.Datums{tree.NewDInt(0), tree.NewDInt(1)}
				da2 := tree.NewDArray(types.Int)
				da2.Array = tree.Datums{tree.NewDInt(2), tree.DNull}
				da3 := tree.NewDArray(types.Int)
				da3.Array = tree.Datums{}
				return [][]tree.Datum{
					{da, da2}, {da3, tree.DNull},
				}, nil
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			datums, err := tc.datums()
			require.NoError(t, err)
			numRows := len(datums)
			numCols := len(datums[0])
			maxRowGroupSize := int64(2)

			fileName := "TestBasicDatums.parquet"
			f, err := os.CreateTemp("", fileName)
			require.NoError(t, err)

			schemaDef, err := NewSchema(tc.sch.columnNames, tc.sch.columnTypes)
			require.NoError(t, err)

			writer, err := NewWriter(schemaDef, f, WithMaxRowGroupLength(maxRowGroupSize))
			require.NoError(t, err)

			for _, row := range datums {
				err = writer.AddRow(row)
				require.NoError(t, err)
			}

			err = writer.Close()
			require.NoError(t, err)

			ReadFileAndVerifyDatums(t, f.Name(), numRows, numCols, writer, datums)
		})
	}
}

func TestInvalidWriterUsage(t *testing.T) {
	colNames := []string{"col1", "col2"}
	colTypes := []*types.T{types.Bool, types.Bool}
	datum := tree.DBoolTrue

	schemaDef, err := NewSchema(colNames, colTypes)
	require.NoError(t, err)

	t.Run("cannot write an invalid number of columns outside of row size", func(t *testing.T) {
		buf := bytes.Buffer{}
		writer, err := NewWriter(schemaDef, &buf)
		require.NoError(t, err)

		err = writer.AddRow([]tree.Datum{datum, datum, datum})
		require.ErrorContains(t, err, "expected 2 datums in row, got 3 datums")

		_ = writer.Close()
	})

	t.Run("cannot write datum of wrong type", func(t *testing.T) {
		buf := bytes.Buffer{}
		writer, err := NewWriter(schemaDef, &buf)
		require.NoError(t, err)

		err = writer.AddRow([]tree.Datum{tree.NewDInt(0), datum})
		require.ErrorContains(t, err, "expected DBool")

		_ = writer.Close()
	})
}

func TestVersions(t *testing.T) {
	schemaDef, err := NewSchema([]string{}, []*types.T{})
	require.NoError(t, err)

	for version := range allowedVersions {
		fileName := "TestVersions.parquet"
		f, err := os.CreateTemp("", fileName)
		require.NoError(t, err)

		writer, err := NewWriter(schemaDef, f, WithVersion(version))
		require.NoError(t, err)

		err = writer.Close()
		require.NoError(t, err)

		f, err = os.Open(f.Name())
		require.NoError(t, err)

		reader, err := file.NewParquetReader(f)
		require.NoError(t, err)

		require.Equal(t, reader.MetaData().Version(), writer.cfg.version)

		err = reader.Close()
		require.NoError(t, err)
	}

	buf := bytes.Buffer{}
	_, err = NewWriter(schemaDef, &buf, WithVersion("invalid"))
	require.Error(t, err)
}
