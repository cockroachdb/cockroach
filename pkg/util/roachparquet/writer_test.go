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
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

var supportedTypes = []*types.T{
	types.Int,
	types.Bool,
	types.String,
	types.Decimal,
	types.Uuid,
	types.Timestamp,
}

type colSchema struct {
	name string
	typ  *types.T
}

func randType(rng *rand.Rand) *types.T {
	return supportedTypes[rng.Intn(len(supportedTypes))]
}

func makeRandDatums(numRows int, sch []colSchema, rng *rand.Rand) [][]tree.Datum {
	datums := make([][]tree.Datum, numRows)
	for i := 0; i < numRows; i++ {
		datums[i] = make([]tree.Datum, len(sch))
		for j := 0; j < len(sch); j++ {
			datums[i][j] = randgen.RandDatum(rng, sch[j].typ, true)
		}
	}
	return datums
}

func makeRandSchema(numRows int, numCols int, rng *rand.Rand) []colSchema {
	sch := make([]colSchema, numCols)
	for i := 0; i < numCols; i++ {
		typ := randType(rng)
		sch[i].typ = randType(rng)
		sch[i].name = fmt.Sprintf("%s%d", typ.Name(), i)
	}
	return sch
}

func TestRandomDatums(t *testing.T) {
	seed := rand.NewSource(timeutil.Now().UnixNano())
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	t.Logf("random seed %d", seed.Int63())

	numRows := 25
	numCols := 10
	maxRowGroupSize := 4

	sch := makeRandSchema(numRows, numCols, rng)
	datums := makeRandDatums(numRows, sch, rng)

	schemaIter := func(f func(colName string, typ *types.T) error) error {
		for _, colSch := range sch {
			if err := f(colSch.name, colSch.typ); err != nil {
				return err
			}
		}
		return nil
	}

	fileName := "TestRandomDatums"
	f, err := os.CreateTemp("", fileName)
	require.NoError(t, err)

	schemaDef, err := NewSchema(schemaIter)
	require.NoError(t, err)

	writer, err := NewWriter(schemaDef, f, int64(maxRowGroupSize))
	require.NoError(t, err)

	for _, row := range datums {
		datumIter := func(f func(tree.Datum) error) error {
			for _, d := range row {
				if err := f(d); err != nil {
					return err
				}
			}
			return nil
		}

		err := writer.AddData(datumIter)
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	numRowGroups := int(math.Ceil(float64(numRows) / float64(maxRowGroupSize)))

	ReadFileAndVerifyDatums(t, f.Name(), numRows, numCols, numRowGroups, schemaDef, datums)
}

func TestBasicDatums(t *testing.T) {
	for _, tc := range []struct {
		name   string
		sch    []colSchema
		datums func() ([][]tree.Datum, error)
	}{
		{
			name: "bool",
			sch: []colSchema{
				{name: "a", typ: types.Bool}, {name: "b", typ: types.Bool}, {name: "c", typ: types.Bool},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{tree.DBoolFalse, tree.DBoolTrue, tree.DNull},
				}, nil
			},
		},
		{
			name: "string",
			sch: []colSchema{
				{name: "a", typ: types.String}, {name: "b", typ: types.String}, {name: "c", typ: types.String},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{tree.NewDString("a"), tree.NewDString(""), tree.DNull}}, nil
			},
		},
		{
			name: "timestamp",
			sch: []colSchema{
				{name: "a", typ: types.Timestamp}, {name: "b", typ: types.Timestamp}, {name: "c", typ: types.Timestamp},
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
			name: "bool",
			sch: []colSchema{
				{name: "a", typ: types.Bool}, {name: "b", typ: types.Bool}, {name: "c", typ: types.Bool},
			},
			datums: func() ([][]tree.Datum, error) {
				return [][]tree.Datum{
					{tree.DBoolFalse, tree.DBoolTrue, tree.DNull},
				}, nil
			},
		},
		{
			name: "int",
			sch: []colSchema{
				{name: "a", typ: types.Int4}, {name: "b", typ: types.Int}, {name: "c",
					typ: types.Int}, {name: "d", typ: types.Int2}, {name: "d", typ: types.Int2},
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
			sch: []colSchema{
				{name: "a", typ: types.Decimal}, {name: "b", typ: types.Decimal}, {name: "c", typ: types.Decimal},
				{name: "d", typ: types.Decimal},
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
			sch: []colSchema{
				{name: "a", typ: types.Uuid}, {name: "b", typ: types.Uuid},
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			datums, err := tc.datums()
			require.NoError(t, err)
			numRows := len(datums)
			numCols := len(datums[0])
			maxRowGroupSize := 2

			schemaIter := func(f func(colName string, typ *types.T) error) error {
				for _, colSch := range tc.sch {
					if err := f(colSch.name, colSch.typ); err != nil {
						return err
					}
				}
				return nil
			}

			fileName := "TestBasicDatums"
			f, err := os.CreateTemp("", fileName)
			require.NoError(t, err)

			schemaDef, err := NewSchema(schemaIter)
			require.NoError(t, err)

			writer, err := NewWriter(schemaDef, f, int64(maxRowGroupSize))
			require.NoError(t, err)

			for _, row := range datums {
				datumIter := func(f func(tree.Datum) error) error {
					for _, d := range row {
						if err := f(d); err != nil {
							return err
						}
					}
					return nil
				}

				err := writer.AddData(datumIter)
				require.NoError(t, err)
			}

			err = writer.Close()
			require.NoError(t, err)

			numRowGroups := int(math.Ceil(float64(numRows) / float64(maxRowGroupSize)))

			ReadFileAndVerifyDatums(t, f.Name(), numRows, numCols, numRowGroups, schemaDef, datums)
		})
	}
}
