// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package parquet

import (
	"math/rand"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// BenchmarkParquetWriter benchmarks the Writer.AddRow operation.
func BenchmarkParquetWriter(b *testing.B) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	numCols := 32
	benchmarkTypes := getBenchmarkTypes()

	for i, typ := range benchmarkTypes {
		bench := func(b *testing.B) {
			fileName := "BenchmarkParquetWriter.parquet"
			f, err := os.CreateTemp("", fileName)
			require.NoError(b, err)

			// Slice a single type out of supportedTypes.
			sch := makeRandSchema(numCols,
				func(rng *rand.Rand) *types.T {
					return benchmarkTypes[i]
				}, rng)
			datums := makeRandDatums(1, sch, rng, false)

			schemaDef, err := NewSchema(sch.columnNames, sch.columnTypes)
			require.NoError(b, err)

			writer, err := NewWriter(schemaDef, f)
			require.NoError(b, err)

			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				err = writer.AddRow(datums[0])
				require.NoError(b, err)
			}
			b.StopTimer()

			err = writer.Close()
			require.NoError(b, err)
		}

		b.Run(typ.Name(), bench)
	}
}

func getBenchmarkTypes() []*types.T {
	var typs []*types.T
	// NB: This depends on randgen.SeedTypes containing all scalar
	// types supported by the writer, and all the types below once.
	for _, typ := range randgen.SeedTypes {
		switch typ.Family() {
		case types.AnyFamily, types.TSQueryFamily, types.TSVectorFamily,
			types.VoidFamily, types.PGVectorFamily:
		case types.TupleFamily:
			// Replace Any Tuple with Tuple of Ints with size 5.
			typs = append(typs, types.MakeTuple([]*types.T{
				types.Int, types.Int, types.Int, types.Int, types.Int,
			}))
		case types.ArrayFamily:
			// Replace Any Array with Int Array.
			typs = append(typs, types.IntArray)
		default:
			typs = append(typs, typ)
		}
	}
	return typs
}
