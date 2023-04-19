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
	"math/rand"
	"os"
	"testing"

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
			sch := makeRandSchema(numCols, benchmarkTypes[i:i+1], rng)
			datums := makeRandDatums(1, sch, rng)

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
	for _, typ := range supportedTypes {
		switch typ.Family() {
		case types.ArrayFamily:
			// Pick out one array type to benchmark arrays.
			if typ.ArrayContents() == types.Int {
				typs = append(typs, typ)
			}
		default:
			typs = append(typs, typ)
		}
	}
	return typs
}
