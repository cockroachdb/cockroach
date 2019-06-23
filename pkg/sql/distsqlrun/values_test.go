// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package distsqlrun

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// generateValueSpec generates a ValuesCoreSpec that encodes the given rows. We pass
// the types as well because zero rows are allowed.
func generateValuesSpec(
	colTypes []types.T, rows sqlbase.EncDatumRows, rowsPerChunk int,
) (distsqlpb.ValuesCoreSpec, error) {
	var spec distsqlpb.ValuesCoreSpec
	spec.Columns = make([]distsqlpb.DatumInfo, len(colTypes))
	for i := range spec.Columns {
		spec.Columns[i].Type = colTypes[i]
		spec.Columns[i].Encoding = sqlbase.DatumEncoding_VALUE
	}

	var a sqlbase.DatumAlloc
	for i := 0; i < len(rows); {
		var buf []byte
		for end := i + rowsPerChunk; i < len(rows) && i < end; i++ {
			for j, info := range spec.Columns {
				var err error
				buf, err = rows[i][j].Encode(&colTypes[j], &a, info.Encoding, buf)
				if err != nil {
					return distsqlpb.ValuesCoreSpec{}, err
				}
			}
		}
		spec.RawBytes = append(spec.RawBytes, buf)
	}
	return spec, nil
}

func TestValuesProcessor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, _ := randutil.NewPseudoRand()
	for _, numRows := range []int{0, 1, 10, 13, 15} {
		for _, numCols := range []int{1, 3} {
			for _, rowsPerChunk := range []int{1, 2, 5} {
				t.Run(fmt.Sprintf("%d-%d-%d", numRows, numCols, rowsPerChunk), func(t *testing.T) {
					inRows, colTypes := sqlbase.RandEncDatumRows(rng, numRows, numCols)

					spec, err := generateValuesSpec(colTypes, inRows, rowsPerChunk)
					if err != nil {
						t.Fatal(err)
					}

					out := &RowBuffer{}
					st := cluster.MakeTestingClusterSettings()
					evalCtx := tree.NewTestingEvalContext(st)
					defer evalCtx.Stop(context.Background())
					flowCtx := FlowCtx{
						Settings: st,
						EvalCtx:  evalCtx,
					}

					v, err := newValuesProcessor(&flowCtx, 0 /* processorID */, &spec, &distsqlpb.PostProcessSpec{}, out)
					if err != nil {
						t.Fatal(err)
					}
					v.Run(context.Background())
					if !out.ProducerClosed() {
						t.Fatalf("output RowReceiver not closed")
					}

					var res sqlbase.EncDatumRows
					for {
						row := out.NextNoMeta(t)
						if row == nil {
							break
						}
						res = append(res, row)
					}

					if len(res) != numRows {
						t.Fatalf("incorrect number of rows %d, expected %d", len(res), numRows)
					}

					var a sqlbase.DatumAlloc
					for i := 0; i < numRows; i++ {
						if len(res[i]) != numCols {
							t.Fatalf("row %d incorrect length %d, expected %d", i, len(res[i]), numCols)
						}
						for j, val := range res[i] {
							cmp, err := val.Compare(&colTypes[j], &a, evalCtx, &inRows[i][j])
							if err != nil {
								t.Fatal(err)
							}
							if cmp != 0 {
								t.Errorf(
									"row %d, column %d: received %s, expected %s",
									i, j, val.String(&colTypes[j]), inRows[i][j].String(&colTypes[j]),
								)
							}
						}
					}
				})
			}
		}
	}
}

func BenchmarkValuesProcessor(b *testing.B) {
	const numCols = 2

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.MakeTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

	flowCtx := FlowCtx{
		Settings: st,
		EvalCtx:  &evalCtx,
	}
	post := distsqlpb.PostProcessSpec{}
	output := RowDisposer{}
	for _, numRows := range []int{1 << 4, 1 << 8, 1 << 12, 1 << 16} {
		for _, rowsPerChunk := range []int{1, 4, 16} {
			b.Run(fmt.Sprintf("rows=%d,chunkSize=%d", numRows, rowsPerChunk), func(b *testing.B) {
				rows := sqlbase.MakeIntRows(numRows, numCols)
				spec, err := generateValuesSpec(sqlbase.TwoIntCols, rows, rowsPerChunk)
				if err != nil {
					b.Fatal(err)
				}

				b.SetBytes(int64(8 * numRows * numCols))
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					v, err := newValuesProcessor(&flowCtx, 0 /* processorID */, &spec, &post, &output)
					if err != nil {
						b.Fatal(err)
					}
					v.Run(context.Background())
				}
			})
		}
	}
}
