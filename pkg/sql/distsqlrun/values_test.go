// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// generateValueSpec generates a ValuesCoreSpec that encodes the given rows. We pass
// the types as well because zero rows are allowed.
func generateValuesSpec(
	colTypes []sqlbase.ColumnType, rows sqlbase.EncDatumRows, rowsPerChunk int,
) (ValuesCoreSpec, error) {
	var spec ValuesCoreSpec
	spec.Columns = make([]DatumInfo, len(colTypes))
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
					return ValuesCoreSpec{}, err
				}
			}
		}
		spec.RawBytes = append(spec.RawBytes, buf)
	}
	return spec, nil
}

func TestValues(t *testing.T) {
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
					flowCtx := FlowCtx{
						Ctx:      context.Background(),
						Settings: st,
					}

					v, err := newValuesProcessor(&flowCtx, &spec, &PostProcessSpec{}, out)
					if err != nil {
						t.Fatal(err)
					}
					v.Run(nil)
					if !out.ProducerClosed {
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

					evalCtx := tree.NewTestingEvalContext(st)
					defer evalCtx.Stop(context.Background())
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
