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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlrun

import (
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rng, _ := randutil.NewPseudoRand()
	for _, numRows := range []int{0, 1, 10, 13, 15} {
		for _, numCols := range []int{1, 3} {
			for _, rowsPerChunk := range []int{1, 2, 5} {
				t.Run(fmt.Sprintf("%d-%d-%d", numRows, numCols, rowsPerChunk), func(t *testing.T) {
					var a sqlbase.DatumAlloc
					var spec ValuesCoreSpec
					spec.Columns = make([]DatumInfo, numCols)
					for i := range spec.Columns {
						spec.Columns[i].Type = sqlbase.RandColumnType(rng)
						spec.Columns[i].Encoding = sqlbase.DatumEncoding_VALUE
					}

					// Generate random rows.
					inRows := make(sqlbase.EncDatumRows, numRows)
					for i := 0; i < numRows; i++ {
						inRows[i] = make(sqlbase.EncDatumRow, numCols)
						for j, info := range spec.Columns {
							d := sqlbase.RandDatum(rng, info.Type, true)
							inRows[i][j] = sqlbase.DatumToEncDatum(info.Type, d)
						}
					}

					// Encode the rows.
					for i := 0; i < numRows; {
						var buf []byte
						for end := i + rowsPerChunk; i < numRows && i < end; i++ {
							for j, info := range spec.Columns {
								var err error
								buf, err = inRows[i][j].Encode(&a, info.Encoding, buf)
								if err != nil {
									t.Fatal(err)
								}
							}
						}
						spec.RawBytes = append(spec.RawBytes, buf)
					}

					out := &RowBuffer{}
					flowCtx := FlowCtx{}

					v, err := newValuesProcessor(&flowCtx, &spec, &PostProcessSpec{}, out)
					if err != nil {
						t.Fatal(err)
					}
					v.Run(context.Background(), nil)
					if !out.ProducerClosed {
						t.Fatalf("output RowReceiver not closed")
					}

					var res sqlbase.EncDatumRows
					for {
						row, meta := out.Next()
						if !meta.Empty() {
							t.Fatalf("unexpected metadata: %v", meta)
						}
						if row == nil {
							break
						}
						res = append(res, row)
					}

					if len(res) != numRows {
						t.Fatalf("incorrect number of rows %d, expected %d", len(res), numRows)
					}

					evalCtx := &parser.EvalContext{}
					for i := 0; i < numRows; i++ {
						if len(res[i]) != numCols {
							t.Fatalf("row %d incorrect length %d, expected %d", i, len(res[i]), numCols)
						}
						for j, res := range res[i] {
							cmp, err := res.Compare(&a, evalCtx, &inRows[i][j])
							if err != nil {
								t.Fatal(err)
							}
							if cmp != 0 {
								t.Errorf("row %d, column %d: received %s, expected %s", i, j, &res, &inRows[i][j])
							}
						}
					}
				})
			}
		}
	}
}
