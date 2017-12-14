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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

func TestMemRowBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	nBuffers := 100
	nRows := 100

	buffers := make([]*memRowBuffer, nBuffers)

	errChan := make(chan error)
	doneChan := make(chan struct{})

	// Rows to plumb through the pipeline of row buffers.
	rows := make(sqlbase.EncDatumRows, nRows)
	for i := range rows {
		rows[i] = sqlbase.EncDatumRow{{Datum: tree.NewDInt(tree.DInt(i))}}
	}

	evalCtx := tree.NewTestingEvalContext()
	da := &sqlbase.DatumAlloc{}

	for i := range buffers {
		buffers[i] = makeMemRowBuffer(context.TODO(), evalCtx, []sqlbase.ColumnType{columnTypeInt})
	}

	for i := range buffers {
		go func(i int) {
			defer func() {
				doneChan <- struct{}{}
			}()

			curRowIdx := 0
			cur := RowSource(buffers[i])

			var next *memRowBuffer
			if i+1 != len(buffers) {
				next = buffers[i+1]
			}
			for {
				row, meta := cur.Next()
				if !meta.Empty() {
					errChan <- errors.Errorf("%dth buffer: expected empty producer metadata: got %v\n", i, meta)
					break
				}
				if row == nil {
					if curRowIdx < len(rows) {
						errChan <- errors.Errorf("%dth buffer: expected to get %d rows, got %d\n", i, len(rows), curRowIdx)
					} else if next != nil {
						next.ProducerDone()
						cur.ConsumerDone()
						cur.ConsumerClosed()
					}

					break
				}

				comp, err := rows[curRowIdx].Compare(
					[]sqlbase.ColumnType{columnTypeInt},
					da,
					sqlbase.ColumnOrdering{{ColIdx: 0, Direction: encoding.Ascending}},
					evalCtx,
					row,
				)
				if err != nil {
					errChan <- err
					break
				}

				if comp != 0 {
					errChan <- errors.Errorf("%dth buffer: un-expected current %dth row: %v\n", i, curRowIdx, row)
					break
				}

				curRowIdx++

				if next != nil {
					status, err := next.addRow(row)
					if err != nil {
						errChan <- err
						break
					}

					if status != NeedMoreRows {
						errChan <- errors.Errorf("%dth buffer: expected NeedMoreRows consumer status, got %v\n", i, status)
						break
					}
				}
			}
		}(i)
	}

	for _, row := range rows {
		status, err := buffers[0].addRow(row)
		if err != nil {
			t.Fatal(err)
		}
		if status != NeedMoreRows {
			t.Fatalf("expected NeedMoreRows consumer status, got %v\n", status)
		}
	}
	buffers[0].ProducerDone()

	count := 0
	for {
		select {
		case err := <-errChan:
			close(doneChan)
			t.Fatal(err)
		case <-doneChan:
			count++
			if count == nBuffers {
				close(errChan)
				return
			}
		}
	}
}
