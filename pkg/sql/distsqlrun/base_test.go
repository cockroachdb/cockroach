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
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"golang.org/x/net/context"
)

// TODO(asubiotto): Add test and benchmark for RowChannel juggling batches.

// Benchmark a pipeline of RowChannels.
func BenchmarkRowChannelPipeline(b *testing.B) {
	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}

	for _, length := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("length=%d", length), func(b *testing.B) {
			rc := make([]RowChannel, length)
			var wg sync.WaitGroup
			wg.Add(len(rc))

			for i := range rc {
				rc[i].Init([]sqlbase.ColumnType{columnTypeInt})

				go func(i int) {
					defer wg.Done()
					cur := &rc[i]
					var next *RowChannel
					if i+1 != len(rc) {
						next = &rc[i+1]
					}
					for {
						row, meta := cur.Next()
						if row == nil {
							if next != nil {
								next.ProducerDone()
							}
							break
						}
						if next != nil {
							_ = next.Push(row, meta)
						}
					}
				}(i)
			}

			row := sqlbase.EncDatumRow{
				sqlbase.DatumToEncDatum(columnTypeInt, tree.NewDInt(tree.DInt(1))),
			}
			b.SetBytes(int64(unsafe.Sizeof(tree.DInt(1))))
			for i := 0; i < b.N; i++ {
				_ = rc[0].Push(row, ProducerMetadata{})
			}
			rc[0].ProducerDone()
			wg.Wait()
		})

	}
}

type callbackFn func(row sqlbase.EncDatumRow, meta ProducerMetadata) ConsumerStatus

type CallbackReceiver struct {
	fn callbackFn
}

func NewCallbackReceiver(fn callbackFn) *CallbackReceiver {
	return &CallbackReceiver{fn: fn}
}

func (c *CallbackReceiver) Push(row sqlbase.EncDatumRow, meta ProducerMetadata) ConsumerStatus {
	return c.fn(row, meta)
}

func (c *CallbackReceiver) ProducerDone() {}

var _ RowReceiver = &CallbackReceiver{}

func BenchmarkJoinAndCount(b *testing.B) {
	ctx := context.Background()
	evalCtx := tree.MakeTestingEvalContext()
	defer evalCtx.Stop(ctx)
	flowCtx := FlowCtx{
		Settings: cluster.MakeTestingClusterSettings(),
		EvalCtx:  evalCtx,
	}

	spec := HashJoinerSpec{
		LeftEqColumns:  []uint32{0},
		RightEqColumns: []uint32{0},
		Type:           JoinType_INNER,
	}
	//post := PostProcessSpec{Projection: true, OutputColumns: []uint32{0}}
	// TODO(asubiotto): I think this should be right.
	post := PostProcessSpec{}

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}

	// Input will serve as both left and right inputs. Every row will have the
	// same column value, thus producing inputSize^2 rows.
	const inputSize = 1000
	input := make(sqlbase.EncDatumRows, inputSize)
	for i := range input {
		input[i] = sqlbase.EncDatumRow{sqlbase.DatumToEncDatum(columnTypeInt, tree.NewDInt(tree.DInt(1)))}
	}

	types := []sqlbase.ColumnType{columnTypeInt}

	leftInput := NewRepeatableRowSource(types, input)
	rightInput := NewRepeatableRowSource(types, input)

	b.Run("Normal", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			leftInput.Reset()
			rightInput.Reset()
			conn := &RowChannel{}
			conn.Init(types)
			ag, err := newAggregator(
				&flowCtx,
				&AggregatorSpec{
					Aggregations: []AggregatorSpec_Aggregation{{Func: AggregatorSpec_COUNT_ROWS}},
				},
				conn,
				&post,
				&RowDisposer{},
			)
			if err != nil {
				b.Fatal(err)
			}
			var wg sync.WaitGroup
			go func() {
				wg.Add(1)
				ag.Run(ctx, &wg)
			}()
			h, err := newHashJoiner(&flowCtx, &spec, leftInput, rightInput, &post, conn)
			if err != nil {
				b.Fatal(err)
			}
			h.Run(ctx, nil)

			wg.Wait()
		}
	})

	b.Run("ElideRowChan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			leftInput.Reset()
			rightInput.Reset()
			ag, err := newAggregator(
				&flowCtx,
				&AggregatorSpec{
					Aggregations: []AggregatorSpec_Aggregation{{Func: AggregatorSpec_COUNT_ROWS}},
				},
				// This doesn't matter as we're not reading from input.
				&RepeatableRowSource{},
				&post,
				&RowDisposer{},
			)
			if err != nil {
				b.Fatal(err)
			}
			defer ag.Close(ctx)
			var scratch []byte
			out := NewCallbackReceiver(func(row sqlbase.EncDatumRow, meta ProducerMetadata) ConsumerStatus {
				if _, err := ag.NextRow(ctx, scratch, row, meta); err != nil {
					b.Fatal(err)
				}
				return NeedMoreRows
			})
			h, err := newHashJoiner(&flowCtx, &spec, leftInput, rightInput, &post, out)
			if err != nil {
				b.Fatal(err)
			}
			h.Run(ctx, nil)
			ag.RenderResults(ctx)
		}
	})

	b.Run("RowBatching", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			leftInput.Reset()
			rightInput.Reset()
			conn := &RowChannel{}
			conn.Init(types)
			ag, err := newAggregator(
				&flowCtx,
				&AggregatorSpec{
					Aggregations: []AggregatorSpec_Aggregation{{Func: AggregatorSpec_COUNT_ROWS}},
				},
				conn,
				&post,
				&RowDisposer{},
			)
			if err != nil {
				b.Fatal(err)
			}
			ag.Batching = true
			var wg sync.WaitGroup
			go func() {
				wg.Add(1)
				ag.Run(ctx, &wg)
			}()
			batch := make(RowBatch, 0, RowBatchSize)
			h, err := newHashJoiner(
				&flowCtx,
				&spec,
				leftInput,
				rightInput,
				&post,
				NewCallbackReceiver(func(row sqlbase.EncDatumRow, meta ProducerMetadata) ConsumerStatus {
					if row != nil {
						batch = append(batch, row)
						if len(batch) == RowBatchSize {
							status := conn.PushBatch(batch)
							batch = make(RowBatch, 0, RowBatchSize)
							return status
						}
						return NeedMoreRows
					}
					return conn.Push(row, meta)
				}),
			)
			if err != nil {
				b.Fatal(err)
			}
			h.Run(ctx, nil)
			conn.ProducerDone()

			wg.Wait()
		}
	})
}
