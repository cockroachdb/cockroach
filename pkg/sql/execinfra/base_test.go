// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// Test the behavior of Run in the presence of errors that switch to the drain
// and forward metadata mode.
func TestRunDrain(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	// A source with no rows and 2 ProducerMetadata messages.
	src := &RowChannel{}
	src.InitWithBufSizeAndNumSenders(nil, 10, 1)
	src.Push(nil /* row */, &execinfrapb.ProducerMetadata{Err: fmt.Errorf("test")})
	src.Push(nil /* row */, nil /* meta */)
	src.Start(ctx)

	// A receiver that is marked as done consuming rows so that Run will
	// immediately move from forwarding rows and metadata to draining metadata.
	buf := &RowChannel{}
	buf.InitWithBufSizeAndNumSenders(nil, 10, 1)
	buf.ConsumerDone()

	Run(ctx, src, buf)

	if src.ConsumerStatus != DrainRequested {
		t.Fatalf("expected DrainRequested, but found %d", src.ConsumerStatus)
	}
}

// Benchmark a pipeline of RowChannels.
func BenchmarkRowChannelPipeline(b *testing.B) {
	for _, length := range []int{1, 2, 3, 4} {
		b.Run(fmt.Sprintf("length=%d", length), func(b *testing.B) {
			rc := make([]RowChannel, length)
			var wg sync.WaitGroup
			wg.Add(len(rc))

			for i := range rc {
				rc[i].InitWithNumSenders(types.OneIntCol, 1)

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

			row := rowenc.EncDatumRow{
				rowenc.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(1))),
			}
			b.SetBytes(int64(8 * 1 * 1))
			for i := 0; i < b.N; i++ {
				_ = rc[0].Push(row, nil /* meta */)
			}
			rc[0].ProducerDone()
			wg.Wait()
		})
	}
}

func BenchmarkMultiplexedRowChannel(b *testing.B) {
	numRows := 1 << 16
	row := rowenc.EncDatumRow{randgen.IntEncDatum(0)}
	for _, senders := range []int{2, 4, 8} {
		b.Run(fmt.Sprintf("senders=%d", senders), func(b *testing.B) {
			b.SetBytes(int64(senders * numRows * 8))
			for i := 0; i < b.N; i++ {
				var wg sync.WaitGroup
				wg.Add(senders + 1)
				mrc := &RowChannel{}
				mrc.InitWithNumSenders(types.OneIntCol, senders)
				go func() {
					for {
						if r, _ := mrc.Next(); r == nil {
							break
						}
					}
					wg.Done()
				}()
				for j := 0; j < senders; j++ {
					go func() {
						for k := 0; k < numRows; k++ {
							mrc.Push(row, nil /* meta */)
						}
						mrc.ProducerDone()
						wg.Done()
					}()
				}
				wg.Wait()
			}
		})
	}
}
