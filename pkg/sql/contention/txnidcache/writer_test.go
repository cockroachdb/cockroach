// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package txnidcache

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type blackHoleSink struct {
	pool *sync.Pool

	// Simulate a real sink
	ch chan *messageBlock
}

var _ messageSink = &blackHoleSink{}

func newBlackHoleSink(pool *sync.Pool, chanSize int) *blackHoleSink {
	return &blackHoleSink{
		pool: pool,
		ch:   make(chan *messageBlock, chanSize),
	}
}

func (b *blackHoleSink) start() {
	go func() {
		for block := range b.ch {
			*block = messageBlock{}
			b.pool.Put(block)
		}
	}()
}

func (b *blackHoleSink) stop() {
	close(b.ch)
}

// push implements messageSink interface.
func (b *blackHoleSink) push(block *messageBlock) {
	b.ch <- block
}

// generateUUID uses a provided integer to populate uuid.UUID. This is
// to avoid UUID generation slowing down the benchmark.
func generateUUID(i uint64) uuid.UUID {
	id := uuid.UUID{}
	binary.LittleEndian.PutUint64(id[:], i)
	return id
}

func BenchmarkWriter(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)

	ctx := context.Background()

	for _, sinkType := range []string{"blackHoleSink", "realSink"} {
		b.Run(fmt.Sprintf("sinkType=%s", sinkType), func(b *testing.B) {
			for _, numOfConcurrentWriter := range []int{1, 24, 48, 64, 92, 128} {
				b.Run(fmt.Sprintf("concurrentWriter=%d", numOfConcurrentWriter), func(b *testing.B) {
					starter := make(chan struct{})

					blockPool := &sync.Pool{
						New: func() interface{} {
							return &messageBlock{}
						},
					}
					var sink messageSink

					switch sinkType {
					case "blackHoleSink":
						blackHole := newBlackHoleSink(blockPool, channelSize)
						blackHole.start()
						defer blackHole.stop()
						sink = blackHole
					case "realSink":
						st := cluster.MakeTestingClusterSettings()
						metrics := NewMetrics()
						realSink := NewTxnIDCache(st, &metrics)
						realSink.Start(ctx, stop.NewStopper())
						sink = realSink
					default:
						panic(fmt.Sprintf("invalid sink type: %s", sinkType))
					}

					w := newWriter(sink, blockPool)

					b.ResetTimer()
					b.SetBytes(messageBlockSize * entrySize)
					var wg sync.WaitGroup
					for writerIdx := 0; writerIdx < numOfConcurrentWriter; writerIdx++ {
						wg.Add(1)

						go func() {
							defer wg.Done()

							<-starter

							for i := 0; i < b.N; i++ {
								w.Record(ResolvedTxnID{
									TxnID: generateUUID(uint64(i)),
								})
							}
						}()
					}

					close(starter)
					wg.Wait()
				})
			}
		})
	}
}
