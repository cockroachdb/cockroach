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
	"math"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

type blackHoleSink struct {
	// Simulate a real sink.
	ch chan *messageBlock
}

var _ messageSink = &blackHoleSink{}

func newBlackHoleSink(chanSize int) *blackHoleSink {
	return &blackHoleSink{
		ch: make(chan *messageBlock, chanSize),
	}
}

func (b *blackHoleSink) start() {
	go func() {
		for block := range b.ch {
			*block = messageBlock{}
			blockPool.Put(block)
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
	binary.LittleEndian.PutUint64(id[:8], i)
	binary.BigEndian.PutUint64(id[8:], i)
	return id
}

func BenchmarkWriter(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)

	ctx := context.Background()

	run := func(b *testing.B, sink messageSink, numOfConcurrentWriter int) {
		starter := make(chan struct{})

		w := newWriter(sink)

		b.ResetTimer()
		b.SetBytes(messageBlockSize * entrySize)
		var wg sync.WaitGroup
		for writerIdx := 0; writerIdx < numOfConcurrentWriter; writerIdx++ {
			wg.Add(1)

			go func(writerIdx int) {
				defer wg.Done()

				<-starter

				numOfOps := b.N / numOfConcurrentWriter
				randomValueBase := numOfOps * writerIdx
				for i := 0; i < numOfOps; i++ {
					randomValue := randomValueBase + i
					w.Record(ResolvedTxnID{
						TxnID:            generateUUID(uint64(randomValue)),
						TxnFingerprintID: roachpb.TransactionFingerprintID(math.MaxInt - randomValue),
					})
				}
			}(writerIdx)
		}

		close(starter)
		wg.Wait()
	}

	type testSinkType struct {
		name string
		new  func() (_ messageSink, cleanup func())
	}

	sinkTypes := []testSinkType{
		{
			name: "blackHole",
			new: func() (_ messageSink, cleanup func()) {
				blackHole := newBlackHoleSink(channelSize)
				blackHole.start()

				return blackHole, blackHole.stop
			},
		},
		{
			name: "real",
			new: func() (_ messageSink, cleanup func()) {
				st := cluster.MakeTestingClusterSettings()
				metrics := NewMetrics()
				realSink := NewTxnIDCache(st, &metrics)

				stopper := stop.NewStopper()
				realSink.Start(ctx, stopper)

				return realSink, func() {
					stopper.Stop(ctx)
				}
			},
		},
	}

	for _, sinkType := range sinkTypes {
		b.Run(fmt.Sprintf("sinkType=%s", sinkType.name), func(b *testing.B) {
			for _, numOfConcurrentWriter := range []int{1, 24, 48, 64, 92, 128} {
				b.Run(fmt.Sprintf("concurrentWriter=%d", numOfConcurrentWriter), func(b *testing.B) {
					sink, cleanup := sinkType.new()
					defer cleanup()

					run(b, sink, numOfConcurrentWriter)
				})
			}
		})
	}
}
