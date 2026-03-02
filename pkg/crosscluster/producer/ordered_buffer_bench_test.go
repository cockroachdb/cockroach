// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// makeOrderedBufferConfigForBench creates a config with in-memory temp storage
// for benchmarks. It delegates to makeOrderedBufferConfigTB in ordered_buffer_test.go.
func makeOrderedBufferConfigForBench(
	b *testing.B, flushByteSizeThreshold int64,
) (OrderedBufferConfig, func()) {
	return makeOrderedBufferConfigTB(b, flushByteSizeThreshold)
}

// makeBenchKV returns a RangeFeedValue with the given key index and timestamp.
// The value is a small fixed string to keep event size stable.
func makeBenchKV(keyIdx int, ts int64) *kvpb.RangeFeedValue {
	return &kvpb.RangeFeedValue{
		Key: roachpb.Key(fmt.Sprintf("key%08d", keyIdx)),
		Value: roachpb.Value{
			RawBytes:  []byte("v"),
			Timestamp: hlc.Timestamp{WallTime: ts},
		},
	}
}

func BenchmarkOrderedBufferAdd(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	// High threshold so Add does not trigger flush; we measure in-memory add only.
	cfg, cleanup := makeOrderedBufferConfigForBench(b, 1<<20)
	defer cleanup()

	for _, numEvents := range []int{100, 1000, 10000} {
		kvs := make([]*kvpb.RangeFeedValue, numEvents)
		for j := 0; j < numEvents; j++ {
			kvs[j] = makeBenchKV(j, int64(j))
		}
		b.Run(fmt.Sprintf("events=%d", numEvents), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buf := newOrderedBuffer(cfg)
				for j := 0; j < numEvents; j++ {
					if err := buf.Add(ctx, kvs[j]); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func BenchmarkOrderedBufferFlushToDisk(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	cfg, cleanup := makeOrderedBufferConfigForBench(b, 1<<20)
	defer cleanup()

	for _, numEvents := range []int{100, 1000, 10000} {
		kvs := make([]*kvpb.RangeFeedValue, numEvents)
		for j := 0; j < numEvents; j++ {
			kvs[j] = makeBenchKV(j, int64(j))
		}
		b.Run(fmt.Sprintf("events=%d", numEvents), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buf := newOrderedBuffer(cfg)
				for j := 0; j < numEvents; j++ {
					if err := buf.Add(ctx, kvs[j]); err != nil {
						b.Fatal(err)
					}
				}
				if err := buf.FlushToDisk(ctx, hlc.MaxTimestamp); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkOrderedBufferGetEventsFromDisk(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	cfg, cleanup := makeOrderedBufferConfigForBench(b, 1<<20)
	defer cleanup()

	for _, numEvents := range []int{100, 1000, 10000} {
		kvs := make([]*kvpb.RangeFeedValue, numEvents)
		for j := 0; j < numEvents; j++ {
			kvs[j] = makeBenchKV(j, int64(j))
		}
		b.Run(fmt.Sprintf("events=%d", numEvents), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				buf := newOrderedBuffer(cfg)
				for j := 0; j < numEvents; j++ {
					if err := buf.Add(ctx, kvs[j]); err != nil {
						b.Fatal(err)
					}
				}
				if err := buf.FlushToDisk(ctx, hlc.MaxTimestamp); err != nil {
					b.Fatal(err)
				}
				b.StartTimer()
				for {
					events, iterExhausted, err := buf.GetEventsFromDisk(ctx, hlc.MaxTimestamp)
					if err != nil {
						b.Fatal(err)
					}
					_ = events
					if iterExhausted {
						break
					}
				}
			}
		})
	}
}
