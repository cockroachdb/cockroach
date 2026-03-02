// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package producer

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// makeDiskBackedConfig creates a config with disk-backed temp storage for benchmarks.
func makeDiskBackedConfig(b *testing.B, flushByteSizeThreshold int64) (OrderedBufferConfig, func()) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	tempDir := b.TempDir()

	tempStorageCfg := base.NewTempStorageConfig(
		ctx, st,
		false, /* InMemory */
		tempDir,
		nil, /* encryption */
		100*1024*1024, /* 100 MiB max */
		"", /* tempDirsRecordPath */
	)

	tempEngine, _, err := storage.NewTempEngine(ctx, tempStorageCfg, nil)
	if err != nil {
		b.Fatal(err)
	}

	cfg := OrderedBufferConfig{
		settings:               st,
		streamID:               streampb.StreamID(1),
		tempStorage:            tempEngine,
		flushByteSizeThreshold: flushByteSizeThreshold,
	}
	return cfg, func() { tempEngine.Close() }
}

// makeBenchKV returns a RangeFeedValue with the given key index and timestamp.
func makeBenchKV(keyIdx int, ts int64) *kvpb.RangeFeedValue {
	value := make([]byte, 1024)
	for i := range value {
		value[i] = byte(keyIdx % 256)
	}
	return &kvpb.RangeFeedValue{
		Key: roachpb.Key(fmt.Sprintf("key%08d", keyIdx)),
		Value: roachpb.Value{
			RawBytes:  value,
			Timestamp: hlc.Timestamp{WallTime: ts},
		},
	}
}

func BenchmarkOrderedBufferFlushToDisk(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	cfg, cleanup := makeDiskBackedConfig(b, defaultBatchSize)
	defer cleanup()

	for _, numEvents := range []int{100, 1000, 10000} {
		kvs := make([]*kvpb.RangeFeedValue, numEvents)
		for j := 0; j < numEvents; j++ {
			kvs[j] = makeBenchKV(j, int64(j))
		}
		b.Run(fmt.Sprintf("events=%d", numEvents), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var totalFlushes int64
			for i := 0; i < b.N; i++ {
				buf := newOrderedBuffer(cfg)
				for j := 0; j < numEvents; j++ {
					if err := buf.Add(ctx, kvs[j]); err != nil {
						b.Fatal(err)
					}
				}
				if buf.Len() > 0 {
					if err := buf.FlushToDisk(ctx, hlc.MaxTimestamp); err != nil {
						b.Fatal(err)
					}
					totalFlushes += buf.FlushCount()
				}
			}
			b.ReportMetric(float64(totalFlushes)/float64(b.N), "flushes/op")
		})
	}
}

func BenchmarkOrderedBufferGetEventsFromDisk(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	cfg, cleanup := makeDiskBackedConfig(b, defaultBatchSize)
	defer cleanup()

	for _, numEvents := range []int{100, 1000, 10000} {
		kvs := make([]*kvpb.RangeFeedValue, numEvents)
		for j := 0; j < numEvents; j++ {
			kvs[j] = makeBenchKV(j, int64(j))
		}
		b.Run(fmt.Sprintf("events=%d", numEvents), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			var totalFlushes int64
			for i := 0; i < b.N; i++ {
				buf := newOrderedBuffer(cfg)
				for j := 0; j < numEvents; j++ {
					if err := buf.Add(ctx, kvs[j]); err != nil {
						b.Fatal(err)
					}
				}
				totalFlushes += buf.FlushCount()
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
			b.ReportMetric(float64(totalFlushes)/float64(b.N), "flushes/op")
		})
	}
}
