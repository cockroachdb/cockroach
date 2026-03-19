// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// BenchmarkMVCCScan_BlockOnlyMaxTimestamp measures the effect of using
// BlockOnlyMaxTimestamp to skip SST data blocks during MVCCScan. This models
// the TTL scan optimization where blocks containing only recently-written rows
// (timestamps above a threshold) are skipped entirely.
//
// The benchmark writes keys across multiple SST blocks with controlled
// timestamps: a fraction of keys are "old" (below the threshold) and the rest
// are "new" (above). The block property filter should allow Pebble to skip
// blocks that contain only "new" keys.
//
// Run with:
//
//	./dev test pkg/storage -f BenchmarkMVCCScan_BlockOnlyMaxTimestamp -v --bench-only
func BenchmarkMVCCScan_BlockOnlyMaxTimestamp(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	const (
		valueSize = 128
		// Total number of keys. Distributed across batches to create multiple
		// SST blocks.
		numKeys = 50000
		// Number of batches. Each batch is flushed to create separate SST data
		// blocks, ensuring the block property filter has block boundaries to
		// work with.
		numBatches   = 50
		keysPerBatch = numKeys / numBatches
		// Timestamp layout: "old" keys are written at wall times [1..1000],
		// "new" keys at wall times [2001..3000]. The threshold is set at 1500,
		// so blocks with only new keys (wall time > 1500) should be skipped.
		oldTSBase = int64(1)
		newTSBase = int64(2001)
		threshold = int64(1500)
	)

	// pctNew controls what fraction of batches contain only "new" (skippable)
	// keys. Higher values mean more data can be skipped.
	for _, pctNew := range []int{0, 50, 90, 99} {
		b.Run(fmt.Sprintf("pctNew=%d", pctNew), func(b *testing.B) {
			eng, err := Open(ctx, InMemory(), cluster.MakeClusterSettings(), CacheSize(64<<20))
			if err != nil {
				b.Fatal(err)
			}
			defer eng.Close()

			numNewBatches := numBatches * pctNew / 100
			numOldBatches := numBatches - numNewBatches

			// Write data in batches, flushing between each to create SST blocks.
			// Old batches first, then new batches. This layout means the new
			// batches form contiguous SST blocks that can be skipped.
			keyBuf := make([]byte, 0, 64)
			for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
				batch := eng.NewBatch()
				isNew := batchIdx >= numOldBatches

				for k := 0; k < keysPerBatch; k++ {
					keyIdx := batchIdx*keysPerBatch + k
					key := roachpb.Key(encoding.EncodeUvarintAscending(
						append(keyBuf[:0], "key-"...), uint64(keyIdx)))

					var wallTime int64
					if isNew {
						wallTime = newTSBase + int64(k)
					} else {
						wallTime = oldTSBase + int64(k)
					}
					ts := hlc.Timestamp{WallTime: wallTime}

					val := roachpb.MakeValueFromBytes(make([]byte, valueSize))
					val.InitChecksum(key)
					if _, err := MVCCPut(ctx, batch, key, ts, val, MVCCWriteOptions{}); err != nil {
						b.Fatal(err)
					}
				}
				if err := batch.Commit(false /* sync */); err != nil {
					b.Fatal(err)
				}
				batch.Close()
				if err := eng.Flush(); err != nil {
					b.Fatal(err)
				}
			}

			// Compact to move all data to L6, ensuring block property filters
			// are active on all data.
			if err := eng.Compact(ctx); err != nil {
				b.Fatal(err)
			}

			startKey := roachpb.Key(encoding.EncodeUvarintAscending(
				[]byte("key-"), 0))
			endKey := roachpb.Key(encoding.EncodeUvarintAscending(
				[]byte("key-"), uint64(numKeys))).Next()
			scanTS := hlc.Timestamp{WallTime: newTSBase + int64(keysPerBatch)}

			// Sub-benchmark: scan without block skipping (baseline).
			b.Run("without_hint", func(b *testing.B) {
				b.SetBytes(int64(numKeys * valueSize))
				for b.Loop() {
					res, err := MVCCScan(ctx, eng, startKey, endKey, scanTS, MVCCScanOptions{
						MaxKeys:      numKeys,
						ReadCategory: fs.BatchEvalReadCategory,
					})
					if err != nil {
						b.Fatal(err)
					}
					if len(res.KVs) != numKeys {
						b.Fatalf("expected %d keys, got %d", numKeys, len(res.KVs))
					}
				}
			})

			// Sub-benchmark: scan with BlockOnlyMaxTimestamp hint.
			b.Run("with_hint", func(b *testing.B) {
				b.SetBytes(int64(numKeys * valueSize))
				for b.Loop() {
					res, err := MVCCScan(ctx, eng, startKey, endKey, scanTS, MVCCScanOptions{
						MaxKeys:               numKeys,
						ReadCategory:          fs.BatchEvalReadCategory,
						BlockOnlyMaxTimestamp: hlc.Timestamp{WallTime: threshold},
					})
					if err != nil {
						b.Fatal(err)
					}
					// With the hint, blocks containing only "new" keys are
					// skipped. Only keys from "old" batches should be returned.
					expectedKeys := numOldBatches * keysPerBatch
					if len(res.KVs) != expectedKeys {
						b.Fatalf("expected %d keys, got %d", expectedKeys, len(res.KVs))
					}
				}
			})
		})
	}
}
