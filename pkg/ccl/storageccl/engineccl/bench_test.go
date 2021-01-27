// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package engineccl

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors/oserror"
)

// loadTestData writes numKeys keys in numBatches separate batches. Keys are
// written in order. Every key in a given batch has the same MVCC timestamp;
// batch timestamps start at batchTimeSpan and increase in intervals of
// batchTimeSpan.
//
// Importantly, writing keys in order convinces RocksDB to output one SST per
// batch, where each SST contains keys of only one timestamp. E.g., writing A,B
// at t0 and C at t1 will create two SSTs: one for A,B that only contains keys
// at t0, and one for C that only contains keys at t1. Conversely, writing A, C
// at t0 and B at t1 would create just one SST that contained A,B,C (due to an
// immediate compaction).
//
// The creation of the database is time consuming, so the caller can choose
// whether to use a temporary or permanent location.
func loadTestData(
	dir string, numKeys, numBatches, batchTimeSpan, valueBytes int,
) (storage.Engine, error) {
	ctx := context.Background()

	exists := true
	if _, err := os.Stat(dir); oserror.IsNotExist(err) {
		exists = false
	}

	eng, err := storage.NewPebble(
		ctx,
		storage.PebbleConfig{
			StorageConfig: base.StorageConfig{
				Settings: cluster.MakeTestingClusterSettings(),
				Dir:      dir,
			},
		},
	)
	if err != nil {
		return nil, err
	}

	if exists {
		testutils.ReadAllFiles(filepath.Join(dir, "*"))
		return eng, nil
	}

	log.Infof(context.Background(), "creating test data: %s", dir)

	// Generate the same data every time.
	rng := rand.New(rand.NewSource(1449168817))

	keys := make([]roachpb.Key, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(i)))
	}

	sstTimestamps := make([]int64, numBatches)
	for i := 0; i < len(sstTimestamps); i++ {
		sstTimestamps[i] = int64((i + 1) * batchTimeSpan)
	}

	var batch storage.Batch
	var minWallTime int64
	for i, key := range keys {
		if scaled := len(keys) / numBatches; (i % scaled) == 0 {
			if i > 0 {
				log.Infof(ctx, "committing (%d/~%d)", i/scaled, numBatches)
				if err := batch.Commit(false /* sync */); err != nil {
					return nil, err
				}
				batch.Close()
				if err := eng.Flush(); err != nil {
					return nil, err
				}
			}
			batch = eng.NewBatch()
			minWallTime = sstTimestamps[i/scaled]
		}
		timestamp := hlc.Timestamp{WallTime: minWallTime + rand.Int63n(int64(batchTimeSpan))}
		value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueBytes))
		value.InitChecksum(key)
		if err := storage.MVCCPut(ctx, batch, nil, key, timestamp, value, nil); err != nil {
			return nil, err
		}
	}
	if err := batch.Commit(false /* sync */); err != nil {
		return nil, err
	}
	batch.Close()
	if err := eng.Flush(); err != nil {
		return nil, err
	}

	return eng, nil
}

// runIterate benchmarks iteration over the entire keyspace within time bounds
// derived by the loadFactor. A loadFactor of 0.5 means that approximately 50%
// of the SSTs contain keys in the range [startTime, endTime].
func runIterate(
	b *testing.B,
	loadFactor float32,
	makeIterator func(storage.Engine, hlc.Timestamp, hlc.Timestamp) storage.MVCCIterator,
) {
	const numKeys = 100000
	const numBatches = 100
	const batchTimeSpan = 10
	const valueBytes = 512

	// Store the database in this directory so we don't have to regenerate it on
	// each benchmark run.
	eng, err := loadTestData("mvcc_data", numKeys, numBatches, batchTimeSpan, valueBytes)
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	b.SetBytes(int64(numKeys * valueBytes))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		n := 0
		startTime := hlc.MinTimestamp
		endTime := hlc.Timestamp{WallTime: int64(loadFactor * numBatches * batchTimeSpan)}
		if endTime.IsEmpty() {
			endTime = endTime.Next()
		}
		it := makeIterator(eng, startTime, endTime)
		defer it.Close()
		for it.SeekGE(storage.MVCCKey{Key: keys.LocalMax}); ; it.Next() {
			if ok, err := it.Valid(); !ok {
				if err != nil {
					b.Fatal(err)
				}
				break
			}
			n++
		}
		if e := int(loadFactor * numKeys); n < e {
			b.Fatalf("expected at least %d keys, but got %d\n", e, n)
		}
	}

	b.StopTimer()
}

func BenchmarkTimeBoundIterate(b *testing.B) {
	for _, loadFactor := range []float32{1.0, 0.5, 0.1, 0.05, 0.0} {
		b.Run(fmt.Sprintf("LoadFactor=%.2f", loadFactor), func(b *testing.B) {
			b.Run("NormalIterator", func(b *testing.B) {
				runIterate(b, loadFactor, func(e storage.Engine, _, _ hlc.Timestamp) storage.MVCCIterator {
					return e.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: roachpb.KeyMax})
				})
			})
			b.Run("TimeBoundIterator", func(b *testing.B) {
				runIterate(b, loadFactor, func(e storage.Engine, startTime, endTime hlc.Timestamp) storage.MVCCIterator {
					return e.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{
						MinTimestampHint: startTime,
						MaxTimestampHint: endTime,
						UpperBound:       roachpb.KeyMax,
					})
				})
			})
		})
	}
}
