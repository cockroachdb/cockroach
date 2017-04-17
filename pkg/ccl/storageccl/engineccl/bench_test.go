// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/ccl/LICENSE

package engineccl

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// Output the keys in 100 batches. If we used a single batch to output all of
// the keys rocksdb would create a single sstable. We want multiple sstables in
// order to exercise filtering of which sstables are examined during iterator
// seeking. We fix the number of batches we output so that optimizations which
// change the data size result in the same number of sstables.
const numBatches = 100
const numKeys = 100000
const valueBytes = 512
const batchTimeSpan = 10

// loadTestData writes numKeys keys in numBatches separate batches. Keys are
// written in order. Every key in a given batch has the same MVCC timestamp;
// batch timestamps start at 0 and increase sequentially.
//
// This approach convinces RocksDB to output one SST per batch, where each SST
// contains keys of only one timestamp. We don't randomize key order to prevent
// RocksDB from intermingling MVCC keys with different timestamps in the same
// SST. In the above example, a time-bound scan over the time range [0, 1] can
// eliminate the first SST from consideration entirely.
//
// The creation of the database is time consuming, especially for larger numbers
// of versions. The database is persisted between runs and stored in the current
// directory as "mvcc_data".
func loadTestData() (engine.Engine, error) {
	ctx := context.Background()
	loc := fmt.Sprintf("mvcc_data_%d", numKeys)

	exists := true
	if _, err := os.Stat(loc); os.IsNotExist(err) {
		exists = false
	}

	eng, err := engine.NewRocksDB(
		roachpb.Attributes{},
		loc,
		engine.RocksDBCache{},
		0,
		engine.DefaultMaxOpenFiles,
	)
	if err != nil {
		return nil, err
	}

	if exists {
		testutils.ReadAllFiles(filepath.Join(loc, "*"))
		return eng, nil
	}

	log.Infof(context.Background(), "creating mvcc data: %s", loc)

	// Generate the same data every time.
	rng := rand.New(rand.NewSource(1449168817))

	keys := make([]roachpb.Key, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(i)))
	}

	sstTimestamps := make([]int64, numBatches)
	for i := 0; i < len(sstTimestamps); i++ {
		sstTimestamps[i] = int64(i * batchTimeSpan)
	}

	var batch engine.Batch
	var minWallTime int64
	for i, key := range keys {
		if scaled := len(keys) / numBatches; (i % scaled) == 0 {
			if i > 0 {
				log.Infof(ctx, "committing (%d/~%d)", i/scaled, numBatches)
				if err := batch.Commit(false /* !sync */); err != nil {
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
		timestamp := hlc.Timestamp{WallTime: minWallTime + rand.Int63n(batchTimeSpan)}
		value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueBytes))
		value.InitChecksum(key)
		if err := engine.MVCCPut(ctx, batch, nil, key, timestamp, value, nil); err != nil {
			return nil, err
		}
	}
	if err := batch.Commit(false /* !sync */); err != nil {
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
	makeIterator func(engine.Engine, hlc.Timestamp, hlc.Timestamp) engine.Iterator,
) {
	eng, err := loadTestData()
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	b.SetBytes(int64(numKeys * valueBytes))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		n := 0
		startTime := hlc.MinTimestamp
		endTime := hlc.Timestamp{WallTime: int64(loadFactor * numBatches)}
		it := makeIterator(eng, startTime, endTime)
		defer it.Close()
		for it.Seek(engine.MVCCKey{}); ; it.Next() {
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
		b.Run(fmt.Sprintf("LoadFactor%.2f", loadFactor), func(b *testing.B) {
			b.Run("NormalIterator", func(b *testing.B) {
				runIterate(b, loadFactor, func(e engine.Engine, _, _ hlc.Timestamp) engine.Iterator {
					return e.NewIterator(false)
				})
			})
			b.Run("TimeBoundIterator", func(b *testing.B) {
				runIterate(b, loadFactor, func(e engine.Engine, startTime, endTime hlc.Timestamp) engine.Iterator {
					return e.NewTimeBoundIterator(startTime, endTime)
				})
			})
		})
	}
}
