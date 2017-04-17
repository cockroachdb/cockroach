// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/ccl/LICENSE

package engineccl

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

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

// loadTestData writes numKeys keys in numBatches separate batches. Keys are
// written in order. Every key in a given batch has the same MVCC timestamp.
// Every timestamp in the range [1, numBatches] is assigned to exactly one
// batch, but the order in which these timestamps is assigned is randomized.
// For example, given numBatches=2 and numKeys=6, the following batches might
// be created:
//
// key-1#2  ┐
// key-2#2  ├ Batch 1
// key-3#2  ┘
// key-4#0  ┐
// key-5#0  ├ Batch 2
// key-6#0  ┘
// key-7#1  ┐
// key-8#1  ├ Batch 3
// key-9#1  ┘
//
// This will convince RocksDB to output one SST per batch, where each SST
// contains keys of only one timestamp. We don't randomize key order to prevent
// RocksDB from intermingling MVCC keys with different timestamps in the same
// SST. In the above example, a time-bound scan over the time range [0, 1] can
// eliminate the first SST from consideration entirely.
//
// The creation of the database is time consuming, especially for larger
// numbers of versions. The database is persisted between runs and stored in
// the current directory as "mvcc_data".
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
		sstTimestamps[i] = int64(i) + 1
	}

	// Randomize the progression of SST timestamps.
	for i, n := 0, len(sstTimestamps); i < n; i++ {
		j := i + rng.Intn(n-i)
		sstTimestamps[i], sstTimestamps[j] = sstTimestamps[j], sstTimestamps[i]
	}

	var batch engine.Batch
	var timestamp hlc.Timestamp
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
			timestamp = hlc.Timestamp{WallTime: sstTimestamps[i/scaled]}
		}
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
		if e, a := int(loadFactor*numKeys), n; a < e {
			b.Fatalf("expected at least %d keys, but got %d\n", e, a)
		}
	}

	b.StopTimer()
}

func BenchmarkTimeBoundIterate(b *testing.B) {
	for _, loadFactor := range []float32{1.0, 0.5, 0.1, 0.05, 0.0} {
		b.Run(fmt.Sprintf("LoadFactor%.2fNormalIterator", loadFactor), func(b *testing.B) {
			runIterate(b, loadFactor, func(e engine.Engine, _, _ hlc.Timestamp) engine.Iterator {
				return e.NewIterator(false)
			})
		})
		b.Run(fmt.Sprintf("LoadFactor%.2fTimeBoundIterator", loadFactor), func(b *testing.B) {
			runIterate(b, loadFactor, func(e engine.Engine, startTime, endTime hlc.Timestamp) engine.Iterator {
				return e.NewTimeBoundIterator(startTime, endTime)
			})
		})
	}
}
