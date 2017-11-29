// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package engineccl

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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
	tb testing.TB, dir string, numKeys, numBatches, batchTimeSpan, revisions int, valueBytes int,
) (engine.Engine, error) {
	ctx := context.Background()

	exists := true
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		exists = false
	}

	eng, err := engine.NewRocksDB(
		engine.RocksDBConfig{
			Settings: cluster.MakeTestingClusterSettings(),
			Dir:      dir,
		},
		engine.RocksDBCache{},
	)
	if err != nil {
		return nil, err
	}

	if exists {
		tb.Logf("using existing data in %q", dir)
		testutils.ReadAllFiles(filepath.Join(dir, "*"))
		return eng, nil
	}

	tb.Logf("creating test data: %s", dir)

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

	var batch engine.Batch
	var minWallTime int64
	for rev := 1; rev <= revisions; rev++ {
		for i, key := range keys {
			if scaled := len(keys) / numBatches; (i % scaled) == 0 {
				if i > 0 {
					tb.Logf("committing (%d/~%d)", i/scaled, numBatches)
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
			timestamp := hlc.Timestamp{WallTime: minWallTime*int64(rev) + rand.Int63n(int64(batchTimeSpan))}
			value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueBytes))
			value.InitChecksum(key)
			if err := engine.MVCCPut(ctx, batch, nil, key, timestamp, value, nil); err != nil {
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
	}

	return eng, nil
}

func BenchmarkExport(b *testing.B) {
	const numBatches = 10
	const batchTimeSpan = 100
	const valueBytes = 64
	const revisions = 5

	// we need lots of rows to work with for our various batches and offsts and do
	// not want small initial b.N values to cause problems.
	rows := b.N * 100
	dir := filepath.Join("testdata", fmt.Sprintf("export-%d-%d", rows, revisions))
	eng, err := loadTestData(
		b, dir, rows, numBatches, batchTimeSpan, revisions, valueBytes,
	)
	if err != nil {
		b.Fatal(err)
	}
	defer eng.Close()

	// this is a rough (i.e. doesn't include timestamp) average size.
	keyLen := len(encoding.EncodeUvarintAscending([]byte("key-"), uint64(rows/2)))
	b.SetBytes(int64(rows * (valueBytes + keyLen)))

	// swap via var rather than separate subbenchmarks to allow benchstat compare.
	var f = MVCCIncIterExport

	if envutil.EnvOrDefaultBool("COCKROACH_BENCH_CPP_SST", false) {
		f = ExportSST
	}

	// Aim to get roughly 1/2 of the key range at neither the start or end, and
	// pick start and end times to ensure both new and old ignored revisions.
	startKey := encoding.EncodeUvarintAscending([]byte("key-"), uint64(rows/4))
	endKey := encoding.EncodeUvarintAscending([]byte("key-"), uint64((rows/4)*3))

	// batchTimeSpan * numBatches should be after loadTestData's first revisions.
	startTime := hlc.Timestamp{WallTime: int64(batchTimeSpan * numBatches)}
	endTime := hlc.Timestamp{WallTime: int64(batchTimeSpan * numBatches * (revisions - 2))}

	b.ResetTimer()

	if data, _, err := f(
		context.TODO(),
		eng,
		startKey, endKey,
		startTime, endTime,
		false,
	); err != nil {
		b.Fatal(err)
	} else if len(data) < (rows/2)*64 {
		// it's easy to mess up the setup and end up not actually exporting anything
		b.Fatalf("expected at least 64b/key in %d row export, got %d", rows, len(data))
	}
}

// runIterate benchmarks iteration over the entire keyspace within time bounds
// derived by the loadFactor. A loadFactor of 0.5 means that approximately 50%
// of the SSTs contain keys in the range [startTime, endTime].
func runIterate(
	b *testing.B,
	loadFactor float32,
	makeIterator func(engine.Engine, hlc.Timestamp, hlc.Timestamp) engine.Iterator,
) {
	const numKeys = 100000
	const numBatches = 100
	const batchTimeSpan = 10
	const valueBytes = 512
	const revisions = 1

	// Store the database in this directory so we don't have to regenerate it on
	// each benchmark run.
	eng, err := loadTestData(b, "mvcc_data", numKeys, numBatches, batchTimeSpan, revisions, valueBytes)
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
		b.Run(fmt.Sprintf("LoadFactor=%.2f", loadFactor), func(b *testing.B) {
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
