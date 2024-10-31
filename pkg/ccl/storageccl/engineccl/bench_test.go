// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package engineccl

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testfixtures"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

// loadTestData writes numKeys keys in numBatches separate batches. Each key is
// in the form 'key-XXXXX' where XXXXXX is an ascending-encoded uvarint of the
// key index. Keys are written in order. Every batch is assigned a minimum MVCC
// timestamp T_min. All keys within the batch have timestamps in the interval
// [T_min,T_min+batchTimeSpan). Each successive batch has a T_min that's
// batchTimeSpan higher than the previous. Thus batches are disjoint in both
// user key space and MVCC timestamps.
//
// After each batch is committed, a memtable flush is forced to compel Pebble
// into separating batches into separate sstables where each sstable contains
// keys only for a single batch (and a single `batchTimeSpan` time window).
//
// The creation of the database is mildly time consuming, so it's cached as a
// test fixture (see testfixtures.ReuseOrGenerate).
//
// TODO(jackson): This test was initially written when time-bound iteration
// could only optimize iteration by skipping entire sstables. With the
// introduction of block-property filters in 22.1, this is no longer the case.
// The test could be adapted to test more granular iteration, if useful.
func loadTestData(
	tb testing.TB, dirPrefix string, numKeys, numBatches, batchTimeSpan, valueBytes int,
) storage.Engine {
	ctx := context.Background()

	verStr := fmt.Sprintf("v%s", clusterversion.Latest.String())
	name := fmt.Sprintf("%s_v%s_%d_%d_%d_%d", dirPrefix, verStr, numKeys, numBatches, batchTimeSpan, valueBytes)
	dir := testfixtures.ReuseOrGenerate(tb, name, func(dir string) {
		eng, err := storage.Open(
			ctx,
			fs.MustInitPhysicalTestingEnv(dir),
			cluster.MakeTestingClusterSettings())
		if err != nil {
			tb.Fatal(err)
		}

		log.Infof(context.Background(), "creating test data: %s", dir)

		// Generate the same data every time.
		rng := rand.New(rand.NewSource(1449168817))

		keys := make([]roachpb.Key, numKeys)
		for i := 0; i < numKeys; i++ {
			keys[i] = roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(i)))
		}

		minSStableTimestamps := make([]int64, numBatches)
		for i := 0; i < len(minSStableTimestamps); i++ {
			minSStableTimestamps[i] = int64(i * batchTimeSpan)
		}

		var batch storage.Batch
		var minWallTime int64
		for i, key := range keys {
			if scaled := len(keys) / numBatches; (i % scaled) == 0 {
				if i > 0 {
					log.Infof(ctx, "committing (%d/~%d)", i/scaled, numBatches)
					if err := batch.Commit(false /* sync */); err != nil {
						tb.Fatal(err)
					}
					batch.Close()
					if err := eng.Flush(); err != nil {
						tb.Fatal(err)
					}
				}
				batch = eng.NewBatch()
				minWallTime = minSStableTimestamps[i/scaled]
			}
			timestamp := hlc.Timestamp{WallTime: minWallTime + rand.Int63n(int64(batchTimeSpan))}
			if timestamp.Less(hlc.MinTimestamp) {
				timestamp = hlc.MinTimestamp
			}
			value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueBytes))
			value.InitChecksum(key)
			if _, err := storage.MVCCPut(ctx, batch, key, timestamp, value, storage.MVCCWriteOptions{}); err != nil {
				tb.Fatal(err)
			}
		}
		if err := batch.Commit(false /* sync */); err != nil {
			tb.Fatal(err)
		}
		batch.Close()
		if err := eng.Flush(); err != nil {
			tb.Fatal(err)
		}
		eng.Close()
	})

	log.Infof(context.Background(), "using test data: %s", dir)
	eng, err := storage.Open(
		ctx,
		fs.MustInitPhysicalTestingEnv(dir),
		cluster.MakeTestingClusterSettings(),
		storage.MustExist,
	)
	if err != nil {
		tb.Fatal(err)
	}
	testutils.ReadAllFiles(filepath.Join(dir, "*"))
	return eng
}

// runIterate benchmarks iteration over the entire keyspace within time bounds
// derived by the loadFactor. A loadFactor of 0.5 means that approximately 50%
// of the SSTs contain keys in the range [startTime, endTime].
func runIterate(
	b *testing.B,
	loadFactor float32,
	makeIterator func(storage.Engine, hlc.Timestamp, hlc.Timestamp) (storage.MVCCIterator, error),
) {
	const numKeys = 100000
	const numBatches = 100
	const batchTimeSpan = 10
	const valueBytes = 512

	// Store the database in this directory so we don't have to regenerate it on
	// each benchmark run.
	eng := loadTestData(b, "mvcc_data_v3", numKeys, numBatches, batchTimeSpan, valueBytes)
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
		it, err := makeIterator(eng, startTime, endTime)
		if err != nil {
			b.Fatal(err)
		}
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
				runIterate(b, loadFactor, func(e storage.Engine, _, _ hlc.Timestamp) (storage.MVCCIterator, error) {
					return e.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: roachpb.KeyMax})
				})
			})
			b.Run("TimeBoundIterator", func(b *testing.B) {
				runIterate(b, loadFactor, func(e storage.Engine, startTime, endTime hlc.Timestamp) (storage.MVCCIterator, error) {
					return e.NewMVCCIterator(context.Background(), storage.MVCCKeyIterKind, storage.IterOptions{
						MinTimestamp: startTime,
						MaxTimestamp: endTime,
						UpperBound:   roachpb.KeyMax,
					})
				})
			})
		})
	}
}
