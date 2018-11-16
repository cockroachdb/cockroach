// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package engine

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func setupMVCCRocksDB(b testing.TB, dir string) Engine {
	cache := NewRocksDBCache(1 << 30 /* 1GB */)
	defer cache.Release()

	rocksdb, err := NewRocksDB(
		RocksDBConfig{
			Settings: cluster.MakeTestingClusterSettings(),
			Dir:      dir,
		},
		cache,
	)
	if err != nil {
		b.Fatalf("could not create new rocksdb db instance at %s: %v", dir, err)
	}
	return rocksdb
}

func setupMVCCInMemRocksDB(_ testing.TB, loc string) Engine {
	return NewInMem(roachpb.Attributes{}, testCacheSize)
}

// Read benchmarks. All of them run with on-disk data.

func BenchmarkMVCCScan_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("TODO: fix benchmark")
	}
	for _, numRows := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for _, numVersions := range []int{1, 2, 10, 100} {
				b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
					for _, valueSize := range []int{8, 64, 512} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							runMVCCScan(b, setupMVCCRocksDB, benchScanOptions{
								benchDataOptions: benchDataOptions{
									numVersions: numVersions,
									valueBytes:  valueSize,
								},
								numRows: numRows,
								reverse: false,
							})
						})
					}
				})
			}
		})
	}
}

func BenchmarkMVCCReverseScan_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("TODO: fix benchmark")
	}
	for _, numRows := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for _, numVersions := range []int{1, 2, 10, 100} {
				b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
					for _, valueSize := range []int{8, 64, 512} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							runMVCCScan(b, setupMVCCRocksDB, benchScanOptions{
								benchDataOptions: benchDataOptions{
									numVersions: numVersions,
									valueBytes:  valueSize,
								},
								numRows: numRows,
								reverse: true,
							})
						})
					}
				})
			}
		})
	}
}

func BenchmarkMVCCGet_RocksDB(b *testing.B) {
	for _, numVersions := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
			for _, valueSize := range []int{8} {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					runMVCCGet(b, setupMVCCRocksDB, benchDataOptions{
						numVersions: numVersions,
						valueBytes:  valueSize,
					})
				})
			}
		})
	}
}

func BenchmarkMVCCComputeStats_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	for _, valueSize := range []int{8, 32, 256} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCComputeStats(b, setupMVCCRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCFindSplitKey_RocksDB(b *testing.B) {
	for _, valueSize := range []int{32} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCFindSplitKey(b, setupMVCCRocksDB, valueSize)
		})
	}
}

func BenchmarkIterOnBatch_RocksDB(b *testing.B) {
	for _, writes := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("writes=%d", writes), func(b *testing.B) {
			benchmarkIterOnBatch(b, writes)
		})
	}
}

// BenchmarkIterOnReadOnly_RocksDB is a microbenchmark that measures the performance of creating an iterator
// and seeking to a key if a read-only ReadWriter that caches the RocksDB iterator is used
func BenchmarkIterOnReadOnly_RocksDB(b *testing.B) {
	for _, writes := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("writes=%d", writes), func(b *testing.B) {
			benchmarkIterOnReadWriter(b, writes, Engine.NewReadOnly, true)
		})
	}
}

// BenchmarkIterOnEngine_RocksDB is a microbenchmark that measures the performance of creating an iterator
// and seeking to a key without caching is used (see BenchmarkIterOnReadOnly_RocksDB)
func BenchmarkIterOnEngine_RocksDB(b *testing.B) {
	for _, writes := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("writes=%d", writes), func(b *testing.B) {
			benchmarkIterOnReadWriter(b, writes, func(e Engine) ReadWriter { return e }, false)
		})
	}
}

// Write benchmarks. Most of them run in-memory except for DeleteRange benchs,
// which make more sense when data is present.

func BenchmarkMVCCPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCPut(b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCBlindPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindPut(b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCConditionalPut_RocksDB(b *testing.B) {
	for _, createFirst := range []bool{false, true} {
		prefix := "Create"
		if createFirst {
			prefix = "Replace"
		}
		b.Run(prefix, func(b *testing.B) {
			for _, valueSize := range []int{10, 100, 1000, 10000} {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					runMVCCConditionalPut(b, setupMVCCInMemRocksDB, valueSize, createFirst)
				})
			}
		})
	}
}

func BenchmarkMVCCBlindConditionalPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindConditionalPut(b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCInitPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCInitPut(b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCBlindInitPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindInitPut(b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCPutDelete_RocksDB(b *testing.B) {
	rocksdb := setupMVCCInMemRocksDB(b, "put_delete")
	defer rocksdb.Close()

	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(r, 10))
	var blockNum int64

	for i := 0; i < b.N; i++ {
		blockID := r.Int63()
		blockNum++
		key := encoding.EncodeVarintAscending(nil, blockID)
		key = encoding.EncodeVarintAscending(key, blockNum)

		if err := MVCCPut(context.Background(), rocksdb, nil, key, hlc.Timestamp{}, value, nil /* txn */); err != nil {
			b.Fatal(err)
		}
		if err := MVCCDelete(context.Background(), rocksdb, nil, key, hlc.Timestamp{}, nil /* txn */); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMVCCBatchPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			for _, batchSize := range []int{1, 100, 10000, 100000} {
				b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
					runMVCCBatchPut(b, setupMVCCInMemRocksDB, valueSize, batchSize)
				})
			}
		})
	}
}

func BenchmarkMVCCBatchTimeSeries_RocksDB(b *testing.B) {
	for _, batchSize := range []int{282} {
		b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
			runMVCCBatchTimeSeries(b, setupMVCCInMemRocksDB, batchSize)
		})
	}
}

// BenchmarkMVCCMergeTimeSeries computes performance of merging time series
// data. Uses an in-memory engine.
func BenchmarkMVCCMergeTimeSeries_RocksDB(b *testing.B) {
	ts := &roachpb.InternalTimeSeriesData{
		StartTimestampNanos: 0,
		SampleDurationNanos: 1000,
		Samples: []roachpb.InternalTimeSeriesSample{
			{Offset: 0, Count: 1, Sum: 5.0},
		},
	}
	var value roachpb.Value
	if err := value.SetProto(ts); err != nil {
		b.Fatal(err)
	}
	runMVCCMerge(b, setupMVCCInMemRocksDB, &value, 1024)
}

// DeleteRange benchmarks below (using on-disk data).

func BenchmarkMVCCDeleteRange_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	for _, valueSize := range []int{8, 32, 256} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCDeleteRange(b, setupMVCCRocksDB, valueSize)
		})
	}
}

func BenchmarkClearRange_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("TODO: fix benchmark")
	}
	runClearRange(b, func(eng Engine, batch Batch, start, end MVCCKey) error {
		return batch.ClearRange(start, end)
	})
}

func BenchmarkClearIterRange_RocksDB(b *testing.B) {
	runClearRange(b, func(eng Engine, batch Batch, start, end MVCCKey) error {
		iter := eng.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
		defer iter.Close()
		return batch.ClearIterRange(iter, start, end)
	})
}

func BenchmarkMVCCGarbageCollect(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	defer leaktest.AfterTest(b)()
	ctx := context.Background()
	ts := hlc.Timestamp{}.Add(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(), 0)

	// Write 'numKeys' of the given 'keySize' and 'valSize' to the given engine.
	// For each key, write 'numVersions' versions, and add a GCRequest_GCKey to
	// the returned slice that affects the oldest 'deleteVersions' versions. The
	// first write for each key will be at `ts`, the second one at `ts+(0,1)`,
	// etc.
	//
	// NB: a real invocation of MVCCGarbageCollect typically has most of the keys
	// in sorted order. Here they will be ordered randomly.
	setup := func(
		engine Engine, keySize, valSize, numKeys, numVersions, deleteVersions int,
	) []roachpb.GCRequest_GCKey {
		var ms enginepb.MVCCStats
		var gcKeys []roachpb.GCRequest_GCKey

		mkKey := func() []byte {
			k := make([]byte, keySize)
			if n, err := rand.Read(k); err != nil || n != keySize {
				b.Fatalf("error or read too little: n=%d err=%v", n, err)
			}
			return k
		}
		valBytes := make([]byte, valSize)
		if n, err := rand.Read(valBytes); err != nil || n != valSize {
			b.Fatalf("error or read too little: n=%d err=%v", n, err)
		}
		v := roachpb.MakeValueFromBytesAndTimestamp(valBytes, hlc.Timestamp{})

		for j := 0; j < numKeys; j++ {
			curKey := mkKey()
			if deleteVersions > 0 {
				gcKeys = append(gcKeys, roachpb.GCRequest_GCKey{
					Timestamp: ts.Add(0, int32(deleteVersions-1)),
					Key:       curKey,
				})
			}

			for i := 0; i < numVersions; i++ {
				if err := MVCCPut(
					ctx, engine, &ms, curKey, ts.Add(0, int32(i)), v, nil); err != nil {
					b.Fatal(err)
				}
			}
		}
		return gcKeys
	}

	// We write values at ts+(0,i), set now=ts+(1,0) so that we're ahead of all
	// the writes. This value doesn't matter in practice, it's used only for
	// stats updates.
	now := ts.Add(1, 0)

	run := func(
		b *testing.B, keySize, valSize, numKeys, numVersions, deleteVersions int,
	) {
		engine := createTestEngine()
		defer engine.Close()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			gcKeys := setup(engine, keySize, valSize, numKeys, numVersions, deleteVersions)

			b.StartTimer()
			if err := MVCCGarbageCollect(
				ctx, engine, &enginepb.MVCCStats{}, gcKeys, now,
			); err != nil {
				b.Fatal(err)
			}
		}
	}

	// NB: To debug #16068, test only 128-128-15000-6.
	for _, keySize := range []int{128} {
		b.Run(fmt.Sprintf("keySize=%d", keySize), func(b *testing.B) {
			for _, valSize := range []int{128} {
				b.Run(fmt.Sprintf("valSize=%d", valSize), func(b *testing.B) {
					for _, numKeys := range []int{1, 1024} {
						b.Run(fmt.Sprintf("numKeys=%d", numKeys), func(b *testing.B) {
							for _, numVersions := range []int{2, 1024} {
								b.Run(fmt.Sprintf("numVersions=%d", numVersions), func(b *testing.B) {
									deleteVersions := numVersions - 1
									run(b, keySize, valSize, numKeys, numVersions, deleteVersions)
								})
							}
						})
					}
				})
			}
		})
	}
}

func BenchmarkBatchApplyBatchRepr(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	for _, writeOnly := range []bool{false, true} {
		b.Run(fmt.Sprintf("writeOnly=%t ", writeOnly), func(b *testing.B) {
			for _, valueSize := range []int{10} {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					for _, batchSize := range []int{1000000} {
						b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
							runBatchApplyBatchRepr(b, setupMVCCInMemRocksDB, writeOnly, valueSize, batchSize)
						})
					}
				})
			}
		})
	}
}

func BenchmarkBatchBuilderPut(b *testing.B) {
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	b.ResetTimer()

	const batchSize = 1000
	batch := &RocksDBBatchBuilder{}
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(j)))
			ts := hlc.Timestamp{WallTime: int64(j)}
			batch.Put(MVCCKey{key, ts}, value)
		}
		batch.Finish()
	}

	b.StopTimer()
}
