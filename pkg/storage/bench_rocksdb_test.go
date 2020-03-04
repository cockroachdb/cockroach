// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func setupMVCCRocksDB(b testing.TB, dir string) Engine {
	cache := NewRocksDBCache(1 << 30 /* 1GB */)
	defer cache.Release()

	rocksdb, err := NewRocksDB(
		RocksDBConfig{
			StorageConfig: base.StorageConfig{
				Settings: cluster.MakeTestingClusterSettings(),
				Dir:      dir,
			},
		},
		cache,
	)
	if err != nil {
		b.Fatalf("could not create new rocksdb db instance at %s: %+v", dir, err)
	}
	return rocksdb
}

func setupMVCCInMemRocksDB(_ testing.TB, loc string) Engine {
	return newRocksDBInMem(roachpb.Attributes{}, testCacheSize)
}

// Read benchmarks. All of them run with on-disk data.

func BenchmarkMVCCScan_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("TODO: fix benchmark")
	}

	ctx := context.Background()
	for _, numRows := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for _, numVersions := range []int{1, 2, 10, 100} {
				b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
					for _, valueSize := range []int{8, 64, 512} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							runMVCCScan(ctx, b, setupMVCCRocksDB, benchScanOptions{
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

	ctx := context.Background()
	for _, numRows := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for _, numVersions := range []int{1, 2, 10, 100} {
				b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
					for _, valueSize := range []int{8, 64, 512} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							runMVCCScan(ctx, b, setupMVCCRocksDB, benchScanOptions{
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

func BenchmarkMVCCScanTransactionalData_RocksDB(b *testing.B) {
	ctx := context.Background()
	runMVCCScan(ctx, b, setupMVCCRocksDB, benchScanOptions{
		numRows: 10000,
		benchDataOptions: benchDataOptions{
			numVersions:   2,
			valueBytes:    8,
			transactional: true,
		},
	})
}

func BenchmarkMVCCGet_RocksDB(b *testing.B) {
	ctx := context.Background()
	for _, numVersions := range []int{1, 10, 100} {
		b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
			for _, valueSize := range []int{8} {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					runMVCCGet(ctx, b, setupMVCCRocksDB, benchDataOptions{
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
	ctx := context.Background()
	for _, valueSize := range []int{8, 32, 256} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCComputeStats(ctx, b, setupMVCCRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCFindSplitKey_RocksDB(b *testing.B) {
	ctx := context.Background()
	for _, valueSize := range []int{32} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCFindSplitKey(ctx, b, setupMVCCRocksDB, valueSize)
		})
	}
}

func BenchmarkIterOnBatch_RocksDB(b *testing.B) {
	ctx := context.Background()
	for _, writes := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("writes=%d", writes), func(b *testing.B) {
			benchmarkIterOnBatch(ctx, b, writes)
		})
	}
}

// BenchmarkIterOnReadOnly_RocksDB is a microbenchmark that measures the
// performance of creating an iterator and seeking to a key if a read-only
// ReadWriter that caches the RocksDB iterator is used
func BenchmarkIterOnReadOnly_RocksDB(b *testing.B) {
	for _, writes := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("writes=%d", writes), func(b *testing.B) {
			benchmarkIterOnReadWriter(b, writes, Engine.NewReadOnly, true)
		})
	}
}

// BenchmarkIterOnEngine_RocksDB is a microbenchmark that measures the
// performance of creating an iterator and seeking to a key without caching is
// used (see BenchmarkIterOnIterCacher_RocksDB).
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
	ctx := context.Background()
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCPut(ctx, b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCBlindPut_RocksDB(b *testing.B) {
	ctx := context.Background()
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindPut(ctx, b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCConditionalPut_RocksDB(b *testing.B) {
	ctx := context.Background()
	for _, createFirst := range []bool{false, true} {
		prefix := "Create"
		if createFirst {
			prefix = "Replace"
		}
		b.Run(prefix, func(b *testing.B) {
			for _, valueSize := range []int{10, 100, 1000, 10000} {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					runMVCCConditionalPut(ctx, b, setupMVCCInMemRocksDB, valueSize, createFirst)
				})
			}
		})
	}
}

func BenchmarkMVCCBlindConditionalPut_RocksDB(b *testing.B) {
	ctx := context.Background()
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindConditionalPut(ctx, b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCInitPut_RocksDB(b *testing.B) {
	ctx := context.Background()
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCInitPut(ctx, b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCBlindInitPut_RocksDB(b *testing.B) {
	ctx := context.Background()
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindInitPut(ctx, b, setupMVCCInMemRocksDB, valueSize)
		})
	}
}

func BenchmarkMVCCPutDelete_RocksDB(b *testing.B) {
	ctx := context.Background()
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

		if err := MVCCPut(ctx, rocksdb, nil, key, hlc.Timestamp{}, value, nil /* txn */); err != nil {
			b.Fatal(err)
		}
		if err := MVCCDelete(ctx, rocksdb, nil, key, hlc.Timestamp{}, nil /* txn */); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMVCCBatchPut_RocksDB(b *testing.B) {
	ctx := context.Background()
	for _, valueSize := range []int{10} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			for _, batchSize := range []int{1, 100, 10000, 100000} {
				b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
					runMVCCBatchPut(ctx, b, setupMVCCInMemRocksDB, valueSize, batchSize)
				})
			}
		})
	}
}

func BenchmarkMVCCBatchTimeSeries_RocksDB(b *testing.B) {
	ctx := context.Background()
	for _, batchSize := range []int{282} {
		b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
			runMVCCBatchTimeSeries(ctx, b, setupMVCCInMemRocksDB, batchSize)
		})
	}
}

// BenchmarkMVCCMergeTimeSeries computes performance of merging time series
// data. Uses an in-memory engine.
func BenchmarkMVCCMergeTimeSeries_RocksDB(b *testing.B) {
	ctx := context.Background()
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
	runMVCCMerge(ctx, b, setupMVCCInMemRocksDB, &value, 1024)
}

// BenchmarkMVCCGetMergedTimeSeries computes performance of reading merged
// time series data using `MVCCGet()`. Uses an in-memory engine.
func BenchmarkMVCCGetMergedTimeSeries_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	ctx := context.Background()
	for _, numKeys := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("numKeys=%d", numKeys), func(b *testing.B) {
			for _, mergesPerKey := range []int{1, 16, 256} {
				b.Run(fmt.Sprintf("mergesPerKey=%d", mergesPerKey), func(b *testing.B) {
					runMVCCGetMergedValue(ctx, b, setupMVCCInMemRocksDB, numKeys, mergesPerKey)
				})
			}
		})
	}
}

// DeleteRange benchmarks below (using on-disk data).

func BenchmarkMVCCDeleteRange_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	ctx := context.Background()
	for _, valueSize := range []int{8, 32, 256} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCDeleteRange(ctx, b, setupMVCCRocksDB, valueSize)
		})
	}
}

func BenchmarkClearRange_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("TODO: fix benchmark")
	}
	ctx := context.Background()
	runClearRange(ctx, b, setupMVCCRocksDB, func(eng Engine, batch Batch, start, end MVCCKey) error {
		return batch.ClearRange(start, end)
	})
}

func BenchmarkClearIterRange_RocksDB(b *testing.B) {
	ctx := context.Background()
	runClearRange(ctx, b, setupMVCCRocksDB, func(eng Engine, batch Batch, start, end MVCCKey) error {
		iter := eng.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
		defer iter.Close()
		return batch.ClearIterRange(iter, start.Key, end.Key)
	})
}

func BenchmarkMVCCGarbageCollect_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}

	// NB: To debug #16068, test only 128-128-15000-6.
	ctx := context.Background()
	for _, keySize := range []int{128} {
		b.Run(fmt.Sprintf("keySize=%d", keySize), func(b *testing.B) {
			for _, valSize := range []int{128} {
				b.Run(fmt.Sprintf("valSize=%d", valSize), func(b *testing.B) {
					for _, numKeys := range []int{1, 1024} {
						b.Run(fmt.Sprintf("numKeys=%d", numKeys), func(b *testing.B) {
							for _, numVersions := range []int{2, 1024} {
								b.Run(fmt.Sprintf("numVersions=%d", numVersions), func(b *testing.B) {
									runMVCCGarbageCollect(ctx, b, setupMVCCInMemRocksDB, benchGarbageCollectOptions{
										benchDataOptions: benchDataOptions{
											numKeys:     numKeys,
											numVersions: numVersions,
											valueBytes:  valSize,
										},
										keyBytes:       keySize,
										deleteVersions: numVersions - 1,
									})
								})
							}
						})
					}
				})
			}
		})
	}
}

func BenchmarkBatchApplyBatchRepr_RocksDB(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	ctx := context.Background()
	for _, indexed := range []bool{false, true} {
		b.Run(fmt.Sprintf("indexed=%t", indexed), func(b *testing.B) {
			for _, sequential := range []bool{false, true} {
				b.Run(fmt.Sprintf("seq=%t", sequential), func(b *testing.B) {
					for _, valueSize := range []int{10} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							for _, batchSize := range []int{10000} {
								b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
									runBatchApplyBatchRepr(ctx, b, setupMVCCInMemRocksDB,
										indexed, sequential, valueSize, batchSize)
								})
							}
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
