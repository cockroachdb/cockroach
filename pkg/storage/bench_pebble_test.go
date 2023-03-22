// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

const testCacheSize = 1 << 30 // 1 GB

func setupMVCCPebble(b testing.TB, dir string) Engine {
	peb, err := Open(
		context.Background(),
		Filesystem(dir),
		cluster.MakeTestingClusterSettings(),
		CacheSize(testCacheSize))
	if err != nil {
		b.Fatalf("could not create new pebble instance at %s: %+v", dir, err)
	}
	return peb
}

func setupMVCCInMemPebble(b testing.TB, loc string) Engine {
	return setupMVCCInMemPebbleWithSeparatedIntents(b)
}

func setupMVCCInMemPebbleWithSeparatedIntents(b testing.TB) Engine {
	peb, err := Open(
		context.Background(),
		InMemory(),
		cluster.MakeClusterSettings(),
		CacheSize(testCacheSize))
	if err != nil {
		b.Fatalf("could not create new in-mem pebble instance: %+v", err)
	}
	return peb
}

func setupPebbleInMemPebbleForLatestRelease(b testing.TB, _ string) Engine {
	ctx := context.Background()
	s := cluster.MakeClusterSettings()
	if err := clusterversion.Initialize(ctx, clusterversion.TestingBinaryVersion,
		&s.SV); err != nil {
		b.Fatalf("failed to set current cluster version: %+v", err)
	}

	peb, err := Open(ctx, InMemory(), s, CacheSize(testCacheSize))
	if err != nil {
		b.Fatalf("could not create new in-mem pebble instance: %+v", err)
	}
	return peb
}

func BenchmarkMVCCScan_Pebble(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, numRows := range []int{1, 10, 100, 1000, 10000, 50000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for _, numVersions := range []int{1, 2, 10, 100, 1000} {
				b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
					for _, valueSize := range []int{8, 64, 512} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							for _, numRangeKeys := range []int{0, 1, 100} {
								b.Run(fmt.Sprintf("numRangeKeys=%d", numRangeKeys), func(b *testing.B) {
									runMVCCScan(ctx, b, benchScanOptions{
										mvccBenchData: mvccBenchData{
											numVersions:  numVersions,
											valueBytes:   valueSize,
											numRangeKeys: numRangeKeys,
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
		})
	}
}

func BenchmarkMVCCScanGarbage_Pebble(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, numRows := range []int{1, 10, 100, 1000, 10000, 50000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for _, numVersions := range []int{1, 2, 10, 100, 1000} {
				b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
					for _, numRangeKeys := range []int{0, 1, 100} {
						b.Run(fmt.Sprintf("numRangeKeys=%d", numRangeKeys), func(b *testing.B) {
							for _, tombstones := range []bool{false, true} {
								b.Run(fmt.Sprintf("tombstones=%t", tombstones), func(b *testing.B) {
									runMVCCScan(ctx, b, benchScanOptions{
										mvccBenchData: mvccBenchData{
											numVersions:  numVersions,
											numRangeKeys: numRangeKeys,
											garbage:      true,
										},
										numRows:    numRows,
										tombstones: tombstones,
										reverse:    false,
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

func BenchmarkMVCCScanSQLRows_Pebble(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, numRows := range []int{1, 10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for _, numColumnFamilies := range []int{1, 3, 10} {
				b.Run(fmt.Sprintf("columnFamilies=%d", numColumnFamilies), func(b *testing.B) {
					for _, numVersions := range []int{1} {
						b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
							for _, valueSize := range []int{8, 64, 512} {
								b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
									for _, wholeRows := range []bool{false, true} {
										b.Run(fmt.Sprintf("wholeRows=%t", wholeRows), func(b *testing.B) {
											runMVCCScan(ctx, b, benchScanOptions{
												mvccBenchData: mvccBenchData{
													numColumnFamilies: numColumnFamilies,
													numVersions:       numVersions,
													valueBytes:        valueSize,
												},
												numRows:   numRows,
												reverse:   false,
												wholeRows: wholeRows,
											})
										})
									}
								})
							}
						})
					}
				})
			}
		})
	}
}

func BenchmarkMVCCReverseScan_Pebble(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, numRows := range []int{1, 10, 100, 1000, 10000, 50000} {
		b.Run(fmt.Sprintf("rows=%d", numRows), func(b *testing.B) {
			for _, numVersions := range []int{1, 2, 10, 100, 1000} {
				b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
					for _, valueSize := range []int{8, 64, 512} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							for _, numRangeKeys := range []int{0, 1, 100} {
								b.Run(fmt.Sprintf("numRangeKeys=%d", numRangeKeys), func(b *testing.B) {
									runMVCCScan(ctx, b, benchScanOptions{
										mvccBenchData: mvccBenchData{
											numVersions:  numVersions,
											valueBytes:   valueSize,
											numRangeKeys: numRangeKeys,
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
		})
	}
}

func BenchmarkMVCCScanTransactionalData_Pebble(b *testing.B) {
	ctx := context.Background()
	defer log.Scope(b).Close(b)
	runMVCCScan(ctx, b, benchScanOptions{
		numRows: 10000,
		mvccBenchData: mvccBenchData{
			numVersions:   2,
			valueBytes:    8,
			transactional: true,
		},
	})
}

func BenchmarkMVCCGet_Pebble(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, batch := range []bool{false, true} {
		b.Run(fmt.Sprintf("batch=%t", batch), func(b *testing.B) {
			for _, numVersions := range []int{10} {
				b.Run(fmt.Sprintf("versions=%d", numVersions), func(b *testing.B) {
					for _, valueSize := range []int{8} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							for _, numRangeKeys := range []int{0} {
								b.Run(fmt.Sprintf("numRangeKeys=%d", numRangeKeys), func(b *testing.B) {
									runMVCCGet(ctx, b, mvccBenchData{
										numVersions:  numVersions,
										valueBytes:   valueSize,
										numRangeKeys: numRangeKeys,
									}, batch)
								})
							}
						})
					}
				})
			}
		})
	}
}

func BenchmarkMVCCComputeStats_Pebble(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, valueSize := range []int{8, 32, 256} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			for _, numRangeKeys := range []int{0, 1, 100} {
				b.Run(fmt.Sprintf("numRangeKeys=%d", numRangeKeys), func(b *testing.B) {
					runMVCCComputeStats(ctx, b, valueSize, numRangeKeys)
				})
			}
		})
	}
}

func BenchmarkMVCCFindSplitKey_Pebble(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, valueSize := range []int{32} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCFindSplitKey(ctx, b, valueSize)
		})
	}
}

func BenchmarkMVCCPut_Pebble(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, batch := range []bool{false, true} {
		b.Run(fmt.Sprintf("batch=%t", batch), func(b *testing.B) {
			for _, valueSize := range []int{10, 100, 1000, 10000} {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					for _, versions := range []int{1, 10} {
						b.Run(fmt.Sprintf("versions=%d", versions), func(b *testing.B) {
							runMVCCPut(ctx, b, setupMVCCInMemPebble, valueSize, versions, batch)
						})
					}
				})
			}
		})
	}
}

func BenchmarkMVCCBlindPut_Pebble(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindPut(ctx, b, setupMVCCInMemPebble, valueSize)
		})
	}
}

func BenchmarkMVCCConditionalPut_Pebble(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, createFirst := range []bool{false, true} {
		prefix := "Create"
		if createFirst {
			prefix = "Replace"
		}
		b.Run(prefix, func(b *testing.B) {
			for _, valueSize := range []int{10, 100, 1000, 10000} {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					runMVCCConditionalPut(ctx, b, setupMVCCInMemPebble, valueSize, createFirst)
				})
			}
		})
	}
}

func BenchmarkMVCCBlindConditionalPut_Pebble(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindConditionalPut(ctx, b, setupMVCCInMemPebble, valueSize)
		})
	}
}

func BenchmarkMVCCInitPut_Pebble(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCInitPut(ctx, b, setupMVCCInMemPebble, valueSize)
		})
	}
}

func BenchmarkMVCCBlindInitPut_Pebble(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCBlindInitPut(ctx, b, setupMVCCInMemPebble, valueSize)
		})
	}
}

func BenchmarkMVCCPutDelete_Pebble(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	db := setupMVCCInMemPebble(b, "put_delete")
	defer db.Close()

	r := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(r, 10))
	var blockNum int64

	for i := 0; i < b.N; i++ {
		blockID := r.Int63()
		blockNum++
		key := encoding.EncodeVarintAscending(nil, blockID)
		key = encoding.EncodeVarintAscending(key, blockNum)

		if err := MVCCPut(ctx, db, nil, key, hlc.Timestamp{}, hlc.ClockTimestamp{}, value, nil); err != nil {
			b.Fatal(err)
		}
		if _, err := MVCCDelete(ctx, db, nil, key, hlc.Timestamp{}, hlc.ClockTimestamp{}, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMVCCBatchPut_Pebble(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, valueSize := range []int{10} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			for _, batchSize := range []int{1, 100, 10000, 100000} {
				b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
					runMVCCBatchPut(ctx, b, setupMVCCInMemPebble, valueSize, batchSize)
				})
			}
		})
	}
}

func BenchmarkMVCCBatchTimeSeries_Pebble(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, batchSize := range []int{282} {
		b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
			runMVCCBatchTimeSeries(ctx, b, setupMVCCInMemPebble, batchSize)
		})
	}
}

// BenchmarkMVCCGetMergedTimeSeries computes performance of reading merged
// time series data using `MVCCGet()`. Uses an in-memory engine.
func BenchmarkMVCCGetMergedTimeSeries_Pebble(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, numKeys := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("numKeys=%d", numKeys), func(b *testing.B) {
			for _, mergesPerKey := range []int{1, 16, 256} {
				b.Run(fmt.Sprintf("mergesPerKey=%d", mergesPerKey), func(b *testing.B) {
					runMVCCGetMergedValue(ctx, b, setupMVCCInMemPebble, numKeys, mergesPerKey)
				})
			}
		})
	}
}

// DeleteRange benchmarks below (using on-disk data).
//
// TODO(peter): Benchmark{MVCCDeleteRange,ClearRange,ClearIterRange}_Pebble
// give nonsensical results (DeleteRange is absurdly slow and ClearRange
// reports a processing speed of 481 million MB/s!). We need to take a look at
// what these benchmarks are trying to measure, and fix them.

func BenchmarkMVCCDeleteRange_Pebble(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, valueSize := range []int{8, 32, 256} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCDeleteRange(ctx, b, valueSize)
		})
	}
}

func BenchmarkMVCCDeleteRangeUsingTombstone_Pebble(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, numKeys := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("numKeys=%d", numKeys), func(b *testing.B) {
			for _, valueSize := range []int{64} {
				b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
					for _, entireRange := range []bool{false, true} {
						b.Run(fmt.Sprintf("entireRange=%t", entireRange), func(b *testing.B) {
							runMVCCDeleteRangeUsingTombstone(ctx, b, numKeys, valueSize, entireRange)
						})
					}
				})
			}
		})
	}
}

func BenchmarkClearMVCCVersions_Pebble(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	runClearRange(ctx, b, func(eng Engine, batch Batch, start, end MVCCKey) error {
		return batch.ClearMVCCVersions(start, end)
	})
}

func BenchmarkClearMVCCIteratorRange_Pebble(b *testing.B) {
	ctx := context.Background()
	defer log.Scope(b).Close(b)
	runClearRange(ctx, b, func(eng Engine, batch Batch, start, end MVCCKey) error {
		return batch.ClearMVCCIteratorRange(start.Key, end.Key, true, true)
	})
}

func BenchmarkBatchApplyBatchRepr_Pebble(b *testing.B) {
	skip.UnderShort(b)
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	for _, indexed := range []bool{false, true} {
		b.Run(fmt.Sprintf("indexed=%t", indexed), func(b *testing.B) {
			for _, sequential := range []bool{false, true} {
				b.Run(fmt.Sprintf("seq=%t", sequential), func(b *testing.B) {
					for _, valueSize := range []int{10} {
						b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
							for _, batchSize := range []int{10000} {
								b.Run(fmt.Sprintf("batchSize=%d", batchSize), func(b *testing.B) {
									runBatchApplyBatchRepr(ctx, b, setupMVCCInMemPebble,
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
	defer log.Scope(b).Close(b)
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	b.ResetTimer()

	const batchSize = 1000
	var batch pebble.Batch
	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		for j := i; j < end; j++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(j)))
			ts := hlc.Timestamp{WallTime: int64(j)}
			require.NoError(b, batch.Set(EncodeMVCCKey(MVCCKey{key, ts}), value, nil /* WriteOptions */))
		}
		batch.Reset()
	}

	b.StopTimer()
}

func BenchmarkCheckSSTConflicts(b *testing.B) {
	defer log.Scope(b).Close(b)
	for _, numKeys := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("keys=%d", numKeys), func(b *testing.B) {
			for _, numSstKeys := range []int{10, 100, 1000, 10000, 100000} {
				b.Run(fmt.Sprintf("sstKeys=%d", numSstKeys), func(b *testing.B) {
					for _, overlap := range []bool{false, true} {
						b.Run(fmt.Sprintf("overlap=%t", overlap), func(b *testing.B) {
							for _, usePrefixSeek := range []bool{false, true} {
								b.Run(fmt.Sprintf("prefixSeek=%t", usePrefixSeek), func(b *testing.B) {
									runCheckSSTConflicts(b, numKeys, 1 /* numVersions */, numSstKeys, overlap, usePrefixSeek)
								})
							}
						})
					}
				})
			}
		})
	}
}

func BenchmarkSSTIterator(b *testing.B) {
	defer log.Scope(b).Close(b)
	for _, numKeys := range []int{1, 100, 10000} {
		b.Run(fmt.Sprintf("keys=%d", numKeys), func(b *testing.B) {
			for _, verify := range []bool{false, true} {
				b.Run(fmt.Sprintf("verify=%t", verify), func(b *testing.B) {
					runSSTIterator(b, numKeys, verify)
				})
			}
		})
	}
}
