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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package engine

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func setupMVCCRocksDB(b testing.TB, loc string) Engine {
	rocksdb, err := NewRocksDB(
		roachpb.Attributes{},
		loc,
		RocksDBCache{},
		0,
		DefaultMaxOpenFiles,
	)
	if err != nil {
		b.Fatalf("could not create new rocksdb db instance at %s: %v", loc, err)
	}
	return rocksdb
}

func setupMVCCInMemRocksDB(_ testing.TB, loc string) Engine {
	return NewInMem(roachpb.Attributes{}, testCacheSize)
}

// Read benchmarks. All of them run with on-disk data.

func BenchmarkMVCCScan_RocksDB(b *testing.B) {
	for _, numRows := range []int{1, 10, 100, 1000} {
		for _, numVersions := range []int{10, 100} {
			for _, valueSize := range []int{8, 64, 512} {
				b.Run(fmt.Sprintf("%dVersions%dRows%dBytes", numVersions, numRows, valueSize), func(b *testing.B) {
					runMVCCScan(setupMVCCRocksDB, numRows, numVersions, valueSize, b)
				})
			}
		}
	}
}

func BenchmarkMVCCGet_RocksDB(b *testing.B) {
	for _, numVersions := range []int{1, 10, 100} {
		for _, valueSize := range []int{8} {
			b.Run(fmt.Sprintf("%dVersions%dBytes", numVersions, valueSize), func(b *testing.B) {
				runMVCCGet(setupMVCCRocksDB, numVersions, valueSize, b)
			})
		}
	}
}

func BenchmarkMVCCComputeStats_RocksDB(b *testing.B) {
	for _, valueSize := range []int{8, 32, 256} {
		b.Run(fmt.Sprintf("1Version%dBytes", valueSize), func(b *testing.B) {
			runMVCCComputeStats(setupMVCCRocksDB, valueSize, b)
		})
	}
}

func BenchmarkIterOnBatch_RocksDB(b *testing.B) {
	for _, writes := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("%d", writes), func(b *testing.B) {
			benchmarkIterOnBatch(b, writes)
		})
	}
}

// Write benchmarks. Most of them run in-memory except for DeleteRange benchs,
// which make more sense when data is present.

func BenchmarkMVCCPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("%dBytes", valueSize), func(b *testing.B) {
			runMVCCPut(setupMVCCInMemRocksDB, valueSize, b)
		})
	}
}

func BenchmarkMVCCBlindPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("%dBytes", valueSize), func(b *testing.B) {
			runMVCCBlindPut(setupMVCCInMemRocksDB, valueSize, b)
		})
	}
}

func BenchmarkMVCCConditionalPut_RocksDB(b *testing.B) {
	for _, createFirst := range []bool{false, true} {
		for _, valueSize := range []int{10, 100, 1000, 10000} {
			prefix := "Create"
			if createFirst {
				prefix = "Replace"
			}
			b.Run(fmt.Sprintf("%s%dBytes", prefix, valueSize), func(b *testing.B) {
				runMVCCConditionalPut(setupMVCCInMemRocksDB, valueSize, createFirst, b)
			})
		}
	}
}

func BenchmarkMVCCBlindConditionalPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10, 100, 1000, 10000} {
		b.Run(fmt.Sprintf("%dBytes", valueSize), func(b *testing.B) {
			runMVCCBlindConditionalPut(setupMVCCInMemRocksDB, valueSize, b)
		})
	}
}

func BenchmarkMVCCBatchPut_RocksDB(b *testing.B) {
	for _, valueSize := range []int{10} {
		for _, batchSize := range []int{1, 100, 10000, 100000} {
			b.Run(fmt.Sprintf("%dx%dBytes", batchSize, valueSize), func(b *testing.B) {
				runMVCCBatchPut(setupMVCCInMemRocksDB, valueSize, batchSize, b)
			})
		}
	}
}

func BenchmarkMVCCBatchTimeSeries_RocksDB(b *testing.B) {
	for _, batchSize := range []int{282} {
		b.Run(fmt.Sprintf("%d", batchSize), func(b *testing.B) {
			runMVCCBatchTimeSeries(setupMVCCInMemRocksDB, batchSize, b)
		})
	}
}

// DeleteRange benchmarks below (using on-disk data).

func BenchmarkMVCCDeleteRange_RocksDB(b *testing.B) {
	for _, valueSize := range []int{8, 32, 256} {
		b.Run(fmt.Sprintf("%dBytes", valueSize), func(b *testing.B) {
			runMVCCDeleteRange(setupMVCCRocksDB, valueSize, b)
		})
	}
}

func BenchmarkBatchApplyBatchRepr(b *testing.B) {
	for _, writeOnly := range []bool{false, true} {
		for _, valueSize := range []int{10} {
			for _, batchSize := range []int{1000000} {
				b.Run(fmt.Sprintf("writeOnly=%t valueSize=%d batchSize=%d", writeOnly, valueSize, batchSize), func(b *testing.B) {
					runBatchApplyBatchRepr(setupMVCCInMemRocksDB, writeOnly, valueSize, batchSize, b)
				})
			}
		}
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
