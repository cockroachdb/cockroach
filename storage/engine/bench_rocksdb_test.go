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
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/stop"
)

func setupMVCCRocksDB(b testing.TB, loc string) (Engine, *stop.Stopper) {
	const cacheSize = 0
	const memtableBudget = 512 << 20 // 512 MB
	stopper := stop.NewStopper()
	rocksdb := NewRocksDB(roachpb.Attributes{}, loc, cacheSize, memtableBudget, 0, stopper)
	if err := rocksdb.Open(); err != nil {
		b.Fatalf("could not create new rocksdb db instance at %s: %v", loc, err)
	}
	return rocksdb, stopper
}

func setupMVCCInMemRocksDB(_ testing.TB, loc string) (Engine, *stop.Stopper) {
	stopper := stop.NewStopper()
	return NewInMem(roachpb.Attributes{}, testCacheSize, stopper), stopper
}

// Read benchmarks. All of them run with on-disk data.

func BenchmarkMVCCScan10Versions1Row64Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 1, 10, 64, b)
}

func BenchmarkMVCCScan10Versions1Row512Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 1, 10, 512, b)
}

func BenchmarkMVCCScan10Versions10Rows8Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 10, 10, 8, b)
}

func BenchmarkMVCCScan10Versions10Rows64Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 10, 10, 64, b)
}

func BenchmarkMVCCScan10Versions10Rows512Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 10, 10, 512, b)
}

func BenchmarkMVCCScan10Versions100Rows8Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 100, 10, 8, b)
}

func BenchmarkMVCCScan10Versions100Rows64Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 100, 10, 64, b)
}

func BenchmarkMVCCScan10Versions100Rows512Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 100, 10, 512, b)
}

func BenchmarkMVCCScan10Versions1000Rows8Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 1000, 10, 8, b)
}

func BenchmarkMVCCScan10Versions1000Rows64Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 1000, 10, 64, b)
}

func BenchmarkMVCCScan10Versions1000Rows512Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 1000, 10, 512, b)
}

func BenchmarkMVCCScan100Versions1Row512Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 1, 100, 512, b)
}

func BenchmarkMVCCScan100Versions10Rows512Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 10, 100, 512, b)
}

func BenchmarkMVCCScan100Versions100Rows512Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 100, 100, 512, b)
}

func BenchmarkMVCCScan100Versions1000Rows512Bytes_RocksDB(b *testing.B) {
	runMVCCScan(setupMVCCRocksDB, 1000, 100, 512, b)
}

func BenchmarkMVCCGet1Version8Bytes_RocksDB(b *testing.B) {
	runMVCCGet(setupMVCCRocksDB, 1, 8, b)
}

func BenchmarkMVCCGet10Versions8Bytes_RocksDB(b *testing.B) {
	runMVCCGet(setupMVCCRocksDB, 10, 8, b)
}

func BenchmarkMVCCGet100Versions8Bytes_RocksDB(b *testing.B) {
	runMVCCGet(setupMVCCRocksDB, 100, 8, b)
}

func BenchmarkMVCCComputeStats1Version8Bytes_RocksDB(b *testing.B) {
	runMVCCComputeStats(setupMVCCRocksDB, 8, b)
}

func BenchmarkMVCCComputeStats1Version32Bytes_RocksDB(b *testing.B) {
	runMVCCComputeStats(setupMVCCRocksDB, 32, b)
}

func BenchmarkMVCCComputeStats1Version256Bytes_RocksDB(b *testing.B) {
	runMVCCComputeStats(setupMVCCRocksDB, 256, b)
}

func BenchmarkIterOnBatch10_RocksDB(b *testing.B) {
	benchmarkIterOnBatch(b, 10)
}

func BenchmarkIterOnBatch100_RocksDB(b *testing.B) {
	benchmarkIterOnBatch(b, 100)
}

func BenchmarkIterOnBatch1000_RocksDB(b *testing.B) {
	benchmarkIterOnBatch(b, 1000)
}

func BenchmarkIterOnBatch10000_RocksDB(b *testing.B) {
	benchmarkIterOnBatch(b, 10000)
}

// Write benchmarks. Most of them run in-memory except for DeleteRange benchs,
// which make more sense when data is present.

func BenchmarkMVCCPut10_RocksDB(b *testing.B) {
	runMVCCPut(setupMVCCInMemRocksDB, 10, b)
}

func BenchmarkMVCCPut100_RocksDB(b *testing.B) {
	runMVCCPut(setupMVCCInMemRocksDB, 100, b)
}

func BenchmarkMVCCPut1000_RocksDB(b *testing.B) {
	runMVCCPut(setupMVCCInMemRocksDB, 1000, b)
}

func BenchmarkMVCCPut10000_RocksDB(b *testing.B) {
	runMVCCPut(setupMVCCInMemRocksDB, 10000, b)
}

func BenchmarkMVCCConditionalPutCreate10_RocksDB(b *testing.B) {
	runMVCCConditionalPut(setupMVCCInMemRocksDB, 10, false, b)
}

func BenchmarkMVCCConditionalPutCreate100_RocksDB(b *testing.B) {
	runMVCCConditionalPut(setupMVCCInMemRocksDB, 100, false, b)
}

func BenchmarkMVCCConditionalPutCreate1000_RocksDB(b *testing.B) {
	runMVCCConditionalPut(setupMVCCInMemRocksDB, 1000, false, b)
}

func BenchmarkMVCCConditionalPutCreate10000_RocksDB(b *testing.B) {
	runMVCCConditionalPut(setupMVCCInMemRocksDB, 10000, false, b)
}

func BenchmarkMVCCConditionalPutReplace10_RocksDB(b *testing.B) {
	runMVCCConditionalPut(setupMVCCInMemRocksDB, 10, true, b)
}

func BenchmarkMVCCConditionalPutReplace100_RocksDB(b *testing.B) {
	runMVCCConditionalPut(setupMVCCInMemRocksDB, 100, true, b)
}

func BenchmarkMVCCConditionalPutReplace1000_RocksDB(b *testing.B) {
	runMVCCConditionalPut(setupMVCCInMemRocksDB, 1000, true, b)
}

func BenchmarkMVCCConditionalPutReplace10000_RocksDB(b *testing.B) {
	runMVCCConditionalPut(setupMVCCInMemRocksDB, 10000, true, b)
}
func BenchmarkMVCCBatch1Put10_RocksDB(b *testing.B) {
	runMVCCBatchPut(setupMVCCInMemRocksDB, 10, 1, b)
}

func BenchmarkMVCCBatch100Put10_RocksDB(b *testing.B) {
	runMVCCBatchPut(setupMVCCInMemRocksDB, 10, 100, b)
}

func BenchmarkMVCCBatch10000Put10_RocksDB(b *testing.B) {
	runMVCCBatchPut(setupMVCCInMemRocksDB, 10, 10000, b)
}

func BenchmarkMVCCBatch100000Put10_RocksDB(b *testing.B) {
	runMVCCBatchPut(setupMVCCInMemRocksDB, 10, 100000, b)
}

// DeleteRange benchmarks below (using on-disk data).

func BenchmarkMVCCDeleteRange1Version8Bytes_RocksDB(b *testing.B) {
	runMVCCDeleteRange(setupMVCCRocksDB, 8, b)
}

func BenchmarkMVCCDeleteRange1Version32Bytes_RocksDB(b *testing.B) {
	runMVCCDeleteRange(setupMVCCRocksDB, 32, b)
}

func BenchmarkMVCCDeleteRange1Version256Bytes_RocksDB(b *testing.B) {
	runMVCCDeleteRange(setupMVCCRocksDB, 256, b)
}
