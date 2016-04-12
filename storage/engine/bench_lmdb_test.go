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
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/util/stop"
)

var mvccLMDBMaker = engineMaker{
	suffix: "lmdb",
	new: func(b testing.TB, loc string) (Engine, *stop.Stopper) {
		const size = 1 << 35 // 32GB
		eng := NewLMDB(size, loc)
		if err := eng.Open(); err != nil {
			b.Fatalf("could not open LMDB instance at %s: %v", loc, err)
		}
		return eng, stop.NewStopper()
	},
}

var mvccTransientLMDBMaker = engineMaker{
	suffix: "lmdb_transient",
	new: func(b testing.TB, loc string) (Engine, *stop.Stopper) {
		eng, stopper := mvccLMDBMaker.new(b, loc)
		stopper.RunWorker(func() {
			<-stopper.ShouldStop()
			if err := os.RemoveAll(loc); err != nil {
				panic(err)
			}
		})
		return eng, stopper
	},
}

// Read benchmarks. All of them run with on-disk data.

func BenchmarkMVCCScan10Versions1Row64Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 1, 10, 64, b)
}

func BenchmarkMVCCScan10Versions1Row512Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 1, 10, 512, b)
}

func BenchmarkMVCCScan10Versions10Rows8Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 10, 10, 8, b)
}

func BenchmarkMVCCScan10Versions10Rows64Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 10, 10, 64, b)
}

func BenchmarkMVCCScan10Versions10Rows512Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 10, 10, 512, b)
}

func BenchmarkMVCCScan10Versions100Rows8Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 100, 10, 8, b)
}

func BenchmarkMVCCScan10Versions100Rows64Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 100, 10, 64, b)
}

func BenchmarkMVCCScan10Versions100Rows512Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 100, 10, 512, b)
}

func BenchmarkMVCCScan10Versions1000Rows8Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 1000, 10, 8, b)
}

func BenchmarkMVCCScan10Versions1000Rows64Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 1000, 10, 64, b)
}

func BenchmarkMVCCScan10Versions1000Rows512Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 1000, 10, 512, b)
}

func BenchmarkMVCCScan100Versions1Row512Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 1, 100, 512, b)
}

func BenchmarkMVCCScan100Versions10Rows512Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 10, 100, 512, b)
}

func BenchmarkMVCCScan100Versions100Rows512Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 100, 100, 512, b)
}

func BenchmarkMVCCScan100Versions1000Rows512Bytes_LMDB(b *testing.B) {
	runMVCCScan(mvccLMDBMaker, 1000, 100, 512, b)
}

func BenchmarkMVCCGet1Version8Bytes_LMDB(b *testing.B) {
	runMVCCGet(mvccLMDBMaker, 1, 8, b)
}

func BenchmarkMVCCGet10Versions8Bytes_LMDB(b *testing.B) {
	runMVCCGet(mvccLMDBMaker, 10, 8, b)
}

func BenchmarkMVCCGet100Versions8Bytes_LMDB(b *testing.B) {
	runMVCCGet(mvccLMDBMaker, 100, 8, b)
}

func BenchmarkMVCCComputeStats1Version8Bytes_LMDB(b *testing.B) {
	b.Skip("unimplemented")
	runMVCCComputeStats(mvccLMDBMaker, 8, b)
}

func BenchmarkMVCCComputeStats1Version32Bytes_LMDB(b *testing.B) {
	b.Skip("unimplemented")
	runMVCCComputeStats(mvccLMDBMaker, 32, b)
}

func BenchmarkMVCCComputeStats1Version256Bytes_LMDB(b *testing.B) {
	b.Skip("unimplemented")
	runMVCCComputeStats(mvccLMDBMaker, 256, b)
}

func BenchmarkIterOnBatch10_LMDB(b *testing.B) {
	benchmarkIterOnBatch(b, 10)
}

func BenchmarkIterOnBatch100_LMDB(b *testing.B) {
	benchmarkIterOnBatch(b, 100)
}

func BenchmarkIterOnBatch1000_LMDB(b *testing.B) {
	benchmarkIterOnBatch(b, 1000)
}

func BenchmarkIterOnBatch10000_LMDB(b *testing.B) {
	benchmarkIterOnBatch(b, 10000)
}

// Write benchmarks. Unlike with RocksDB, all of them use on-disk data (which
// puts them at a disadvantage).

func BenchmarkMVCCPut10_LMDB(b *testing.B) {
	runMVCCPut(mvccTransientLMDBMaker, 10, b)
}

func BenchmarkMVCCPut100_LMDB(b *testing.B) {
	runMVCCPut(mvccTransientLMDBMaker, 100, b)
}

func BenchmarkMVCCPut1000_LMDB(b *testing.B) {
	runMVCCPut(mvccTransientLMDBMaker, 1000, b)
}

func BenchmarkMVCCPut10000_LMDB(b *testing.B) {
	runMVCCPut(mvccTransientLMDBMaker, 10000, b)
}

func BenchmarkMVCCConditionalPutCreate10_LMDB(b *testing.B) {
	runMVCCConditionalPut(mvccTransientLMDBMaker, 10, false, b)
}

func BenchmarkMVCCConditionalPutCreate100_LMDB(b *testing.B) {
	runMVCCConditionalPut(mvccTransientLMDBMaker, 100, false, b)
}

func BenchmarkMVCCConditionalPutCreate1000_LMDB(b *testing.B) {
	runMVCCConditionalPut(mvccTransientLMDBMaker, 1000, false, b)
}

func BenchmarkMVCCConditionalPutCreate10000_LMDB(b *testing.B) {
	runMVCCConditionalPut(mvccTransientLMDBMaker, 10000, false, b)
}

func BenchmarkMVCCConditionalPutReplace10_LMDB(b *testing.B) {
	runMVCCConditionalPut(mvccTransientLMDBMaker, 10, true, b)
}

func BenchmarkMVCCConditionalPutReplace100_LMDB(b *testing.B) {
	runMVCCConditionalPut(mvccTransientLMDBMaker, 100, true, b)
}

func BenchmarkMVCCConditionalPutReplace1000_LMDB(b *testing.B) {
	runMVCCConditionalPut(mvccTransientLMDBMaker, 1000, true, b)
}

func BenchmarkMVCCConditionalPutReplace10000_LMDB(b *testing.B) {
	runMVCCConditionalPut(mvccTransientLMDBMaker, 10000, true, b)
}
func BenchmarkMVCCBatch1Put10_LMDB(b *testing.B) {
	runMVCCBatchPut(mvccTransientLMDBMaker, 10, 1, b)
}

func BenchmarkMVCCBatch100Put10_LMDB(b *testing.B) {
	runMVCCBatchPut(mvccTransientLMDBMaker, 10, 100, b)
}

func BenchmarkMVCCBatch10000Put10_LMDB(b *testing.B) {
	runMVCCBatchPut(mvccTransientLMDBMaker, 10, 10000, b)
}

func BenchmarkMVCCBatch100000Put10_LMDB(b *testing.B) {
	runMVCCBatchPut(mvccTransientLMDBMaker, 10, 100000, b)
}

// DeleteRange benchmarks below (still using on-disk data).

func BenchmarkMVCCDeleteRange1Version8Bytes_LMDB(b *testing.B) {
	runMVCCDeleteRange(mvccLMDBMaker, 8, b)
}

func BenchmarkMVCCDeleteRange1Version32Bytes_LMDB(b *testing.B) {
	runMVCCDeleteRange(mvccLMDBMaker, 32, b)
}

func BenchmarkMVCCDeleteRange1Version256Bytes_LMDB(b *testing.B) {
	runMVCCDeleteRange(mvccLMDBMaker, 256, b)
}
