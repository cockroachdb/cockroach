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
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/termie/go-shutil"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/stop"
)

const testCacheSize = 1 << 30 // 1 GB

func TestMinMemtableBudget(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rocksdb := NewRocksDB(roachpb.Attributes{}, ".", 0, 0, 0, stop.NewStopper())
	const expected = "memtable budget must be at least"
	if err := rocksdb.Open(); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but got %v", expected, err)
	}
}

// readAllFiles reads all of the files matching pattern thus ensuring they are
// in the OS buffer cache.
func readAllFiles(pattern string) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return
	}
	for _, m := range matches {
		f, err := os.Open(m)
		if err != nil {
			continue
		}
		_, _ = io.Copy(ioutil.Discard, bufio.NewReader(f))
		f.Close()
	}
}

// setupMVCCData writes up to numVersions values at each of numKeys
// keys. The number of versions written for each key is chosen
// randomly according to a uniform distribution. Each successive
// version is written starting at 5ns and then in 5ns increments. This
// allows scans at various times, starting at t=5ns, and continuing to
// t=5ns*(numVersions+1). A version for each key will be read on every
// such scan, but the dynamics of the scan will change depending on
// the historical timestamp. Earlier timestamps mean scans which must
// skip more historical versions; later timestamps mean scans which
// skip fewer.
//
// The creation of the rocksdb database is time consuming, especially
// for larger numbers of versions. The database is persisted between
// runs and stored in the current directory as
// "mvcc_scan_<versions>_<keys>_<valueBytes>".
func setupMVCCData(numVersions, numKeys, valueBytes int, b *testing.B) (*RocksDB, *stop.Stopper) {
	loc := fmt.Sprintf("mvcc_data_%d_%d_%d", numVersions, numKeys, valueBytes)

	exists := true
	if _, err := os.Stat(loc); os.IsNotExist(err) {
		exists = false
	}

	const cacheSize = 0
	const memtableBudget = 512 << 20 // 512 MB
	stopper := stop.NewStopper()
	rocksdb := NewRocksDB(roachpb.Attributes{}, loc, cacheSize, memtableBudget, 0, stopper)
	if err := rocksdb.Open(); err != nil {
		b.Fatalf("could not create new rocksdb db instance at %s: %v", loc, err)
	}

	if exists {
		readAllFiles(filepath.Join(loc, "*"))
		return rocksdb, stopper
	}

	log.Infof("creating mvcc data: %s", loc)

	// Generate the same data every time.
	rng := rand.New(rand.NewSource(1449168817))

	keys := make([]roachpb.Key, numKeys)
	var order []int
	for i := 0; i < numKeys; i++ {
		keys[i] = roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(i)))
		keyVersions := rng.Intn(numVersions) + 1
		for j := 0; j < keyVersions; j++ {
			order = append(order, i)
		}
	}

	// Randomize the order in which the keys are written.
	for i, n := 0, len(order); i < n-1; i++ {
		j := i + rng.Intn(n-i)
		order[i], order[j] = order[j], order[i]
	}

	counts := make([]int, numKeys)
	batch := rocksdb.NewBatch()
	for i, idx := range order {
		// Output the keys in ~20 batches. If we used a single batch to output all
		// of the keys rocksdb would create a single sstable. We want multiple
		// sstables in order to exercise filtering of which sstables are examined
		// during iterator seeking. We fix the number of batches we output so that
		// optimizations which change the data size result in the same number of
		// sstables.
		if i > 0 && (i%(len(order)/20)) == 0 {
			if err := batch.Commit(); err != nil {
				b.Fatal(err)
			}
			batch.Close()
			batch = rocksdb.NewBatch()
			if err := rocksdb.Flush(); err != nil {
				b.Fatal(err)
			}
		}

		key := keys[idx]
		ts := makeTS(int64(counts[idx]+1)*5, 0)
		counts[idx]++
		value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueBytes))
		value.InitChecksum(key)
		if err := MVCCPut(batch, nil, key, ts, value, nil); err != nil {
			b.Fatal(err)
		}
	}
	if err := batch.Commit(); err != nil {
		b.Fatal(err)
	}
	batch.Close()
	if err := rocksdb.Flush(); err != nil {
		b.Fatal(err)
	}

	return rocksdb, stopper
}

// runMVCCScan first creates test data (and resets the benchmarking
// timer). It then performs b.N MVCCScans in increments of numRows
// keys over all of the data in the rocksdb instance, restarting at
// the beginning of the keyspace, as many times as necessary.
func runMVCCScan(numRows, numVersions, valueSize int, b *testing.B) {
	// Use the same number of keys for all of the mvcc scan
	// benchmarks. Using a different number of keys per test gives
	// preferential treatment to tests with fewer keys. Note that the
	// datasets all fit in cache and the cache is pre-warmed.
	const numKeys = 100000

	rocksdb, stopper := setupMVCCData(numVersions, numKeys, valueSize, b)
	defer stopper.Stop()

	b.SetBytes(int64(numRows * valueSize))
	b.ResetTimer()

	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)
	for i := 0; i < b.N; i++ {
		// Choose a random key to start scan.
		keyIdx := rand.Int31n(int32(numKeys - numRows))
		startKey := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(keyIdx)))
		walltime := int64(5 * (rand.Int31n(int32(numVersions)) + 1))
		ts := makeTS(walltime, 0)
		kvs, _, err := MVCCScan(rocksdb, startKey, keyMax, int64(numRows), ts, true, nil)
		if err != nil {
			b.Fatalf("failed scan: %s", err)
		}
		if len(kvs) != numRows {
			b.Fatalf("failed to scan: %d != %d", len(kvs), numRows)
		}
	}

	b.StopTimer()
}

func BenchmarkMVCCScan1Version1Row8Bytes(b *testing.B) {
	runMVCCScan(1, 1, 8, b)
}

func BenchmarkMVCCScan1Version1Row64Bytes(b *testing.B) {
	runMVCCScan(1, 1, 64, b)
}

func BenchmarkMVCCScan1Version1Row512Bytes(b *testing.B) {
	runMVCCScan(1, 1, 512, b)
}

func BenchmarkMVCCScan1Version10Rows8Bytes(b *testing.B) {
	runMVCCScan(10, 1, 8, b)
}

func BenchmarkMVCCScan1Version10Rows64Bytes(b *testing.B) {
	runMVCCScan(10, 1, 64, b)
}

func BenchmarkMVCCScan1Version10Rows512Bytes(b *testing.B) {
	runMVCCScan(10, 1, 512, b)
}

func BenchmarkMVCCScan1Version100Rows8Bytes(b *testing.B) {
	runMVCCScan(100, 1, 8, b)
}

func BenchmarkMVCCScan1Version100Rows64Bytes(b *testing.B) {
	runMVCCScan(100, 1, 64, b)
}

func BenchmarkMVCCScan1Version100Rows512Bytes(b *testing.B) {
	runMVCCScan(100, 1, 512, b)
}

func BenchmarkMVCCScan1Version1000Rows8Bytes(b *testing.B) {
	runMVCCScan(1000, 1, 8, b)
}

func BenchmarkMVCCScan1Version1000Rows64Bytes(b *testing.B) {
	runMVCCScan(1000, 1, 64, b)
}

func BenchmarkMVCCScan1Version1000Rows512Bytes(b *testing.B) {
	runMVCCScan(1000, 1, 512, b)
}

func BenchmarkMVCCScan10Versions1Row8Bytes(b *testing.B) {
	runMVCCScan(1, 10, 8, b)
}

func BenchmarkMVCCScan10Versions1Row64Bytes(b *testing.B) {
	runMVCCScan(1, 10, 64, b)
}

func BenchmarkMVCCScan10Versions1Row512Bytes(b *testing.B) {
	runMVCCScan(1, 10, 512, b)
}

func BenchmarkMVCCScan10Versions10Rows8Bytes(b *testing.B) {
	runMVCCScan(10, 10, 8, b)
}

func BenchmarkMVCCScan10Versions10Rows64Bytes(b *testing.B) {
	runMVCCScan(10, 10, 64, b)
}

func BenchmarkMVCCScan10Versions10Rows512Bytes(b *testing.B) {
	runMVCCScan(10, 10, 512, b)
}

func BenchmarkMVCCScan10Versions100Rows8Bytes(b *testing.B) {
	runMVCCScan(100, 10, 8, b)
}

func BenchmarkMVCCScan10Versions100Rows64Bytes(b *testing.B) {
	runMVCCScan(100, 10, 64, b)
}

func BenchmarkMVCCScan10Versions100Rows512Bytes(b *testing.B) {
	runMVCCScan(100, 10, 512, b)
}

func BenchmarkMVCCScan10Versions1000Rows8Bytes(b *testing.B) {
	runMVCCScan(1000, 10, 8, b)
}

func BenchmarkMVCCScan10Versions1000Rows64Bytes(b *testing.B) {
	runMVCCScan(1000, 10, 64, b)
}

func BenchmarkMVCCScan10Versions1000Rows512Bytes(b *testing.B) {
	runMVCCScan(1000, 10, 512, b)
}

func BenchmarkMVCCScan100Versions1Row512Bytes(b *testing.B) {
	runMVCCScan(1, 100, 512, b)
}

func BenchmarkMVCCScan100Versions10Rows512Bytes(b *testing.B) {
	runMVCCScan(10, 100, 512, b)
}

func BenchmarkMVCCScan100Versions100Rows512Bytes(b *testing.B) {
	runMVCCScan(100, 100, 512, b)
}

func BenchmarkMVCCScan100Versions1000Rows512Bytes(b *testing.B) {
	runMVCCScan(1000, 100, 512, b)
}

// runMVCCGet first creates test data (and resets the benchmarking
// timer). It then performs b.N MVCCGets.
func runMVCCGet(numVersions, valueSize int, b *testing.B) {
	const overhead = 48          // Per key/value overhead (empirically determined)
	const targetSize = 512 << 20 // 512 MB
	// Adjust the number of keys so that each test has approximately the same
	// amount of data.
	numKeys := targetSize / ((overhead + valueSize) * (1 + (numVersions-1)/2))

	rocksdb, stopper := setupMVCCData(numVersions, numKeys, valueSize, b)
	defer stopper.Stop()

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)
	for i := 0; i < b.N; i++ {
		// Choose a random key to retrieve.
		keyIdx := rand.Int31n(int32(numKeys))
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(keyIdx)))
		walltime := int64(5 * (rand.Int31n(int32(numVersions)) + 1))
		ts := makeTS(walltime, 0)
		if v, _, err := MVCCGet(rocksdb, key, ts, true, nil); err != nil {
			b.Fatalf("failed get: %s", err)
		} else if v == nil {
			b.Fatalf("failed get (key not found): %d@%d", keyIdx, walltime)
		} else if valueBytes, err := v.GetBytes(); err != nil {
			b.Fatal(err)
		} else if len(valueBytes) != valueSize {
			b.Fatalf("unexpected value size: %d", len(valueBytes))
		}
	}

	b.StopTimer()
}

func BenchmarkMVCCGet1Version8Bytes(b *testing.B) {
	runMVCCGet(1, 8, b)
}

func BenchmarkMVCCGet10Versions8Bytes(b *testing.B) {
	runMVCCGet(10, 8, b)
}

func BenchmarkMVCCGet100Versions8Bytes(b *testing.B) {
	runMVCCGet(100, 8, b)
}

func runMVCCPut(valueSize int, b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	stopper := stop.NewStopper()
	defer stopper.Stop()
	rocksdb := NewInMem(roachpb.Attributes{}, testCacheSize, stopper)

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := makeTS(time.Now().UnixNano(), 0)
		if err := MVCCPut(rocksdb, nil, key, ts, value, nil); err != nil {
			b.Fatalf("failed put: %s", err)
		}
	}

	b.StopTimer()
}

func BenchmarkMVCCPut10(b *testing.B) {
	runMVCCPut(10, b)
}

func BenchmarkMVCCPut100(b *testing.B) {
	runMVCCPut(100, b)
}

func BenchmarkMVCCPut1000(b *testing.B) {
	runMVCCPut(1000, b)
}

func BenchmarkMVCCPut10000(b *testing.B) {
	runMVCCPut(10000, b)
}

func runMVCCConditionalPut(valueSize int, createFirst bool, b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	stopper := stop.NewStopper()
	defer stopper.Stop()
	rocksdb := NewInMem(roachpb.Attributes{}, testCacheSize, stopper)

	b.SetBytes(int64(valueSize))
	var expected *roachpb.Value
	if createFirst {
		for i := 0; i < b.N; i++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
			ts := makeTS(time.Now().UnixNano(), 0)
			if err := MVCCPut(rocksdb, nil, key, ts, value, nil); err != nil {
				b.Fatalf("failed put: %s", err)
			}
		}
		expected = &value
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(i)))
		ts := makeTS(time.Now().UnixNano(), 0)
		if err := MVCCConditionalPut(rocksdb, nil, key, ts, value, expected, nil); err != nil {
			b.Fatalf("failed put: %s", err)
		}
	}

	b.StopTimer()
}

func BenchmarkMVCCConditionalPutCreate10(b *testing.B) {
	runMVCCConditionalPut(10, false, b)
}

func BenchmarkMVCCConditionalPutCreate100(b *testing.B) {
	runMVCCConditionalPut(100, false, b)
}

func BenchmarkMVCCConditionalPutCreate1000(b *testing.B) {
	runMVCCConditionalPut(1000, false, b)
}

func BenchmarkMVCCConditionalPutCreate10000(b *testing.B) {
	runMVCCConditionalPut(10000, false, b)
}

func BenchmarkMVCCConditionalPutReplace10(b *testing.B) {
	runMVCCConditionalPut(10, true, b)
}

func BenchmarkMVCCConditionalPutReplace100(b *testing.B) {
	runMVCCConditionalPut(100, true, b)
}

func BenchmarkMVCCConditionalPutReplace1000(b *testing.B) {
	runMVCCConditionalPut(1000, true, b)
}

func BenchmarkMVCCConditionalPutReplace10000(b *testing.B) {
	runMVCCConditionalPut(10000, true, b)
}

func runMVCCBatchPut(valueSize, batchSize int, b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	stopper := stop.NewStopper()
	defer stopper.Stop()
	rocksdb := NewInMem(roachpb.Attributes{}, testCacheSize, stopper)

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		batch := rocksdb.NewBatch()

		for j := i; j < end; j++ {
			key := roachpb.Key(encoding.EncodeUvarintAscending(keyBuf[:4], uint64(j)))
			ts := makeTS(time.Now().UnixNano(), 0)
			if err := MVCCPut(batch, nil, key, ts, value, nil); err != nil {
				b.Fatalf("failed put: %s", err)
			}
		}

		if err := batch.Commit(); err != nil {
			b.Fatal(err)
		}

		batch.Close()
	}

	b.StopTimer()
}

func BenchmarkMVCCBatch1Put10(b *testing.B) {
	runMVCCBatchPut(10, 1, b)
}

func BenchmarkMVCCBatch100Put10(b *testing.B) {
	runMVCCBatchPut(10, 100, b)
}

func BenchmarkMVCCBatch10000Put10(b *testing.B) {
	runMVCCBatchPut(10, 10000, b)
}

func BenchmarkMVCCBatch100000Put10(b *testing.B) {
	runMVCCBatchPut(10, 100000, b)
}

// runMVCCMerge merges value into numKeys separate keys.
func runMVCCMerge(value *roachpb.Value, numKeys int, b *testing.B) {
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rocksdb := NewInMem(roachpb.Attributes{}, testCacheSize, stopper)

	// Precompute keys so we don't waste time formatting them at each iteration.
	keys := make([]roachpb.Key, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = roachpb.Key(fmt.Sprintf("key-%d", i))
	}

	b.ResetTimer()

	ts := roachpb.Timestamp{}
	// Use parallelism if specified when test is run.
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ms := MVCCStats{}
			ts.Logical++
			err := MVCCMerge(rocksdb, &ms, keys[rand.Intn(numKeys)], ts, *value)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Read values out to force merge.
	for _, key := range keys {
		val, _, err := MVCCGet(rocksdb, key, roachpb.ZeroTimestamp, true, nil)
		if err != nil {
			b.Fatal(err)
		} else if val == nil {
			continue
		}
	}

	b.StopTimer()
}

// BenchmarkMVCCMergeTimeSeries computes performance of merging time series data.
func BenchmarkMVCCMergeTimeSeries(b *testing.B) {
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
	runMVCCMerge(&value, 1024, b)
}

func runMVCCDeleteRange(valueBytes int, b *testing.B) {
	// 512 KB ranges so the benchmark doesn't take forever
	const rangeBytes = 512 * 1024
	const overhead = 48 // Per key/value overhead (empirically determined)
	numKeys := rangeBytes / (overhead + valueBytes)
	rocksdb, stopper := setupMVCCData(1, numKeys, valueBytes, b)
	stopper.Stop()

	b.SetBytes(rangeBytes)
	b.StopTimer()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		locDirty := rocksdb.dir + "_dirty"
		if err := os.RemoveAll(locDirty); err != nil {
			b.Fatal(err)
		}
		if err := shutil.CopyTree(rocksdb.dir, locDirty, nil); err != nil {
			b.Fatal(err)
		}
		stopper := stop.NewStopper()
		dupRocksdb := NewRocksDB(roachpb.Attributes{}, locDirty, rocksdb.cacheSize,
			rocksdb.memtableBudget, 0, stopper)
		if err := dupRocksdb.Open(); err != nil {
			b.Fatal(err)
		}

		b.StartTimer()
		_, err := MVCCDeleteRange(dupRocksdb, &MVCCStats{}, roachpb.KeyMin, roachpb.KeyMax, 0, roachpb.MaxTimestamp, nil, false)
		if err != nil {
			b.Fatal(err)
		}
		b.StopTimer()

		stopper.Stop()
	}
}

func BenchmarkMVCCDeleteRange1Version8Bytes(b *testing.B) {
	runMVCCDeleteRange(8, b)
}

func BenchmarkMVCCDeleteRange1Version32Bytes(b *testing.B) {
	runMVCCDeleteRange(32, b)
}

func BenchmarkMVCCDeleteRange1Version256Bytes(b *testing.B) {
	runMVCCDeleteRange(256, b)
}

// runMVCCComputeStats benchmarks computing MVCC stats on a 64MB range of data.
func runMVCCComputeStats(valueBytes int, b *testing.B) {
	const rangeBytes = 64 * 1024 * 1024
	const overhead = 48 // Per key/value overhead (empirically determined)
	numKeys := rangeBytes / (overhead + valueBytes)
	rocksdb, stopper := setupMVCCData(1, numKeys, valueBytes, b)
	defer stopper.Stop()

	b.SetBytes(rangeBytes)
	b.ResetTimer()

	var stats MVCCStats
	var err error
	for i := 0; i < b.N; i++ {
		iter := rocksdb.NewIterator(nil)
		stats, err = iter.ComputeStats(mvccKey(roachpb.KeyMin), mvccKey(roachpb.KeyMax), 0)
		iter.Close()
		if err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	log.Infof("live_bytes: %d", stats.LiveBytes)
}

func BenchmarkMVCCComputeStats1Version8Bytes(b *testing.B) {
	runMVCCComputeStats(8, b)
}

func BenchmarkMVCCComputeStats1Version32Bytes(b *testing.B) {
	runMVCCComputeStats(32, b)
}

func BenchmarkMVCCComputeStats1Version256Bytes(b *testing.B) {
	runMVCCComputeStats(256, b)
}

func BenchmarkMVCCPutDelete(b *testing.B) {
	const cacheSize = 1 << 30 // 1 GB

	stopper := stop.NewStopper()
	rocksdb := NewInMem(roachpb.Attributes{}, cacheSize, stopper)
	defer stopper.Stop()

	r := rand.New(rand.NewSource(int64(time.Now().UnixNano())))
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(r, 10))
	zeroTS := roachpb.ZeroTimestamp
	var blockNum int64

	for i := 0; i < b.N; i++ {
		blockID := r.Int63()
		blockNum++
		key := encoding.EncodeVarintAscending(nil, blockID)
		key = encoding.EncodeVarintAscending(key, blockNum)

		if err := MVCCPut(rocksdb, nil, key, zeroTS, value, nil /* txn */); err != nil {
			b.Fatal(err)
		}
		if err := MVCCDelete(rocksdb, nil, key, zeroTS, nil /* txn */); err != nil {
			b.Fatal(err)
		}
	}
}

func TestBatchIterReadOwnWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const cacheSize = 1 << 30 // 1 GB

	stopper := stop.NewStopper()
	db := NewInMem(roachpb.Attributes{}, cacheSize, stopper)
	defer stopper.Stop()

	b := db.NewBatch()

	k := MakeMVCCMetadataKey(testKey1)

	before := b.NewIterator(nil)
	defer before.Close()

	nonBatchBefore := db.NewIterator(nil)
	defer nonBatchBefore.Close()

	if err := b.Put(k, []byte("abc")); err != nil {
		t.Fatal(err)
	}

	after := b.NewIterator(nil)
	defer after.Close()

	if after.Seek(k); !after.Valid() {
		t.Fatal("write missing on batch iter created after write")
	}
	if before.Seek(k); !before.Valid() {
		t.Fatal("write missing on batch iter created before write")
	}
	if nonBatchBefore.Seek(k); nonBatchBefore.Valid() {
		t.Fatal("uncommitted write seen by non-batch iter")
	}

	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}

	nonBatchAfter := db.NewIterator(nil)
	defer nonBatchAfter.Close()

	if nonBatchBefore.Seek(k); nonBatchBefore.Valid() {
		t.Fatal("committed write seen by non-batch iter created before commit")
	}
	if nonBatchAfter.Seek(k); !nonBatchAfter.Valid() {
		t.Fatal("committed write missing by non-batch iter created after commit")
	}

	// `Commit` frees the batch, so iterators backed by it should panic.
	func() {
		defer func() {
			if err, expected := recover(), "iterator used after backing engine closed"; err != expected {
				t.Fatalf("Unexpected panic: expected %q, got %q", expected, err)
			}
		}()
		after.Seek(k)
		t.Fatalf(`Seek on batch-backed iter after batched closed should panic.
			iter.engine: %T, iter.engine.Closed: %v, batch.Closed %v`,
			after.(*rocksDBIterator).engine,
			after.(*rocksDBIterator).engine.Closed(),
			b.Closed(),
		)
	}()
}
