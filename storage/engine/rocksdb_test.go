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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package engine

import (
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/uuid"
	"github.com/gogo/protobuf/proto"
)

const testCacheSize = 1 << 30 // GB.

// encodePutResponse creates a put response using the specified
// timestamp and encodes it using gogoprotobuf.
func encodePutResponse(timestamp roachpb.Timestamp, t *testing.T) []byte {
	batch := &roachpb.BatchResponse{}
	batch.Timestamp = timestamp
	batch.Add(&roachpb.PutResponse{
		ResponseHeader: roachpb.ResponseHeader{
			Timestamp: timestamp,
		},
	})
	data, err := proto.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// encodeTransaction creates a transaction using the specified
// timestamp and encodes it using gogoprotobuf.
func encodeTransaction(timestamp roachpb.Timestamp, t *testing.T) []byte {
	txn := &roachpb.Transaction{
		Timestamp: timestamp,
	}
	data, err := proto.Marshal(txn)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestRocksDBCompaction verifies that a garbage collector can be
// installed on a RocksDB engine and will properly compact transaction
// entries.
func TestRocksDBCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)
	stopper := stop.NewStopper()
	defer stopper.Stop()
	rocksdb := newMemRocksDB(roachpb.Attributes{Attrs: []string{"ssd"}}, testCacheSize, stopper)
	err := rocksdb.Open()
	if err != nil {
		t.Fatalf("could not create new in-memory rocksdb db instance: %v", err)
	}
	rocksdb.SetGCTimeouts(1)

	// Write two transaction values such that exactly one should be GC'd based
	// on our GC timeouts.
	kvs := []roachpb.KeyValue{
		{
			Key:   keys.TransactionKey(roachpb.Key("a"), roachpb.Key(uuid.NewUUID4())),
			Value: roachpb.MakeValueFromBytes(encodeTransaction(makeTS(1, 0), t)),
		},
		{
			Key:   keys.TransactionKey(roachpb.Key("b"), roachpb.Key(uuid.NewUUID4())),
			Value: roachpb.MakeValueFromBytes(encodeTransaction(makeTS(2, 0), t)),
		},
	}
	for _, kv := range kvs {
		if err := MVCCPut(rocksdb, nil, kv.Key, roachpb.ZeroTimestamp, kv.Value, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Compact range and scan remaining values to compare.
	rocksdb.CompactRange(nil, nil)
	actualKVs, _, err := MVCCScan(rocksdb, keyMin, keyMax, 0, roachpb.ZeroTimestamp, true, nil)
	if err != nil {
		t.Fatalf("could not run scan: %v", err)
	}
	var keys []roachpb.Key
	for _, kv := range actualKVs {
		keys = append(keys, kv.Key)
	}
	expKeys := []roachpb.Key{
		kvs[1].Key,
	}
	if !reflect.DeepEqual(expKeys, keys) {
		t.Errorf("expected keys %+v, got keys %+v", expKeys, keys)
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
func setupMVCCScanData(numVersions, numKeys, valueBytes int, b *testing.B) (*RocksDB, *stop.Stopper) {
	loc := fmt.Sprintf("mvcc_scan_%d_%d_%d", numVersions, numKeys, valueBytes)

	exists := true
	if _, err := os.Stat(loc); os.IsNotExist(err) {
		exists = false
	}

	const cacheSize = 8 << 30 // 8 GB
	stopper := stop.NewStopper()
	rocksdb := NewRocksDB(roachpb.Attributes{Attrs: []string{"ssd"}}, loc, cacheSize, stopper)
	if err := rocksdb.Open(); err != nil {
		b.Fatalf("could not create new rocksdb db instance at %s: %v", loc, err)
	}

	if exists {
		return rocksdb, stopper
	}

	log.Infof("creating mvcc data: %s", loc)

	rng, _ := randutil.NewPseudoRand()
	keys := make([]roachpb.Key, numKeys)
	nvs := make([]int, numKeys)
	for t := 1; t <= numVersions; t++ {
		walltime := int64(5 * t)
		ts := makeTS(walltime, 0)
		batch := rocksdb.NewBatch()
		for i := 0; i < numKeys; i++ {
			if t == 1 {
				keys[i] = roachpb.Key(encoding.EncodeUvarint([]byte("key-"), uint64(i)))
				nvs[i] = rand.Intn(numVersions) + 1
			}
			// Only write values if this iteration is less than the random
			// number of versions chosen for this key.
			if t <= nvs[i] {
				value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueBytes))
				value.InitChecksum(keys[i])
				if err := MVCCPut(batch, nil, keys[i], ts, value, nil); err != nil {
					b.Fatal(err)
				}
			}
		}
		if err := batch.Commit(); err != nil {
			b.Fatal(err)
		}
		batch.Close()
	}
	rocksdb.CompactRange(nil, nil)

	return rocksdb, stopper
}

// prewarmCache prewarms the rocksdb cache by iterating over the
// entire database.
func prewarmCache(rocksdb *RocksDB) {
	iter := rocksdb.NewIterator()
	iter.Seek(keyMin)
	defer iter.Close()

	for iter.Valid() {
		iter.Next()
	}
}

// runMVCCScan first creates test data (and resets the benchmarking
// timer). It then performs b.N MVCCScans in increments of numRows
// keys over all of the data in the rocksdb instance, restarting at
// the beginning of the keyspace, as many times as necessary.
func runMVCCScan(numRows, numVersions int, b *testing.B) {
	// Use the same number of keys for all of the mvcc scan
	// benchmarks. Using a different number of keys per test gives
	// preferential treatment to tests with fewer keys. Note that the
	// datasets all fit in cache and the cache is pre-warmed.
	const numKeys = 100000

	rocksdb, stopper := setupMVCCScanData(numVersions, numKeys, 1024, b)
	defer stopper.Stop()

	prewarmCache(rocksdb)

	b.SetBytes(int64(numRows * 1024))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		keyBuf := append(make([]byte, 0, 64), []byte("key-")...)
		for pb.Next() {
			// Choose a random key to start scan.
			keyIdx := rand.Int31n(int32(numKeys - numRows))
			startKey := roachpb.Key(encoding.EncodeUvarint(keyBuf[0:4], uint64(keyIdx)))
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
	})

	b.StopTimer()
}

// The 1 version tests generate a rocksdb database that is 105M.
func BenchmarkMVCCScan1Version1Row(b *testing.B) {
	runMVCCScan(1, 1, b)
}

func BenchmarkMVCCScan1Version10Rows(b *testing.B) {
	runMVCCScan(10, 1, b)
}

func BenchmarkMVCCScan1Version100Rows(b *testing.B) {
	runMVCCScan(100, 1, b)
}

func BenchmarkMVCCScan1Version1000Rows(b *testing.B) {
	runMVCCScan(1000, 1, b)
}

// The 10 version tests generate a rocksdb database that is 564M.
func BenchmarkMVCCScan10Versions1Row(b *testing.B) {
	runMVCCScan(1, 10, b)
}

func BenchmarkMVCCScan10Versions10Rows(b *testing.B) {
	runMVCCScan(10, 10, b)
}

func BenchmarkMVCCScan10Versions100Rows(b *testing.B) {
	runMVCCScan(100, 10, b)
}

func BenchmarkMVCCScan10Versions1000Rows(b *testing.B) {
	runMVCCScan(1000, 10, b)
}

// The 100 version tests generate a rocksdb database that is ~5G.
func BenchmarkMVCCScan100Versions1Row(b *testing.B) {
	runMVCCScan(1, 100, b)
}

func BenchmarkMVCCScan100Versions10Rows(b *testing.B) {
	runMVCCScan(10, 100, b)
}

func BenchmarkMVCCScan100Versions100Rows(b *testing.B) {
	runMVCCScan(100, 100, b)
}

func BenchmarkMVCCScan100Versions1000Rows(b *testing.B) {
	runMVCCScan(1000, 100, b)
}

// runMVCCGet first creates test data (and resets the benchmarking
// timer). It then performs b.N MVCCGets.
func runMVCCGet(numVersions int, b *testing.B) {
	// Use the same number of keys for all of the mvcc get
	// benchmarks. Using a different number of keys per test gives
	// preferential treatment to tests with fewer keys. Note that the
	// datasets all fit in cache and the cache is pre-warmed.
	const numKeys = 100000

	rocksdb, stopper := setupMVCCScanData(numVersions, numKeys, 1024, b)
	defer stopper.Stop()

	prewarmCache(rocksdb)

	b.SetBytes(1024)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		keyBuf := append(make([]byte, 0, 64), []byte("key-")...)
		for pb.Next() {
			// Choose a random key to retrieve.
			keyIdx := rand.Int31n(int32(numKeys))
			key := roachpb.Key(encoding.EncodeUvarint(keyBuf[0:4], uint64(keyIdx)))
			walltime := int64(5 * (rand.Int31n(int32(numVersions)) + 1))
			ts := makeTS(walltime, 0)
			if v, _, err := MVCCGet(rocksdb, key, ts, true, nil); err != nil {
				b.Fatalf("failed get: %s", err)
			} else if valueBytes, err := v.GetBytes(); err != nil {
				b.Fatal(err)
			} else if len(valueBytes) != 1024 {
				b.Fatalf("unexpected value size: %d", len(valueBytes))
			}
		}
	})

	b.StopTimer()
}

func BenchmarkMVCCGet1Version(b *testing.B) {
	runMVCCGet(1, b)
}

func BenchmarkMVCCGet10Versions(b *testing.B) {
	runMVCCGet(10, b)
}

func BenchmarkMVCCGet100Versions(b *testing.B) {
	runMVCCGet(100, b)
}

func runMVCCPut(valueSize int, b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	stopper := stop.NewStopper()
	defer stopper.Stop()
	rocksdb := NewInMem(roachpb.Attributes{Attrs: []string{"ssd"}}, testCacheSize, stopper)

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := roachpb.Key(encoding.EncodeUvarint(keyBuf[0:4], uint64(i)))
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

func runMVCCBatchPut(valueSize, batchSize int, b *testing.B) {
	rng, _ := randutil.NewPseudoRand()
	value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, valueSize))
	keyBuf := append(make([]byte, 0, 64), []byte("key-")...)

	stopper := stop.NewStopper()
	defer stopper.Stop()
	rocksdb := NewInMem(roachpb.Attributes{Attrs: []string{"ssd"}}, testCacheSize, stopper)

	b.SetBytes(int64(valueSize))
	b.ResetTimer()

	for i := 0; i < b.N; i += batchSize {
		end := i + batchSize
		if end > b.N {
			end = b.N
		}

		batch := rocksdb.NewBatch()

		for j := i; j < end; j++ {
			key := roachpb.Key(encoding.EncodeUvarint(keyBuf[0:4], uint64(j)))
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
	rocksdb := NewInMem(roachpb.Attributes{Attrs: []string{"ssd"}}, testCacheSize, stopper)

	// Precompute keys so we don't waste time formatting them at each iteration.
	keys := make([]roachpb.Key, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = roachpb.Key(fmt.Sprintf("key-%d", i))
	}

	b.ResetTimer()

	// Use parallelism if specified when test is run.
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ms := MVCCStats{}
			if err := MVCCMerge(rocksdb, &ms, keys[rand.Intn(numKeys)], *value); err != nil {
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
		Samples: []*roachpb.InternalTimeSeriesSample{
			{Offset: 0, Count: 1, Sum: 5.0},
		},
	}
	var value roachpb.Value
	if err := value.SetProto(ts); err != nil {
		b.Fatal(err)
	}
	runMVCCMerge(&value, 1024, b)
}

// runMVCCComputeStats benchmarks computing MVCC stats on a 64MB range of data.
func runMVCCComputeStats(valueBytes int, b *testing.B) {
	const rangeBytes = 64 * 1024 * 1024
	const overhead = 48 // Per key/value overhead (empirically determined)
	numKeys := rangeBytes / (overhead + valueBytes)
	rocksdb, stopper := setupMVCCScanData(1, numKeys, valueBytes, b)
	defer stopper.Stop()

	prewarmCache(rocksdb)

	b.SetBytes(rangeBytes)
	b.ResetTimer()

	var stats MVCCStats
	for i := 0; i < b.N; i++ {
		iter := rocksdb.NewIterator()
		iter.Seek(keyMin)
		var err error
		stats, err = MVCCComputeStats(iter, 0)
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
