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
	"bytes"
	"encoding/gob"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"code.google.com/p/go-uuid/uuid"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/log"
	gogoproto "github.com/gogo/protobuf/proto"
)

const testCacheSize = 1 << 30 // GB.

// encodePutResponse creates a put response using the specified
// timestamp and encodes it using gogoprotobuf.
func encodePutResponse(timestamp proto.Timestamp, t *testing.T) []byte {
	rwCmd := &proto.ReadWriteCmdResponse{
		Put: &proto.PutResponse{
			ResponseHeader: proto.ResponseHeader{
				Timestamp: timestamp,
			},
		},
	}
	data, err := gogoproto.Marshal(rwCmd)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// encodeTransaction creates a transaction using the specified
// timestamp and encodes it using gogoprotobuf.
func encodeTransaction(timestamp proto.Timestamp, t *testing.T) []byte {
	txn := &proto.Transaction{
		Timestamp: timestamp,
	}
	data, err := gogoproto.Marshal(txn)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestRocksDBCompaction verifies that a garbage collector can be
// installed on a RocksDB engine and will properly compact response
// cache and transaction entries.
func TestRocksDBCompaction(t *testing.T) {
	gob.Register(proto.Timestamp{})
	loc := util.CreateTempDirectory()
	rocksdb := NewRocksDB(proto.Attributes{Attrs: []string{"ssd"}}, loc, testCacheSize)
	err := rocksdb.Start()
	if err != nil {
		t.Fatalf("could not create new rocksdb db instance at %s: %v", loc, err)
	}
	rocksdb.SetGCTimeouts(1, 2)
	defer func(t *testing.T) {
		rocksdb.Stop()
		if err := rocksdb.Destroy(); err != nil {
			t.Errorf("could not delete rocksdb db at %s: %v", loc, err)
		}
	}(t)

	cmdID := &proto.ClientCmdID{WallTime: 1, Random: 1}

	// Write two transaction values and two response cache values such
	// that exactly one of each should be GC'd based on our GC timeouts.
	kvs := []proto.KeyValue{
		{
			Key:   ResponseCacheKey(1, cmdID),
			Value: proto.Value{Bytes: encodePutResponse(makeTS(2, 0), t)},
		},
		{
			Key:   ResponseCacheKey(2, cmdID),
			Value: proto.Value{Bytes: encodePutResponse(makeTS(3, 0), t)},
		},
		{
			Key:   TransactionKey(proto.Key("a"), proto.Key(uuid.New())),
			Value: proto.Value{Bytes: encodeTransaction(makeTS(1, 0), t)},
		},
		{
			Key:   TransactionKey(proto.Key("b"), proto.Key(uuid.New())),
			Value: proto.Value{Bytes: encodeTransaction(makeTS(2, 0), t)},
		},
	}
	for _, kv := range kvs {
		if err := MVCCPut(rocksdb, nil, kv.Key, proto.ZeroTimestamp, kv.Value, nil); err != nil {
			t.Fatal(err)
		}
	}

	// Compact range and scan remaining values to compare.
	rocksdb.CompactRange(nil, nil)
	actualKVs, err := MVCCScan(rocksdb, KeyMin, KeyMax, 0, proto.ZeroTimestamp, nil)
	if err != nil {
		t.Fatalf("could not run scan: %v", err)
	}
	var keys []proto.Key
	for _, kv := range actualKVs {
		keys = append(keys, kv.Key)
	}
	expKeys := []proto.Key{
		kvs[1].Key,
		kvs[3].Key,
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
// Returns a map from walltime (in 5ns increments) to slices of byte
// values for each key.
func setupMVCCData(rocksdb *RocksDB, numVersions, numKeys int, b *testing.B) map[int64][][]byte {
	rng := util.NewPseudoRand()
	keys := make([]proto.Key, numKeys)
	nvs := make([]int, numKeys)
	results := map[int64][][]byte{}
	for t := 1; t <= numVersions; t++ {
		walltime := int64(5 * t)
		ts := makeTS(walltime, 0)
		batch := rocksdb.NewBatch()
		for i := 0; i < numKeys; i++ {
			if t == 1 {
				keys[i] = proto.Key(encoding.EncodeInt([]byte("key-"), int64(i)))
				nvs[i] = int(rand.Int31n(int32(numVersions)) + 1)
			}
			// Only write values if this iteration is less than the random
			// number of versions chosen for this key.
			if t <= nvs[i] {
				value := proto.Value{Bytes: []byte(util.RandString(rng, 1024))}
				if err := MVCCPut(batch, nil, keys[i], ts, value, nil); err != nil {
					b.Fatal(err)
				}
				results[walltime] = append(results[walltime], value.Bytes)
			} else {
				// If we're not creating a new key, just copy the previous one.
				results[walltime] = append(results[walltime], results[walltime-5][i])
			}
		}
		if err := batch.Commit(); err != nil {
			b.Fatal(err)
		}
	}
	rocksdb.CompactRange(nil, nil)
	return results
}

// runMVCCScan first creates test data (and resets benchmarking
// timer). It then performs b.N MVCCScans in increments of
// scanIncrement keys over all of the data in the rocksdb instance,
// restarting at the beginning of the keyspace, as many times as
// necessary.
func runMVCCScan(numVersions, numKeys int, b *testing.B) {
	loc := util.CreateTempDirectory()
	rocksdb := NewRocksDB(proto.Attributes{Attrs: []string{"ssd"}}, loc, testCacheSize)
	if err := rocksdb.Start(); err != nil {
		b.Fatalf("could not create new rocksdb db instance at %s: %v", loc, err)
	}
	defer func(b *testing.B) {
		rocksdb.Stop()
		if err := rocksdb.Destroy(); err != nil {
			b.Errorf("could not delete rocksdb db at %s: %v", loc, err)
		}
	}(b)

	results := setupMVCCData(rocksdb, numVersions, numKeys, b)
	log.Infof("starting scan benchmark...")
	b.ResetTimer()

	const maxRows = 1024
	for i := 0; i < b.N; i++ {
		// Choose a random key to start scan.
		keyIdx := rand.Int31n(int32(numKeys - maxRows))
		startKey := proto.Key(encoding.EncodeInt([]byte("key-"), int64(keyIdx)))
		walltime := int64(5 * (rand.Int31n(int32(numVersions)) + 1))
		ts := makeTS(walltime, 0)
		kvs, err := MVCCScan(rocksdb, startKey, KeyMax, maxRows, ts, nil)
		if err != nil {
			b.Fatalf("failed scan: %s", err)
		}
		for _, kv := range kvs {
			if !bytes.Equal(kv.Value.Bytes, results[walltime][keyIdx]) {
				b.Errorf("at time=%dns, value for key-%d doesn't match", walltime, keyIdx)
			}
			keyIdx++
		}
	}
}

func BenchmarkMVCCScan10Versions(b *testing.B) {
	const numVersions = 10
	const numKeys = 50 * 1024 // This amounts to ~250MB of data
	runMVCCScan(numVersions, numKeys, b)
}

func BenchmarkMVCCScan1Version(b *testing.B) {
	const numVersions = 1
	const numKeys = 250 * 1024 // This amounts to ~250MB of data
	runMVCCScan(numVersions, numKeys, b)
}

// runMVCCMerge merges value numMerges times into numKeys separate keys.
func runMVCCMerge(value *proto.Value, numMerges, numKeys int, b *testing.B) {
	loc := util.CreateTempDirectory()
	rocksdb := NewRocksDB(proto.Attributes{Attrs: []string{"ssd"}}, loc, testCacheSize)
	if err := rocksdb.Start(); err != nil {
		b.Fatalf("could not create new rocksdb db instance at %s: %v", loc, err)
	}
	defer func(b *testing.B) {
		rocksdb.Stop()
		if err := rocksdb.Destroy(); err != nil {
			b.Errorf("could not delete rocksdb db at %s: %v", loc, err)
		}
	}(b)

	// Precompute keys so we don't waste time formatting them at each iteration.
	keys := make([]proto.Key, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = proto.Key(fmt.Sprintf("key-%d", i))
	}
	// Use parallelism if specified when test is run.
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ms := MVCCStats{}
			for i := 0; i < numMerges; i++ {
				if err := MVCCMerge(rocksdb, &ms, keys[rand.Intn(numKeys)], *value); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	// Read values out to force merge.
	for _, key := range keys {
		val, err := MVCCGet(rocksdb, key, proto.ZeroTimestamp, nil)
		if err != nil {
			b.Fatal(err)
		} else if val == nil {
			continue
		}
		if val.Integer != nil {
			fmt.Printf("%q: %d\n", key, val.GetInteger())
		} else {
			fmt.Printf("%q: [%d]byte\n", key, len(val.Bytes))
		}
	}
}

// BenchmarkMVCCMergeInteger computes performance of merging integers.
func BenchmarkMVCCMergeInteger(b *testing.B) {
	runMVCCMerge(&proto.Value{Integer: gogoproto.Int64(1)}, 1024, 1024, b)
}

// BenchmarkMVCCMergeTimeSeries computes performance of merging time series data.
func BenchmarkMVCCMergeTimeSeries(b *testing.B) {
	ts := &proto.InternalTimeSeriesData{
		StartTimestampNanos: 0,
		SampleDurationNanos: 1000,
		Samples: []*proto.InternalTimeSeriesSample{
			{Offset: 0, IntCount: 1, IntSum: gogoproto.Int64(5)},
		},
	}
	value, err := ts.ToValue()
	if err != nil {
		b.Fatal(err)
	}
	runMVCCMerge(value, 1024, 1024, b)
}
