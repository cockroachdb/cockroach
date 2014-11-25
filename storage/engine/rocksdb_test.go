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
	"encoding/gob"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
)

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
// installed on a RocksDB engine and will properly compact entries
// response cache and transaction entries.
func TestRocksDBCompaction(t *testing.T) {
	gob.Register(proto.Timestamp{})
	loc := util.CreateTempDirectory()
	rocksdb := NewRocksDB(proto.Attributes{Attrs: []string{"ssd"}}, loc)
	rocksdb.SetGCTimeouts(func() (minTxnTS, minRCacheTS int64) {
		minTxnTS = 1
		minRCacheTS = 2
		return
	})
	err := rocksdb.Start()
	if err != nil {
		t.Fatalf("could not create new rocksdb db instance at %s: %v", loc, err)
	}
	defer func(t *testing.T) {
		rocksdb.Stop()
		if err := rocksdb.Destroy(); err != nil {
			t.Errorf("could not delete rocksdb db at %s: %v", loc, err)
		}
	}(t)

	rcPre := KeyLocalResponseCachePrefix
	txnPre := KeyLocalTransactionPrefix

	// Write two transaction values and two response cache values such
	// that exactly one of each should be GC'd based on our GC timeouts.
	kvs := []proto.KeyValue{
		proto.KeyValue{
			Key:   MakeLocalKey(rcPre, proto.Key("a")),
			Value: proto.Value{Bytes: encodePutResponse(makeTS(2, 0), t)},
		},
		proto.KeyValue{
			Key:   MakeLocalKey(rcPre, proto.Key("b")),
			Value: proto.Value{Bytes: encodePutResponse(makeTS(3, 0), t)},
		},
		proto.KeyValue{
			Key:   MakeLocalKey(txnPre, proto.Key("a")),
			Value: proto.Value{Bytes: encodeTransaction(makeTS(1, 0), t)},
		},
		proto.KeyValue{
			Key:   MakeLocalKey(txnPre, proto.Key("b")),
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
		MakeLocalKey(rcPre, proto.Key("b")),
		MakeLocalKey(txnPre, proto.Key("b")),
	}
	if !reflect.DeepEqual(expKeys, keys) {
		t.Errorf("expected keys %+v, got keys %+v", expKeys, keys)
	}
}

// runMVCCMerge merges value numMerges times into numKeys separate keys.
func runMVCCMerge(value *proto.Value, numMerges, numKeys int, b *testing.B) {
	loc := util.CreateTempDirectory()
	rocksdb := NewRocksDB(proto.Attributes{Attrs: []string{"ssd"}}, loc)
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
	ts := &proto.TimeSeriesData{
		StartTimestamp:    0,
		DurationInSeconds: 3600,
		SamplePrecision:   proto.SECONDS,
		Data: []*proto.TimeSeriesDataPoint{
			{Offset: 0, ValueInt: gogoproto.Int64(5)},
		},
	}
	value, err := ts.ToValue()
	if err != nil {
		b.Fatal(err)
	}
	runMVCCMerge(value, 1024, 1024, b)
}
