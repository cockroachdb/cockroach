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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package engine

import (
	"encoding/gob"
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
		rocksdb.Close()
		if err := rocksdb.Destroy(); err != nil {
			t.Errorf("could not delete rocksdb db at %s: %v", loc, err)
		}
	}(t)

	rcPre := KeyLocalRangeResponseCachePrefix
	txnPre := KeyLocalTransactionPrefix

	// Write a two transaction values and two response cache values such
	// that exactly one of each should be GC'd based on our GC timeouts.
	batch := []interface{}{
		// TODO(spencer): use Transaction and Response protobufs here.
		BatchPut{Key: MakeKey(rcPre, Key("a")), Value: encodePutResponse(makeTS(2, 0), t)},
		BatchPut{Key: MakeKey(rcPre, Key("b")), Value: encodePutResponse(makeTS(3, 0), t)},

		BatchPut{Key: MakeKey(txnPre, Key("a")), Value: encodeTransaction(makeTS(1, 0), t)},
		BatchPut{Key: MakeKey(txnPre, Key("b")), Value: encodeTransaction(makeTS(2, 0), t)},
	}
	if err := rocksdb.WriteBatch(batch); err != nil {
		t.Fatal(err)
	}

	// Compact range and scan remaining values to compare.
	rocksdb.CompactRange(nil, nil)
	keyvals, err := rocksdb.Scan(KeyMin, KeyMax, 0)
	if err != nil {
		t.Fatalf("could not run scan: %v", err)
	}
	var keys []Key
	for _, kv := range keyvals {
		keys = append(keys, kv.Key)
	}
	expKeys := []Key{
		MakeKey(rcPre, Key("b")),
		MakeKey(txnPre, Key("b")),
	}
	if !reflect.DeepEqual(expKeys, keys) {
		t.Errorf("expected keys %s, got keys %s", expKeys, keys)
	}
}
