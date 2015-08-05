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
	"testing"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/leaktest"
	gogoproto "github.com/gogo/protobuf/proto"
)

var (
	aKey  = proto.Key("a")
	bKey  = proto.Key("b")
	aKeys = []proto.EncodedKey{
		MVCCEncodeVersionKey(aKey, makeTS(2E9, 0)),
		MVCCEncodeVersionKey(aKey, makeTS(1E9, 1)),
		MVCCEncodeVersionKey(aKey, makeTS(1E9, 0)),
	}
	bKeys = []proto.EncodedKey{
		MVCCEncodeVersionKey(bKey, makeTS(2E9, 0)),
		MVCCEncodeVersionKey(bKey, makeTS(1E9, 0)),
	}
)

func serializedMVCCValue(deleted bool, t *testing.T) []byte {
	data, err := gogoproto.Marshal(&MVCCValue{Deleted: deleted})
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}
	return data
}

// TestGarbageCollectorFilter verifies the filter policies for
// different sorts of MVCC keys.
func TestGarbageCollectorFilter(t *testing.T) {
	defer leaktest.AfterTest(t)
	gcA := NewGarbageCollector(makeTS(0, 0), config.GCPolicy{TTLSeconds: 1})
	gcB := NewGarbageCollector(makeTS(0, 0), config.GCPolicy{TTLSeconds: 2})
	n := serializedMVCCValue(false, t)
	d := serializedMVCCValue(true, t)
	testData := []struct {
		gc       *GarbageCollector
		time     proto.Timestamp
		keys     []proto.EncodedKey
		values   [][]byte
		expDelTS proto.Timestamp
	}{
		{gcA, makeTS(0, 0), aKeys, [][]byte{n, n, n}, proto.ZeroTimestamp},
		{gcA, makeTS(0, 0), aKeys, [][]byte{d, d, d}, makeTS(2E9, 0)},
		{gcB, makeTS(0, 0), bKeys, [][]byte{n, n}, proto.ZeroTimestamp},
		{gcB, makeTS(0, 0), bKeys, [][]byte{d, d}, makeTS(2E9, 0)},
		{gcA, makeTS(1E9, 0), aKeys, [][]byte{n, n, n}, proto.ZeroTimestamp},
		{gcB, makeTS(1E9, 0), bKeys, [][]byte{n, n}, proto.ZeroTimestamp},
		{gcA, makeTS(2E9, 0), aKeys, [][]byte{n, n, n}, proto.ZeroTimestamp},
		{gcB, makeTS(2E9, 0), bKeys, [][]byte{n, n}, proto.ZeroTimestamp},
		{gcA, makeTS(3E9, 0), aKeys, [][]byte{n, n, n}, makeTS(1E9, 1)},
		{gcA, makeTS(3E9, 0), aKeys, [][]byte{d, n, n}, makeTS(2E9, 0)},
		{gcB, makeTS(3E9, 0), bKeys, [][]byte{n, n}, proto.ZeroTimestamp},
		{gcA, makeTS(4E9, 0), aKeys, [][]byte{n, n, n}, makeTS(1E9, 1)},
		{gcB, makeTS(4E9, 0), bKeys, [][]byte{n, n}, makeTS(1E9, 0)},
		{gcB, makeTS(4E9, 0), bKeys, [][]byte{d, n}, makeTS(2E9, 0)},
		{gcA, makeTS(5E9, 0), aKeys, [][]byte{n, n, n}, makeTS(1E9, 1)},
		{gcB, makeTS(5E9, 0), bKeys, [][]byte{n, n}, makeTS(1E9, 0)},
		{gcB, makeTS(5E9, 0), bKeys, [][]byte{d, n}, makeTS(2E9, 0)},
	}
	for i, test := range testData {
		test.gc.expiration = test.time
		test.gc.expiration.WallTime -= int64(test.gc.policy.TTLSeconds) * 1E9
		delTS := test.gc.Filter(test.keys, test.values)
		if !delTS.Equal(test.expDelTS) {
			t.Errorf("%d: expected deletion timestamp %s; got %s", i, test.expDelTS, delTS)
		}
	}
}
