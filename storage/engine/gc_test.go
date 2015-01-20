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
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	gogoproto "github.com/gogo/protobuf/proto"
)

var (
	aKey  = proto.Key("a")
	bKey  = proto.Key("b")
	cKey  = proto.Key("c")
	aKeys = []proto.EncodedKey{
		MVCCEncodeKey(aKey),
		MVCCEncodeVersionKey(aKey, makeTS(2E9, 0)),
		MVCCEncodeVersionKey(aKey, makeTS(1E9, 1)),
		MVCCEncodeVersionKey(aKey, makeTS(1E9, 0)),
	}
	bKeys = []proto.EncodedKey{
		MVCCEncodeKey(bKey),
		MVCCEncodeVersionKey(bKey, makeTS(2E9, 0)),
		MVCCEncodeVersionKey(bKey, makeTS(1E9, 0)),
	}
	cKeys = []proto.EncodedKey{
		MVCCEncodeKey(cKey),
	}
	keys = append(aKeys, append(bKeys, cKeys...)...)
)

func serializedMVCCValue(deleted bool, t *testing.T) []byte {
	data, err := gogoproto.Marshal(&proto.MVCCValue{Deleted: deleted})
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}
	return data
}

// TestGarbageCollectorMVCCPrefix verifies that MVCC variants of same
// key are grouped together and non-MVCC keys are considered singly.
func TestGarbageCollectorMVCCPrefix(t *testing.T) {
	expPrefixes := []proto.EncodedKey{
		MVCCEncodeKey(aKey),
		MVCCEncodeKey(aKey),
		MVCCEncodeKey(aKey),
		MVCCEncodeKey(aKey),
		MVCCEncodeKey(bKey),
		MVCCEncodeKey(bKey),
		MVCCEncodeKey(bKey),
		MVCCEncodeKey(cKey),
	}
	gc := NewGarbageCollector(makeTS(0, 0), nil)
	prefixes := []proto.EncodedKey{}
	for _, key := range keys {
		prefixes = append(prefixes, key[:gc.MVCCPrefix(key)])
	}
	if !reflect.DeepEqual(expPrefixes, prefixes) {
		t.Errorf("prefixes %s doesn't equal expected prefixes %s", prefixes, expPrefixes)
	}
}

// TestGarbageCollectorFilter verifies the filter policies for
// different sorts of MVCC keys.
func TestGarbageCollectorFilter(t *testing.T) {
	gcA := NewGarbageCollector(makeTS(0, 0), &proto.GCPolicy{TTLSeconds: 1})
	gcB := NewGarbageCollector(makeTS(0, 0), &proto.GCPolicy{TTLSeconds: 2})
	gcC := NewGarbageCollector(makeTS(0, 0), &proto.GCPolicy{TTLSeconds: 0})
	e := []byte{}
	n := serializedMVCCValue(false, t)
	d := serializedMVCCValue(true, t)
	testData := []struct {
		gc       *GarbageCollector
		time     proto.Timestamp
		keys     []proto.EncodedKey
		values   [][]byte
		expDelTS proto.Timestamp
	}{
		{gcA, makeTS(0, 0), aKeys, [][]byte{e, n, n, n}, proto.ZeroTimestamp},
		{gcA, makeTS(0, 0), aKeys, [][]byte{e, d, d, d}, makeTS(2E9, 0)},
		{gcB, makeTS(0, 0), bKeys, [][]byte{e, n, n}, proto.ZeroTimestamp},
		{gcB, makeTS(0, 0), bKeys, [][]byte{e, d, d}, makeTS(2E9, 0)},
		{gcC, makeTS(0, 0), cKeys, [][]byte{n}, proto.ZeroTimestamp},
		{gcA, makeTS(1E9, 0), aKeys, [][]byte{e, n, n, n}, proto.ZeroTimestamp},
		{gcB, makeTS(1E9, 0), bKeys, [][]byte{e, n, n}, proto.ZeroTimestamp},
		{gcC, makeTS(1E9, 0), cKeys, [][]byte{n}, proto.ZeroTimestamp},
		{gcA, makeTS(2E9, 0), aKeys, [][]byte{e, n, n, n}, proto.ZeroTimestamp},
		{gcB, makeTS(2E9, 0), bKeys, [][]byte{e, n, n}, proto.ZeroTimestamp},
		{gcC, makeTS(2E9, 0), cKeys, [][]byte{n}, proto.ZeroTimestamp},
		{gcA, makeTS(3E9, 0), aKeys, [][]byte{e, n, n, n}, makeTS(1E9, 1)},
		{gcA, makeTS(3E9, 0), aKeys, [][]byte{e, d, n, n}, makeTS(2E9, 0)},
		{gcB, makeTS(3E9, 0), bKeys, [][]byte{e, n, n}, proto.ZeroTimestamp},
		{gcC, makeTS(3E9, 0), cKeys, [][]byte{n}, proto.ZeroTimestamp},
		{gcA, makeTS(4E9, 0), aKeys, [][]byte{e, n, n, n}, makeTS(1E9, 1)},
		{gcB, makeTS(4E9, 0), bKeys, [][]byte{e, n, n}, makeTS(1E9, 0)},
		{gcB, makeTS(4E9, 0), bKeys, [][]byte{e, d, n}, makeTS(2E9, 0)},
		{gcC, makeTS(4E9, 0), cKeys, [][]byte{n}, proto.ZeroTimestamp},
		{gcA, makeTS(5E9, 0), aKeys, [][]byte{e, n, n, n}, makeTS(1E9, 1)},
		{gcB, makeTS(5E9, 0), bKeys, [][]byte{e, n, n}, makeTS(1E9, 0)},
		{gcB, makeTS(5E9, 0), bKeys, [][]byte{e, d, n}, makeTS(2E9, 0)},
		{gcC, makeTS(5E9, 0), cKeys, [][]byte{n}, proto.ZeroTimestamp},
	}
	for i, test := range testData {
		test.gc.now = test.time
		delTS := test.gc.Filter(test.keys, test.values)
		if !delTS.Equal(test.expDelTS) {
			t.Errorf("expected deletion timestamp %s; got %v", i, test.expDelTS, delTS)
		}
	}
}
