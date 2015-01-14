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
	gc := NewGarbageCollector(makeTS(0, 0), func(key proto.Key) *proto.GCPolicy {
		var seconds int32
		if bytes.Compare(key, proto.Key("b")) < 0 {
			seconds = 1
		} else if bytes.Compare(key, proto.Key("c")) < 0 {
			seconds = 2
		} else {
			seconds = 0
		}
		return &proto.GCPolicy{
			TTLSeconds: seconds,
		}
	})
	e := []byte{}
	n := serializedMVCCValue(false, t)
	d := serializedMVCCValue(true, t)
	testData := []struct {
		time      proto.Timestamp
		keys      []proto.EncodedKey
		values    [][]byte
		expDelete []bool
	}{
		{makeTS(0, 0), aKeys, [][]byte{e, n, n, n}, []bool{false, false, false, false}},
		{makeTS(0, 0), aKeys, [][]byte{e, d, d, d}, []bool{true, true, true, true}},
		{makeTS(0, 0), bKeys, [][]byte{e, n, n}, []bool{false, false, false}},
		{makeTS(0, 0), bKeys, [][]byte{e, d, d}, []bool{true, true, true}},
		{makeTS(0, 0), cKeys, [][]byte{n}, nil},
		{makeTS(1E9, 0), aKeys, [][]byte{e, n, n, n}, []bool{false, false, false, false}},
		{makeTS(1E9, 0), bKeys, [][]byte{e, n, n}, []bool{false, false, false}},
		{makeTS(1E9, 0), cKeys, [][]byte{n}, nil},
		{makeTS(2E9, 0), aKeys, [][]byte{e, n, n, n}, []bool{false, false, false, false}},
		{makeTS(2E9, 0), bKeys, [][]byte{e, n, n}, []bool{false, false, false}},
		{makeTS(2E9, 0), cKeys, [][]byte{n}, nil},
		{makeTS(3E9, 0), aKeys, [][]byte{e, n, n, n}, []bool{false, false, true, true}},
		{makeTS(3E9, 0), aKeys, [][]byte{e, d, n, n}, []bool{true, true, true, true}},
		{makeTS(3E9, 0), bKeys, [][]byte{e, n, n}, []bool{false, false, false}},
		{makeTS(3E9, 0), cKeys, [][]byte{n}, nil},
		{makeTS(4E9, 0), aKeys, [][]byte{e, n, n, n}, []bool{false, false, true, true}},
		{makeTS(4E9, 0), bKeys, [][]byte{e, n, n}, []bool{false, false, true}},
		{makeTS(4E9, 0), bKeys, [][]byte{e, d, n}, []bool{true, true, true}},
		{makeTS(4E9, 0), cKeys, [][]byte{n}, nil},
		{makeTS(5E9, 0), aKeys, [][]byte{e, n, n, n}, []bool{false, false, true, true}},
		{makeTS(5E9, 0), bKeys, [][]byte{e, n, n}, []bool{false, false, true}},
		{makeTS(5E9, 0), bKeys, [][]byte{e, d, n}, []bool{true, true, true}},
		{makeTS(5E9, 0), cKeys, [][]byte{n}, nil},
	}
	for i, test := range testData {
		gc.now = test.time
		toDelete := gc.Filter(test.keys, test.values)
		if !reflect.DeepEqual(toDelete, test.expDelete) {
			t.Errorf("expected deletions (test %d): %v; got %v", i, test.expDelete, toDelete)
		}
	}
}
