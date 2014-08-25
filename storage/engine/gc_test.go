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
	"bytes"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/encoding"
)

var (
	aKey  = encoding.EncodeBinary(nil, Key("a"))
	bKey  = encoding.EncodeBinary(nil, Key("b"))
	cKey  = encoding.EncodeBinary(nil, Key("c"))
	aKeys = []Key{
		aKey,
		mvccEncodeKey(aKey, makeTS(2E9, 0)),
		mvccEncodeKey(aKey, makeTS(1E9, 1)),
		mvccEncodeKey(aKey, makeTS(1E9, 0)),
	}
	bKeys = []Key{
		bKey,
		mvccEncodeKey(bKey, makeTS(2E9, 0)),
		mvccEncodeKey(bKey, makeTS(1E9, 0)),
	}
	cKeys = []Key{
		mvccEncodeKey(cKey, makeTS(1E9, 0)),
	}
	keys = append(aKeys, append(bKeys, cKeys...)...)
)

// TestGarbageCollectorMVCCPrefix verifies that MVCC variants of same
// key are grouped together and non-MVCC keys are considered singly.
func TestGarbageCollectorMVCCPrefix(t *testing.T) {
	expPrefixes := []Key{
		aKey,
		aKey,
		aKey,
		aKey,
		bKey,
		bKey,
		bKey,
		cKey,
	}
	gc := NewGarbageCollector(makeTS(0, 0), nil)
	prefixes := []Key{}
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
	gc := NewGarbageCollector(makeTS(0, 0), func(key Key) *proto.GCPolicy {
		var seconds int32
		if bytes.Compare(key, Key("b")) < 0 {
			seconds = 1
		} else if bytes.Compare(key, Key("c")) < 0 {
			seconds = 2
		} else {
			seconds = 0
		}
		return &proto.GCPolicy{
			TTLSeconds: seconds,
		}
	})
	e := []byte{}
	n := []byte{valueNormalPrefix}
	d := []byte{valueDeletedPrefix}
	testData := []struct {
		time      proto.Timestamp
		keys      []Key
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
