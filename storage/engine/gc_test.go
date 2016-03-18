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
	"testing"

	"github.com/cockroachdb/cockroach/config"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func mvccVersionKey(key roachpb.Key, ts roachpb.Timestamp) MVCCKey {
	return MVCCKey{Key: key, Timestamp: ts}
}

var (
	aKey  = roachpb.Key("a")
	bKey  = roachpb.Key("b")
	aKeys = []MVCCKey{
		mvccVersionKey(aKey, makeTS(2E9, 0)),
		mvccVersionKey(aKey, makeTS(1E9, 1)),
		mvccVersionKey(aKey, makeTS(1E9, 0)),
	}
	bKeys = []MVCCKey{
		mvccVersionKey(bKey, makeTS(2E9, 0)),
		mvccVersionKey(bKey, makeTS(1E9, 0)),
	}
)

// TestGarbageCollectorFilter verifies the filter policies for
// different sorts of MVCC keys.
func TestGarbageCollectorFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	gcA := MakeGarbageCollector(makeTS(0, 0), config.GCPolicy{TTLSeconds: 1})
	gcB := MakeGarbageCollector(makeTS(0, 0), config.GCPolicy{TTLSeconds: 2})
	n := []byte("data")
	d := []byte(nil)
	testData := []struct {
		gc       GarbageCollector
		time     roachpb.Timestamp
		keys     []MVCCKey
		values   [][]byte
		expDelTS roachpb.Timestamp
	}{
		{gcA, makeTS(0, 0), aKeys, [][]byte{n, n, n}, roachpb.ZeroTimestamp},
		{gcA, makeTS(0, 0), aKeys, [][]byte{d, d, d}, makeTS(2E9, 0)},
		{gcB, makeTS(0, 0), bKeys, [][]byte{n, n}, roachpb.ZeroTimestamp},
		{gcB, makeTS(0, 0), bKeys, [][]byte{d, d}, makeTS(2E9, 0)},
		{gcA, makeTS(1E9, 0), aKeys, [][]byte{n, n, n}, roachpb.ZeroTimestamp},
		{gcB, makeTS(1E9, 0), bKeys, [][]byte{n, n}, roachpb.ZeroTimestamp},
		{gcA, makeTS(2E9, 0), aKeys, [][]byte{n, n, n}, roachpb.ZeroTimestamp},
		{gcB, makeTS(2E9, 0), bKeys, [][]byte{n, n}, roachpb.ZeroTimestamp},
		{gcA, makeTS(3E9, 0), aKeys, [][]byte{n, n, n}, makeTS(1E9, 1)},
		{gcA, makeTS(3E9, 0), aKeys, [][]byte{d, n, n}, makeTS(2E9, 0)},
		{gcB, makeTS(3E9, 0), bKeys, [][]byte{n, n}, roachpb.ZeroTimestamp},
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
