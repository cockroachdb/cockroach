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

package engine

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func mvccVersionKey(key roachpb.Key, ts hlc.Timestamp) MVCCKey {
	return MVCCKey{Key: key, Timestamp: ts}
}

var (
	aKey  = roachpb.Key("a")
	bKey  = roachpb.Key("b")
	aKeys = []MVCCKey{
		mvccVersionKey(aKey, hlc.Timestamp{WallTime: 2E9, Logical: 0}),
		mvccVersionKey(aKey, hlc.Timestamp{WallTime: 1E9, Logical: 1}),
		mvccVersionKey(aKey, hlc.Timestamp{WallTime: 1E9, Logical: 0}),
	}
	bKeys = []MVCCKey{
		mvccVersionKey(bKey, hlc.Timestamp{WallTime: 2E9, Logical: 0}),
		mvccVersionKey(bKey, hlc.Timestamp{WallTime: 1E9, Logical: 0}),
	}
)

// TestGarbageCollectorFilter verifies the filter policies for
// different sorts of MVCC keys.
func TestGarbageCollectorFilter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	gcA := MakeGarbageCollector(hlc.Timestamp{WallTime: 0, Logical: 0}, config.GCPolicy{TTLSeconds: 1})
	gcB := MakeGarbageCollector(hlc.Timestamp{WallTime: 0, Logical: 0}, config.GCPolicy{TTLSeconds: 2})
	n := []byte("data")
	d := []byte(nil)
	testData := []struct {
		gc       GarbageCollector
		time     hlc.Timestamp
		keys     []MVCCKey
		values   [][]byte
		expIdx   int
		expDelTS hlc.Timestamp
	}{
		{gcA, hlc.Timestamp{WallTime: 0, Logical: 0}, aKeys, [][]byte{n, n, n}, -1, hlc.Timestamp{}},
		{gcA, hlc.Timestamp{WallTime: 0, Logical: 0}, aKeys, [][]byte{d, d, d}, -1, hlc.Timestamp{}},
		{gcB, hlc.Timestamp{WallTime: 0, Logical: 0}, bKeys, [][]byte{n, n}, -1, hlc.Timestamp{}},
		{gcB, hlc.Timestamp{WallTime: 0, Logical: 0}, bKeys, [][]byte{d, d}, -1, hlc.Timestamp{}},
		{gcA, hlc.Timestamp{WallTime: 1E9, Logical: 0}, aKeys, [][]byte{n, n, n}, -1, hlc.Timestamp{}},
		{gcB, hlc.Timestamp{WallTime: 1E9, Logical: 0}, bKeys, [][]byte{n, n}, -1, hlc.Timestamp{}},
		{gcA, hlc.Timestamp{WallTime: 2E9, Logical: 0}, aKeys, [][]byte{n, n, n}, -1, hlc.Timestamp{}},
		{gcA, hlc.Timestamp{WallTime: 2E9, Logical: 0}, aKeys, [][]byte{d, d, d}, 2, hlc.Timestamp{WallTime: 1E9, Logical: 0}},
		{gcB, hlc.Timestamp{WallTime: 2E9, Logical: 0}, bKeys, [][]byte{n, n}, -1, hlc.Timestamp{}},
		{gcA, hlc.Timestamp{WallTime: 3E9, Logical: 0}, aKeys, [][]byte{n, n, n}, 1, hlc.Timestamp{WallTime: 1E9, Logical: 1}},
		{gcA, hlc.Timestamp{WallTime: 3E9, Logical: 0}, aKeys, [][]byte{d, n, n}, 0, hlc.Timestamp{WallTime: 2E9, Logical: 0}},
		{gcB, hlc.Timestamp{WallTime: 3E9, Logical: 0}, bKeys, [][]byte{n, n}, -1, hlc.Timestamp{}},
		{gcA, hlc.Timestamp{WallTime: 4E9, Logical: 0}, aKeys, [][]byte{n, n, n}, 1, hlc.Timestamp{WallTime: 1E9, Logical: 1}},
		{gcB, hlc.Timestamp{WallTime: 4E9, Logical: 0}, bKeys, [][]byte{n, n}, 1, hlc.Timestamp{WallTime: 1E9, Logical: 0}},
		{gcB, hlc.Timestamp{WallTime: 4E9, Logical: 0}, bKeys, [][]byte{d, n}, 0, hlc.Timestamp{WallTime: 2E9, Logical: 0}},
		{gcA, hlc.Timestamp{WallTime: 5E9, Logical: 0}, aKeys, [][]byte{n, n, n}, 1, hlc.Timestamp{WallTime: 1E9, Logical: 1}},
		{gcB, hlc.Timestamp{WallTime: 5E9, Logical: 0}, bKeys, [][]byte{n, n}, 1, hlc.Timestamp{WallTime: 1E9, Logical: 0}},
		{gcB, hlc.Timestamp{WallTime: 5E9, Logical: 0}, bKeys, [][]byte{d, n}, 0, hlc.Timestamp{WallTime: 2E9, Logical: 0}},
	}
	for i, test := range testData {
		test.gc.Threshold = test.time
		test.gc.Threshold.WallTime -= int64(test.gc.policy.TTLSeconds) * 1E9
		idx, delTS := test.gc.Filter(test.keys, test.values)
		if idx != test.expIdx {
			t.Errorf("%d: expected index %d; got %d", i, test.expIdx, idx)
		}
		if delTS != test.expDelTS {
			t.Errorf("%d: expected deletion timestamp %s; got %s", i, test.expDelTS, delTS)
		}
	}
}
