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
// Author: Kevin GuanJian (keynovo@gmail.com)

package storage

import (
	"bytes"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/coreos/etcd/raft/raftpb"
)

// TestRaftEntryCacheGet
func TestRaftEntryCacheGet(t *testing.T) {
	defer leaktest.AfterTest(t)

	testEntries := []raftpb.Entry{
		{Type: raftpb.EntryNormal, Term: 1, Index: 11, Data: []byte("a")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 12, Data: []byte("b")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 13, Data: []byte("c")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 14, Data: []byte("d")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 15, Data: []byte("e")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 16, Data: []byte("f")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 17, Data: []byte("g")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 18, Data: []byte("h")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 19, Data: []byte("i")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 20, Data: []byte("j")},
	}

	cache := newRaftEntryCache(10)
	cache.addEntry(testEntries)

	testCases := []struct {
		lo        uint64
		hi        uint64
		maxBytes  uint64
		cacheLen  int
		cacheData []byte
	}{
		// Case 0: get from truncated cache.
		{
			lo:       10,
			hi:       12,
			maxBytes: math.MaxUint64,
			cacheLen: 0,
		},
		// Case 1: get a single entry.
		{
			lo:        11,
			hi:        12,
			maxBytes:  math.MaxUint64,
			cacheLen:  1,
			cacheData: []byte("a"),
		},
		// Case 2: hi value is past the last index, should return all available.
		{
			lo:       11,
			hi:       21,
			maxBytes: math.MaxUint64,
			cacheLen: 10,
		},
		// Case 3: hi value is past the last index, but return part of entries.
		{
			lo:        20,
			hi:        30,
			maxBytes:  0,
			cacheLen:  1,
			cacheData: []byte("j"),
		},
		// Case 4: hi value is past the last index, but return by maxBytes.
		{
			lo:        11,
			hi:        30,
			maxBytes:  1,
			cacheLen:  1,
			cacheData: []byte("a"),
		},
		// Case 5: hi value is past the last index, should return all available
		// when maxBytes set to zero.
		{
			lo:       11,
			hi:       30,
			maxBytes: 0,
			cacheLen: 10,
		},
	}

	for i, tc := range testCases {
		hitEnts, _, _ := cache.getEntry(tc.lo, tc.hi, tc.maxBytes)
		if tc.cacheLen != len(hitEnts) {
			t.Errorf("%d: expected entries lenth %d, got %d", i, tc.cacheLen, len(hitEnts))
			continue
		}
		if len(hitEnts) == 1 {
			if !bytes.Equal(tc.cacheData, hitEnts[0].Data) {
				t.Errorf("%d: expected entry data %v, got %v", i, tc.cacheData, hitEnts[0].Data)
			}
			continue
		}
	}
}

// TestRaftEntryCacheEvict
func TestRaftEntryCacheEvict(t *testing.T) {
	defer leaktest.AfterTest(t)

	testBaseEntries := []raftpb.Entry{
		{Type: raftpb.EntryNormal, Term: 1, Index: 11, Data: []byte("a")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 12, Data: []byte("b")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 13, Data: []byte("c")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 14, Data: []byte("d")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 15, Data: []byte("e")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 16, Data: []byte("f")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 17, Data: []byte("g")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 18, Data: []byte("h")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 19, Data: []byte("i")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 20, Data: []byte("j")},
	}

	testIncEntries := []raftpb.Entry{
		{Type: raftpb.EntryNormal, Term: 1, Index: 21, Data: []byte("k")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 22, Data: []byte("l")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 23, Data: []byte("m")},
	}

	cache := newRaftEntryCache(10)
	cache.addEntry(testBaseEntries)

	hitEnts, _, _ := cache.getEntry(11, 12, math.MaxUint64)
	if !bytes.Equal(testBaseEntries[0].Data, hitEnts[0].Data) {
		t.Errorf("expected entry %v, got %v", testBaseEntries[0].Data, hitEnts[0].Data)
	}

	cache.addEntry(testIncEntries)

	hitEnts, _, _ = cache.getEntry(11, 14, math.MaxUint64)
	if len(hitEnts) != 0 {
		t.Errorf("expected entry length %d, got %d", 0, len(hitEnts))
	}

	hitEnts, _, _ = cache.getEntry(14, 15, math.MaxUint64)
	if !bytes.Equal(testBaseEntries[3].Data, hitEnts[0].Data) {
		t.Errorf("expected entry %v, got %v", testBaseEntries[3].Data, hitEnts[0].Data)
	}
}

// TestRaftEntryCacheDelete
func TestRaftEntryCacheDelete(t *testing.T) {
	defer leaktest.AfterTest(t)

	testEntries := []raftpb.Entry{
		{Type: raftpb.EntryNormal, Term: 1, Index: 11, Data: []byte("a")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 12, Data: []byte("b")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 13, Data: []byte("c")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 14, Data: []byte("d")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 15, Data: []byte("e")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 16, Data: []byte("f")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 17, Data: []byte("g")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 18, Data: []byte("h")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 19, Data: []byte("i")},
		{Type: raftpb.EntryNormal, Term: 1, Index: 20, Data: []byte("j")},
	}

	cache := newRaftEntryCache(10)
	cache.addEntry(testEntries)

	hitEnts, _, _ := cache.getEntry(15, 16, math.MaxUint64)
	if !bytes.Equal(testEntries[4].Data, hitEnts[0].Data) {
		t.Errorf("expected entry %v, got %v", testEntries[0].Data, hitEnts[0].Data)
	}

	cache.delEntry(15, 30)

	hitEnts, _, _ = cache.getEntry(15, 20, math.MaxUint64)
	if len(hitEnts) != 0 {
		t.Errorf("expected entry lenth %d, got %d", 0, len(hitEnts))
	}

	hitEnts, _, _ = cache.getEntry(14, 15, math.MaxUint64)
	if !bytes.Equal(testEntries[3].Data, hitEnts[0].Data) {
		t.Errorf("expected entry %v, got %v", testEntries[3].Data, hitEnts[0].Data)
	}
}
