// Copyright 2016 The Cockroach Authors.
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

package storage

import (
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/coreos/etcd/raft/raftpb"
)

func newEntry(index, size uint64) raftpb.Entry {
	return raftpb.Entry{
		Index: index,
		Data:  make([]byte, size, size),
	}
}

func TestEntryCacheClearTo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID := roachpb.RangeID(1)
	rec := newRaftEntryCache(100)
	rec.addEntries(rangeID, []raftpb.Entry{newEntry(2, 1)})
	rec.addEntries(rangeID, []raftpb.Entry{newEntry(20, 1), newEntry(21, 1)})
	rec.clearTo(rangeID, 22)
	if ents, _, _ := rec.getEntries(rangeID, 2, 21, 0); len(ents) != 0 {
		t.Errorf("expected no entries after clearTo")
	}
}

func TestEntryCacheEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID := roachpb.RangeID(1)
	rec := newRaftEntryCache(100)
	rec.addEntries(rangeID, []raftpb.Entry{newEntry(1, 40), newEntry(2, 40)})
	ents, _, hi := rec.getEntries(rangeID, 1, 3, 0)
	if len(ents) != 2 {
		t.Errorf("expected both entries; got %+v", ents)
	}
	// Add another entry to evict first.
	rec.addEntries(rangeID, []raftpb.Entry{newEntry(3, 40)})
	ents, _, hi = rec.getEntries(rangeID, 2, 4, 0)
	if len(ents) != 2 || hi != 4 {
		t.Errorf("expected only two entries; got %+v, %d", ents, hi)
	}
}
