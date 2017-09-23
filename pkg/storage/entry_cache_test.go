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

package storage

import (
	"reflect"
	"testing"

	"github.com/coreos/etcd/raft/raftpb"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func newEntry(index, size uint64) raftpb.Entry {
	return raftpb.Entry{
		Index: index,
		Data:  make([]byte, size),
	}
}

func addEntries(rec *raftEntryCache, rangeID roachpb.RangeID, lo, hi uint64) []raftpb.Entry {
	ents := []raftpb.Entry{}
	for i := lo; i < hi; i++ {
		ents = append(ents, newEntry(i, 1))
	}
	rec.addEntries(rangeID, ents)
	return ents
}

func verifyGet(
	t *testing.T,
	rec *raftEntryCache,
	rangeID roachpb.RangeID,
	lo, hi uint64,
	expEnts []raftpb.Entry,
	expNextIndex uint64,
) {
	ents, _, nextIndex := rec.getEntries(nil, rangeID, lo, hi, 0)
	if !(len(expEnts) == 0 && len(ents) == 0) && !reflect.DeepEqual(expEnts, ents) {
		t.Fatalf("expected entries %+v; got %+v", expEnts, ents)
	}
	if nextIndex != expNextIndex {
		t.Fatalf("expected next index %d; got %d", nextIndex, expNextIndex)
	}
	for _, e := range ents {
		term, ok := rec.getTerm(rangeID, e.Index)
		if !ok {
			t.Fatalf("expected to be able to retrieve term")
		}
		if term != e.Term {
			t.Fatalf("expected term %d, but got %d", e.Term, term)
		}
	}
}

func TestEntryCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rec := newRaftEntryCache(100)
	rangeID := roachpb.RangeID(2)
	// Add entries for range 1, indexes (1-10).
	ents := addEntries(rec, rangeID, 1, 11)
	// Fetch all data with an exact match.
	verifyGet(t, rec, rangeID, 1, 11, ents, 11)
	// Fetch point entry.
	verifyGet(t, rec, rangeID, 1, 2, ents[0:1], 2)
	// Fetch overlapping first half.
	verifyGet(t, rec, rangeID, 0, 5, []raftpb.Entry{}, 0)
	// Fetch overlapping second half.
	verifyGet(t, rec, rangeID, 9, 12, ents[8:], 11)
	// Fetch data from earlier range.
	verifyGet(t, rec, roachpb.RangeID(1), 1, 11, []raftpb.Entry{}, 1)
	// Fetch data from later range.
	verifyGet(t, rec, roachpb.RangeID(3), 1, 11, []raftpb.Entry{}, 1)
	// Create a gap in the entries.
	rec.delEntries(rangeID, 4, 8)
	// Fetch all data; verify we get only first three.
	verifyGet(t, rec, rangeID, 1, 11, ents[0:3], 4)
	// Try to fetch from within the gap; expect no entries.
	verifyGet(t, rec, rangeID, 5, 11, []raftpb.Entry{}, 5)
	// Fetch after the gap.
	verifyGet(t, rec, rangeID, 8, 11, ents[7:], 11)
	// Delete the prefix of entries.
	rec.delEntries(rangeID, 1, 3)
	// Verify entries are gone.
	verifyGet(t, rec, rangeID, 1, 5, []raftpb.Entry{}, 1)
	// Delete the suffix of entries.
	rec.delEntries(rangeID, 10, 11)
	// Verify get of entries at end of range.
	verifyGet(t, rec, rangeID, 8, 11, ents[7:9], 10)

	for _, index := range []uint64{0, 12} {
		if term, ok := rec.getTerm(rangeID, index); ok {
			t.Fatalf("expected no term, but found %d", term)
		}
	}
}

func TestEntryCacheClearTo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID := roachpb.RangeID(1)
	rec := newRaftEntryCache(100)
	rec.addEntries(rangeID, []raftpb.Entry{newEntry(2, 1)})
	rec.addEntries(rangeID, []raftpb.Entry{newEntry(20, 1), newEntry(21, 1)})
	rec.clearTo(rangeID, 21)
	if ents, _, _ := rec.getEntries(nil, rangeID, 2, 21, 0); len(ents) != 0 {
		t.Errorf("expected no entries after clearTo")
	}
	if ents, _, _ := rec.getEntries(nil, rangeID, 21, 22, 0); len(ents) != 1 {
		t.Errorf("expected entry 22 to remain in the cache clearTo")
	}
}

func TestEntryCacheEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID := roachpb.RangeID(1)
	rec := newRaftEntryCache(100)
	rec.addEntries(rangeID, []raftpb.Entry{newEntry(1, 40), newEntry(2, 40)})
	ents, _, hi := rec.getEntries(nil, rangeID, 1, 3, 0)
	if len(ents) != 2 || hi != 3 {
		t.Errorf("expected both entries; got %+v, %d", ents, hi)
	}
	// Add another entry to evict first.
	rec.addEntries(rangeID, []raftpb.Entry{newEntry(3, 40)})
	ents, _, hi = rec.getEntries(nil, rangeID, 2, 4, 0)
	if len(ents) != 2 || hi != 4 {
		t.Errorf("expected only two entries; got %+v, %d", ents, hi)
	}
}

func BenchmarkEntryCacheClearTo(b *testing.B) {
	rangeID := roachpb.RangeID(1)
	ents := make([]raftpb.Entry, 1000)
	for i := range ents {
		ents[i] = newEntry(uint64(i+1), 8)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		rec := newRaftEntryCache(uint64(len(ents) * len(ents[0].Data)))
		rec.addEntries(rangeID, ents)
		b.StartTimer()
		rec.clearTo(rangeID, uint64(len(ents)-10))
	}
}
