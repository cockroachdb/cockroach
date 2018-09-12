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

package raftentry

import (
	"math"
	"reflect"
	"testing"

	"go.etcd.io/etcd/raft/raftpb"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

const noLimit = math.MaxUint64

func newEntry(index, size uint64) raftpb.Entry {
	return raftpb.Entry{
		Index: index,
		Data:  make([]byte, size),
	}
}

func addEntries(c *Cache, rangeID roachpb.RangeID, lo, hi uint64) []raftpb.Entry {
	ents := []raftpb.Entry{}
	for i := lo; i < hi; i++ {
		ents = append(ents, newEntry(i, 1))
	}
	c.Add(rangeID, ents)
	return ents
}

func verifyGet(
	t *testing.T,
	c *Cache,
	rangeID roachpb.RangeID,
	lo, hi uint64,
	expEnts []raftpb.Entry,
	expNextIndex uint64,
) {
	ents, _, nextIndex, _ := c.Scan(nil, rangeID, lo, hi, noLimit)
	if !(len(expEnts) == 0 && len(ents) == 0) && !reflect.DeepEqual(expEnts, ents) {
		t.Fatalf("expected entries %+v; got %+v", expEnts, ents)
	}
	if nextIndex != expNextIndex {
		t.Fatalf("expected next index %d; got %d", expNextIndex, nextIndex)
	}
	for _, e := range ents {
		found, ok := c.Get(rangeID, e.Index)
		if !ok {
			t.Fatalf("expected to be able to retrieve term")
		}
		if !reflect.DeepEqual(found, e) {
			t.Fatalf("expected entry %v, but got %v", e, found)
		}
	}
}

func TestEntryCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewCache(100)
	rangeID := roachpb.RangeID(2)
	// Add entries for range 1, indexes (1-10).
	ents := addEntries(c, rangeID, 1, 11)
	// Fetch all data with an exact match.
	verifyGet(t, c, rangeID, 1, 11, ents, 11)
	// Fetch point entry.
	verifyGet(t, c, rangeID, 1, 2, ents[0:1], 2)
	// Fetch overlapping first half.
	verifyGet(t, c, rangeID, 0, 5, []raftpb.Entry{}, 0)
	// Fetch overlapping second half.
	verifyGet(t, c, rangeID, 9, 12, ents[8:], 11)
	// Fetch data from earlier range.
	verifyGet(t, c, roachpb.RangeID(1), 1, 11, []raftpb.Entry{}, 1)
	// Fetch data from later range.
	verifyGet(t, c, roachpb.RangeID(3), 1, 11, []raftpb.Entry{}, 1)
	// Create a gap in the entries.
	c.Clear(rangeID, 4, 8)
	// Fetch all data; verify we get only first three.
	verifyGet(t, c, rangeID, 1, 11, ents[0:3], 4)
	// Try to fetch from within the gap; expect no entries.
	verifyGet(t, c, rangeID, 5, 11, []raftpb.Entry{}, 5)
	// Fetch after the gap.
	verifyGet(t, c, rangeID, 8, 11, ents[7:], 11)
	// Delete the prefix of entries.
	c.Clear(rangeID, 1, 3)
	// Verify entries are gone.
	verifyGet(t, c, rangeID, 1, 5, []raftpb.Entry{}, 1)
	// Delete the suffix of entries.
	c.Clear(rangeID, 10, 11)
	// Verify get of entries at end of range.
	verifyGet(t, c, rangeID, 8, 11, ents[7:9], 10)

	for _, index := range []uint64{0, 12} {
		if e, ok := c.Get(rangeID, index); ok {
			t.Fatalf("expected no entry, but found %v", e)
		}
	}
}

func TestEntryCacheClearTo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID := roachpb.RangeID(1)
	c := NewCache(100)
	c.Add(rangeID, []raftpb.Entry{newEntry(2, 1)})
	c.Add(rangeID, []raftpb.Entry{newEntry(20, 1), newEntry(21, 1)})
	c.Clear(rangeID, 0, 21)
	if ents, _, _, _ := c.Scan(nil, rangeID, 2, 21, noLimit); len(ents) != 0 {
		t.Errorf("expected no entries after clearTo")
	}
	if ents, _, _, _ := c.Scan(nil, rangeID, 21, 22, noLimit); len(ents) != 1 {
		t.Errorf("expected entry 22 to remain in the cache clearTo")
	}
}

func TestEntryCacheEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID, rangeID2 := roachpb.RangeID(1), roachpb.RangeID(2)
	c := NewCache(100)
	c.Add(rangeID, []raftpb.Entry{newEntry(1, 30), newEntry(2, 30)})
	ents, _, hi, _ := c.Scan(nil, rangeID, 1, 3, noLimit)
	if len(ents) != 2 || hi != 3 {
		t.Errorf("expected both entries; got %+v, %d", ents, hi)
	}
	if c.size != 2 {
		t.Errorf("expected size=2; got %d", c.size)
	}
	// Add another entry to the same range. This would have
	// exceeded the size limit, so it won't be added.
	c.Add(rangeID, []raftpb.Entry{newEntry(3, 30)})
	ents, _, hi, _ = c.Scan(nil, rangeID, 1, 4, noLimit)
	if len(ents) != 2 || hi != 3 {
		t.Errorf("expected two entries; got %+v, %d", ents, hi)
	}
	if c.size != 2 {
		t.Errorf("expected size=2; got %d", c.size)
	}
	// Replace the first entry with a smaller one, then add
	// third entry again. This time, there should be enough
	// space.
	c.Add(rangeID, []raftpb.Entry{newEntry(1, 5), newEntry(3, 30)})
	ents, _, hi, _ = c.Scan(nil, rangeID, 1, 4, noLimit)
	if len(ents) != 3 || hi != 4 {
		t.Errorf("expected three entries; got %+v, %d", ents, hi)
	}
	if c.size != 3 {
		t.Errorf("expected size=3; got %d", c.size)
	}
	// Add an entry to the new range. Should cause the old
	// range's partition to be evicted.
	c.Add(rangeID2, []raftpb.Entry{newEntry(1, 30)})
	ents, _, _, _ = c.Scan(nil, rangeID, 1, 4, noLimit)
	if len(ents) != 0 {
		t.Errorf("expected no entries; got %+v", ents)
	}
	ents, _, hi, _ = c.Scan(nil, rangeID2, 1, 2, noLimit)
	if len(ents) != 1 || hi != 2 {
		t.Errorf("expected one entry; got %+v, %d", ents, hi)
	}
	if c.size != 1 {
		t.Errorf("expected size=1; got %d", c.size)
	}
}

func BenchmarkEntryCache(b *testing.B) {
	rangeID := roachpb.RangeID(1)
	ents := make([]raftpb.Entry, 1000)
	for i := range ents {
		ents[i] = newEntry(uint64(i+1), 8)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c := NewCache(15 * uint64(len(ents)*len(ents[0].Data)))
		for i := roachpb.RangeID(0); i < 10; i++ {
			if i != rangeID {
				c.Add(i, ents)
			}
		}
		b.StartTimer()
		c.Add(rangeID, ents)
		_, _, _, _ = c.Scan(nil, rangeID, 0, uint64(len(ents)-10), noLimit)
		c.Clear(rangeID, 0, uint64(len(ents)-10))
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
		c := NewCache(uint64(len(ents) * len(ents[0].Data)))
		c.Add(rangeID, ents)
		b.StartTimer()
		c.Clear(rangeID, 0, uint64(len(ents)-10))
	}
}
