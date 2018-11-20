// Copyright 2018 The Cockroach Authors.
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
	"crypto/rand"
	"math"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"go.etcd.io/etcd/raft/raftpb"
)

const noLimit = math.MaxUint64

func newEntry(index, size uint64) raftpb.Entry {
	data := make([]byte, size)
	rand.Read(data)
	return raftpb.Entry{
		Index: index,
		Data:  data,
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
	otherRangeID := rangeID + 1
	// Note 9 bytes per entry with data size of 1

	// Add entries for range 1, indexes (1-10).
	ents := addEntries(c, rangeID, 1, 9)
	verifyMetrics(t, c, 8, 72)
	// Fetch all data with an exact match.
	verifyGet(t, c, rangeID, 1, 9, ents, 9)
	// Fetch point entry.
	verifyGet(t, c, rangeID, 1, 2, ents[0:1], 2)
	// Fetch overlapping first half.
	verifyGet(t, c, rangeID, 0, 3, []raftpb.Entry{}, 0)
	// Fetch overlapping second half.
	verifyGet(t, c, rangeID, 5, 9, ents[4:], 9)
	// Fetch data from earlier range.
	verifyGet(t, c, roachpb.RangeID(1), 1, 11, []raftpb.Entry{}, 1)
	// Fetch data from later range.
	verifyGet(t, c, roachpb.RangeID(3), 1, 11, []raftpb.Entry{}, 1)
	// Add another range which we can evict
	otherEnts := addEntries(c, otherRangeID, 1, 3)
	verifyMetrics(t, c, 10, 90)
	verifyGet(t, c, otherRangeID, 1, 3, otherEnts[:], 3)
	// Add overlapping entries which will lead to eviction
	newEnts := addEntries(c, rangeID, 8, 11)
	ents = append(ents[:7], newEnts...)
	verifyMetrics(t, c, 10, 90)
	// Ensure other range got evicted but first range did not
	verifyGet(t, c, rangeID, 1, 11, ents[:], 11)
	verifyGet(t, c, otherRangeID, 1, 11, []raftpb.Entry{}, 1)
	// Clear and show that it makes space
	c.Clear(rangeID, 10)
	verifyGet(t, c, rangeID, 10, 11, ents[9:], 11)
	verifyMetrics(t, c, 1, 9)
	// Clear again and show that it still works
	c.Clear(rangeID, 10)
	verifyGet(t, c, rangeID, 10, 11, ents[9:], 11)
	verifyMetrics(t, c, 1, 9)
	// Add entries before and show that they get cached
	c.Add(rangeID, ents[5:])
	verifyGet(t, c, rangeID, 6, 11, ents[5:], 11)
	verifyMetrics(t, c, 5, 45)
}

func verifyMetrics(t *testing.T, c *Cache, expectedEntries, expectedBytes int64) {
	if got := c.Metrics().Size.Value(); got != expectedEntries {
		t.Errorf("expected cache to have %d entries, got %d", expectedEntries, got)
	}
	if got := c.Metrics().Bytes.Value(); got != expectedBytes {
		t.Errorf("expected cache to have %d bytes, got %d", expectedBytes, got)
	}
}

func TestIgnoredAdd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID := roachpb.RangeID(1)
	c := NewCache(100)
	_ = addEntries(c, rangeID, 1, 21)
	// show that adding entries which are larger than maxBytes is ignored
	verifyGet(t, c, rangeID, 1, 21, nil, 1)
	verifyMetrics(t, c, 0, 0)
	// add some entries so we can show that a non-overlapping add is ignored
	ents := addEntries(c, rangeID, 4, 7)
	verifyGet(t, c, rangeID, 4, 7, ents, 7)
	verifyMetrics(t, c, 3, 27)
	addEntries(c, rangeID, 1, 3)
	verifyMetrics(t, c, 3, 27)
}

func TestExceededMaxBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID := roachpb.RangeID(1)
	c := NewCache(100)
	addEntries(c, rangeID, 1, 10)
	ents, _, next, exceeded := c.Scan(nil, rangeID, 1, 10, 18)
	if len(ents) != 2 || next != 3 || !exceeded {
		t.Errorf("expected 2 entries with next=3 and to have exceededMaxBytes, got %d, %d, %v",
			len(ents), next, exceeded)
	}
}

func TestEntryCacheClearTo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID := roachpb.RangeID(1)
	c := NewCache(100)
	c.Add(rangeID, []raftpb.Entry{newEntry(20, 1), newEntry(21, 1)})
	c.Clear(rangeID, 21)
	c.Clear(rangeID, 18)
	if ents, _, _, _ := c.Scan(nil, rangeID, 2, 21, noLimit); len(ents) != 0 {
		t.Errorf("expected no entries after clearTo")
	}
	if ents, _, _, _ := c.Scan(nil, rangeID, 21, 22, noLimit); len(ents) != 1 {
		t.Errorf("expected entry 22 to remain in the cache clearTo")
	}
	c.Clear(rangeID, 23) // past the end
	if ents, _, _, _ := c.Scan(nil, rangeID, 21, 22, noLimit); len(ents) != 0 {
		t.Errorf("expected e//ntry 22 to be cleared")
	}
	if _, ok := c.Get(rangeID, 21); ok {
		t.Errorf("didn't expect to get any entry")
	}
	verifyMetrics(t, c, 0, 0)
	c.Clear(rangeID, 22)
}

func TestHeadWrappingForward(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID := roachpb.RangeID(1)
	c := NewCache(100)
	ents := addEntries(c, rangeID, 1, 4)
	c.Clear(rangeID, 2)
	verifyMetrics(t, c, 2, 18)
	ents = append(ents[1:], addEntries(c, rangeID, 4, 8)...)
	verifyMetrics(t, c, 6, 54)
	ents = append(ents[:5], addEntries(c, rangeID, 7, 10)...)
	verifyGet(t, c, rangeID, 2, 10, ents, 10)
}

func TestHeadWrappingBackwards(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID := roachpb.RangeID(1)
	c := NewCache(100)
	ents := addEntries(c, rangeID, 3, 5)
	c.Clear(rangeID, 4)
	ents = append(addEntries(c, rangeID, 1, 4), ents[1:]...)
	verifyGet(t, c, rangeID, 1, 5, ents, 5)
}

func TestPanicOnNonContiguousRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewCache(100)
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic with non-contiguous range")
		}
	}()
	c.Add(1, []raftpb.Entry{newEntry(1, 1), newEntry(3, 1)})
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
	if c.entries != 2 {
		t.Errorf("expected size=2; got %d", c.entries)
	}
	// Add another entry to the same range. This will exceed the size limit and
	// lead to eviction.
	c.Add(rangeID, []raftpb.Entry{newEntry(3, 30)})
	ents, _, hi, _ = c.Scan(nil, rangeID, 1, 4, noLimit)
	if len(ents) != 0 || hi != 1 {
		t.Errorf("expected no entries; got %+v, %d", ents, hi)
	}
	if _, ok := c.Get(rangeID, 1); ok {
		t.Errorf("didn't expect to get evicted entry")
	}
	if c.entries != 1 {
		t.Errorf("expected size=1; got %d", c.entries)
	}
	ents, _, hi, _ = c.Scan(nil, rangeID, 3, 4, noLimit)
	if len(ents) != 1 || hi != 4 {
		t.Errorf("expected the new entry; got %+v, %d", ents, hi)
	}
	if c.entries != 1 {
		t.Errorf("expected size=1; got %d", c.entries)
	}
	c.Add(rangeID2, []raftpb.Entry{newEntry(20, 1), newEntry(21, 1)})
	ents, _, hi, _ = c.Scan(nil, rangeID2, 20, 22, noLimit)
	if len(ents) != 2 || hi != 22 {
		t.Errorf("expected both entries; got %+v, %d", ents, hi)
	}
	if c.entries != 3 { // 1 in rangeID, 2 in rangeID2
		t.Errorf("expected size=3; got %d", c.entries)
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
		c := NewCache(uint64(15 * len(ents) * len(ents[0].Data)))
		for i := roachpb.RangeID(0); i < 10; i++ {
			if i != rangeID {
				c.Add(i, ents)
			}
		}
		b.StartTimer()
		c.Add(rangeID, ents)
		_, _, _, _ = c.Scan(nil, rangeID, 0, uint64(len(ents)-10), noLimit)
		c.Clear(rangeID, uint64(len(ents)-10))
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
		c.Clear(rangeID, uint64(len(ents)-10))
	}
}
