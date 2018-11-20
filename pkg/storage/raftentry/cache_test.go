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
	"math"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"go.etcd.io/etcd/raft/raftpb"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

const noLimit = math.MaxUint64

func newEntry(index, size uint64) raftpb.Entry {
	data := make([]byte, size)
	if _, err := rand.Read(data); err != nil {
		panic(err)
	}
	return raftpb.Entry{
		Index: index,
		Data:  data,
	}
}

func newEntries(lo, hi, size uint64) []raftpb.Entry {
	ents := []raftpb.Entry{}
	for i := lo; i < hi; i++ {
		ents = append(ents, newEntry(i, size))
	}
	return ents
}

func addEntries(c *Cache, rangeID roachpb.RangeID, lo, hi uint64) []raftpb.Entry {
	ents := newEntries(lo, hi, 1)
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
	allowEviction bool,
) {
	ents, _, nextIndex, _ := c.Scan(nil, rangeID, lo, hi, noLimit)
	if allowEviction && len(ents) == 0 {
		return
	}
	if !(len(expEnts) == 0 && len(ents) == 0) && !reflect.DeepEqual(expEnts, ents) {
		t.Fatalf("expected entries %+v; got %+v", expEnts, ents)
	}
	if nextIndex != expNextIndex {
		t.Fatalf("expected next index %d; got %d", expNextIndex, nextIndex)
	}
	for _, e := range ents {
		found, ok := c.Get(rangeID, e.Index)
		if !ok {
			if allowEviction {
				break
			}
			t.Fatalf("expected to be able to retrieve term")
		}
		if !reflect.DeepEqual(found, e) {
			t.Fatalf("expected entry %v, but got %v", e, found)
		}
	}
}

func TestEntryCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewCache(100 + 2*uint64(partitionSize))
	rangeID := roachpb.RangeID(2)
	otherRangeID := rangeID + 1
	// Note 9 bytes per entry with data size of 1
	verify := func(rangeID roachpb.RangeID, lo, hi uint64, ents []raftpb.Entry, expNextIndex uint64) {
		verifyGet(t, c, rangeID, lo, hi, ents, expNextIndex, false)
	}
	// Add entries for range 1, indexes (1-10).
	ents := addEntries(c, rangeID, 1, 9)
	verifyMetrics(t, c, 8, 72+int64(partitionSize))
	// Fetch all data with an exact match.
	verify(rangeID, 1, 9, ents, 9)
	// Fetch point entry.
	verify(rangeID, 1, 2, ents[0:1], 2)
	// Fetch overlapping first half.
	verify(rangeID, 0, 3, []raftpb.Entry{}, 0)
	// Fetch overlapping second half.
	verify(rangeID, 5, 9, ents[4:], 9)
	// Fetch data from earlier range.
	verify(roachpb.RangeID(1), 1, 11, []raftpb.Entry{}, 1)
	// Fetch data from later range.
	verify(roachpb.RangeID(3), 1, 11, []raftpb.Entry{}, 1)
	// Add another range which we can evict
	otherEnts := addEntries(c, otherRangeID, 1, 3)
	verifyMetrics(t, c, 10, 90+2*int64(partitionSize))
	verify(otherRangeID, 1, 3, otherEnts[:], 3)
	// Add overlapping entries which will lead to eviction
	newEnts := addEntries(c, rangeID, 8, 11)
	ents = append(ents[:7], newEnts...)
	verifyMetrics(t, c, 10, 90+int64(partitionSize))
	// Ensure other range got evicted but first range did not
	verify(rangeID, 1, 11, ents[:], 11)
	verify(otherRangeID, 1, 11, []raftpb.Entry{}, 1)
	// Clear and show that it makes space
	c.Clear(rangeID, 10)
	verify(rangeID, 10, 11, ents[9:], 11)
	verifyMetrics(t, c, 1, 9+int64(partitionSize))
	// Clear again and show that it still works
	c.Clear(rangeID, 10)
	verify(rangeID, 10, 11, ents[9:], 11)
	verifyMetrics(t, c, 1, 9+int64(partitionSize))
	// Add entries before and show that they get cached
	c.Add(rangeID, ents[5:])
	verify(rangeID, 6, 11, ents[5:], 11)
	verifyMetrics(t, c, 5, 45+int64(partitionSize))
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
	c := NewCache(100 + uint64(partitionSize))
	_ = addEntries(c, rangeID, 1, 41)
	// show that adding entries which are larger than maxBytes is ignored
	verifyGet(t, c, rangeID, 1, 41, nil, 1, false)
	verifyMetrics(t, c, 0, 0)
	// add some entries so we can show that a non-overlapping add is ignored
	ents := addEntries(c, rangeID, 4, 7)
	verifyGet(t, c, rangeID, 4, 7, ents, 7, false)
	verifyMetrics(t, c, 3, 27+int64(partitionSize))
	addEntries(c, rangeID, 1, 3)
	verifyMetrics(t, c, 3, 27+int64(partitionSize))
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
	verifyMetrics(t, c, 0, int64(partitionSize))
	c.Clear(rangeID, 22)
}

func TestMaxBytesLimit(t *testing.T) {
	c := NewCache(1 << 32)
	if c.maxBytes != (1<<31 - 1) {
		t.Fatalf("maxBytes cannot be larger than %d", 1<<31)
	}
}

func TestCacheLaterEntries(t *testing.T) {
	c := NewCache(1000)
	rangeID := roachpb.RangeID(1)
	ents := addEntries(c, rangeID, 1, 10)
	verifyGet(t, c, rangeID, 1, 10, ents, 10, false)
	ents = addEntries(c, rangeID, 11, 21)
	if got, _, _, _ := c.Scan(nil, rangeID, 1, 10, noLimit); len(got) != 0 {
		t.Fatalf("Expected not to get entries from range which preceded new values")
	}
	verifyGet(t, c, rangeID, 11, 21, ents, 21, false)
}

func TestConcurrentEvictions(t *testing.T) {
	// This tests for safety in the face of concurrent updates.
	// The main goroutine randomly chooses a free partition for a read or write.
	// Concurrent operations will lead to evictions. Reads verify that either the
	// data is read and correct or is not read. At the end, all the ranges are
	// cleared and we ensure that the entry count is zero.

	const N = 20000
	const numRanges = 200
	const maxEntriesPerWrite = 111
	rangeData := make(map[roachpb.RangeID][]raftpb.Entry)
	rangeInUse := make(map[roachpb.RangeID]bool)
	c := NewCache(1000)
	rangeDoneChan := make(chan roachpb.RangeID)
	pickRange := func() (r roachpb.RangeID) {
		for {
			r = roachpb.RangeID(rand.Intn(numRanges))
			if !rangeInUse[r] {
				break
			}
		}
		rangeInUse[r] = true
		return r
	}
	var wg sync.WaitGroup
	doRead := func(r roachpb.RangeID) {
		ents := rangeData[r]
		offset := rand.Intn(len(ents))
		length := rand.Intn(len(ents) - offset)
		lo := ents[offset].Index
		hi := lo + uint64(length)
		wg.Add(1)
		go func() {
			time.Sleep(time.Duration(rand.Intn(int(time.Microsecond))))
			verifyGet(t, c, r, lo, hi, ents[offset:offset+length], hi, true)
			rangeDoneChan <- r
			wg.Done()
		}()
	}
	doWrite := func(r roachpb.RangeID) {
		ents := rangeData[r]
		offset := rand.Intn(len(ents)+1) - 1
		length := rand.Intn(maxEntriesPerWrite)
		var toAdd []raftpb.Entry
		if offset >= 0 && offset < len(ents) {
			lo := ents[offset].Index
			hi := lo + uint64(length)
			toAdd = newEntries(lo, hi, 1)
			ents = append(ents[:offset], toAdd...)
		} else {
			lo := uint64(offset + 2)
			hi := lo + uint64(length)
			toAdd = newEntries(lo, hi, 1)
			ents = toAdd
		}
		rangeData[r] = ents
		wg.Add(1)
		go func() {
			time.Sleep(time.Duration(rand.Intn(int(time.Microsecond))))
			c.Add(r, toAdd)
			rangeDoneChan <- r
			wg.Done()
		}()
	}
	for i := 0; i < N; i++ {
		for len(rangeInUse) > numRanges/2 {
			delete(rangeInUse, <-rangeDoneChan)
		}
		r := pickRange()
		if read := rand.Intn(2) == 1; read && len(rangeData[r]) > 0 {
			doRead(r)
		} else {
			doWrite(r)
		}
	}
	go func() { wg.Wait(); close(rangeDoneChan) }()
	for r := range rangeDoneChan {
		delete(rangeInUse, r)
	}
	// Clear the data and ensure that the cache stats are valid
	for r, data := range rangeData {
		if len(data) == 0 {
			continue
		}
		c.Clear(r, data[len(data)-1].Index+1)
	}

	verifyMetrics(t, c, 0, int64(len(c.parts))*int64(partitionSize))
}

func TestHeadWrappingForward(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID := roachpb.RangeID(1)
	c := NewCache(200 + uint64(partitionSize))
	ents := addEntries(c, rangeID, 1, 8)
	// Clear some space at the front of the ringBuf
	c.Clear(rangeID, 4)
	verifyMetrics(t, c, 4, 36+int64(partitionSize))
	// Fill in space at the front of the ringBuf
	ents = append(ents[3:4], addEntries(c, rangeID, 5, 20)...)
	verifyMetrics(t, c, 16, 144+int64(partitionSize))
	verifyGet(t, c, rangeID, 4, 20, ents, 20, false)
	// Realloc copying from the wrapped around ringBuf
	ents = append(ents, addEntries(c, rangeID, 20, 22)...)
	verifyGet(t, c, rangeID, 18, 22, ents[14:], 22, false)
}

func TestHeadWrappingBackwards(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID := roachpb.RangeID(1)
	c := NewCache(100 + uint64(partitionSize))
	ents := addEntries(c, rangeID, 3, 5)
	c.Clear(rangeID, 4)
	ents = append(addEntries(c, rangeID, 1, 4), ents[1:]...)
	verifyGet(t, c, rangeID, 1, 5, ents, 5, false)
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
	c := NewCache(140 + uint64(partitionSize))
	c.Add(rangeID, []raftpb.Entry{newEntry(1, 40), newEntry(2, 40)})
	ents, _, hi, _ := c.Scan(nil, rangeID, 1, 3, noLimit)
	if len(ents) != 2 || hi != 3 {
		t.Errorf("expected both entries; got %+v, %d", ents, hi)
	}
	if c.entries != 2 {
		t.Errorf("expected size=2; got %d", c.entries)
	}
	// Add another entry to the same range. This will exceed the size limit and
	// lead to eviction.
	c.Add(rangeID, []raftpb.Entry{newEntry(3, 40)})
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
	c.Add(rangeID, []raftpb.Entry{newEntry(3, 1)})
	verifyMetrics(t, c, 1, c.Metrics().Bytes.Value())
	c.Add(rangeID2, []raftpb.Entry{newEntry(20, 1), newEntry(21, 1)})
	ents, _, hi, _ = c.Scan(nil, rangeID2, 20, 22, noLimit)
	if len(ents) != 2 || hi != 22 {
		t.Errorf("expected both entries; got %+v, %d", ents, hi)
	}
	verifyMetrics(t, c, 3, c.Metrics().Bytes.Value())
	// evict from rangeID by adding more to rangeID2
	c.Add(rangeID2, []raftpb.Entry{newEntry(20, 35), newEntry(21, 35)})
	if _, ok := c.Get(rangeID, 3); ok {
		t.Errorf("didn't expect to get evicted entry")
	}
}

func TestPartitionList(t *testing.T) {
	var l partitionList
	first := l.pushFront(1)
	l.remove(first)
	if l.back() != nil {
		t.Fatalf("Expected back to be nil after removing the only element")
	}
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("Expected panic when removing list root")
		}
	}()
	l.remove(&l.root)
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
		c := NewCache(uint64(10 * len(ents) * len(ents[0].Data)))
		c.Add(rangeID, ents)
		b.StartTimer()
		c.Clear(rangeID, uint64(len(ents)-10))
	}
}
