// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raftentry

import (
	"math"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const noLimit = math.MaxUint64

func newEntry(index, size uint64) raftpb.Entry {
	r := rand.New(rand.NewSource(int64(index * size)))
	data := make([]byte, size)
	if _, err := r.Read(data); err != nil {
		panic(err)
	}
	ent := raftpb.Entry{
		Index: index,
		Data:  data,
	}
	for {
		entSize := uint64(ent.Size())
		if entSize == size {
			return ent
		}
		if entSize < size {
			panic("size undershot")
		}
		delta := entSize - size
		if uint64(len(ent.Data)) < delta {
			panic("can't shorten ent.Data to target size")
		}
		ent.Data = ent.Data[delta:]
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
	ents := newEntries(lo, hi, 9)
	c.Add(rangeID, ents, false)
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
	t.Helper()
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
			t.Fatalf("expected to be able to retrieve entry")
		}
		if !reflect.DeepEqual(found, e) {
			t.Fatalf("expected entry %v, but got %v", e, found)
		}
	}
}

func requireEqual(t *testing.T, c *Cache, rangeID roachpb.RangeID, idxs ...uint64) {
	t.Helper()
	p := c.getPartLocked(rangeID, false /* create */, false /* recordUse */)
	if p == nil {
		if len(idxs) > 0 {
			t.Fatalf("expected idxs=%v but got empty cache", idxs)
		}
		return
	}
	b := &p.ringBuf
	it := first(b)
	var act []uint64
	ok := it.valid(b)
	for ok {
		act = append(act, it.index(b))
		it, ok = it.next(b)
	}
	require.Equal(t, idxs, act)
}

func TestEntryCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewCache(100 + 2*uint64(partitionSize))
	rangeID := roachpb.RangeID(2)
	otherRangeID := rangeID + 1
	// Note 9 bytes per entry with data size of 1
	verify := func(rangeID roachpb.RangeID, lo, hi uint64, ents []raftpb.Entry, expNextIndex uint64) {
		t.Helper()
		verifyGet(t, c, rangeID, lo, hi, ents, expNextIndex, false)
	}
	// Add entries for range 1, indexes [1-9).
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
	// Add another range which we can evict.
	otherEnts := addEntries(c, otherRangeID, 1, 3)
	verifyMetrics(t, c, 10, 90+2*int64(partitionSize))
	verify(otherRangeID, 1, 3, otherEnts[:], 3)
	// Add overlapping entries which will lead to eviction.
	newEnts := addEntries(c, rangeID, 8, 11)
	ents = append(ents[:7], newEnts...)
	verifyMetrics(t, c, 10, 90+int64(partitionSize))
	// Ensure other range got evicted but first range did not.
	verify(rangeID, 1, 11, ents[:], 11)
	verify(otherRangeID, 1, 11, []raftpb.Entry{}, 1)
	// Clear and show that it makes space.
	c.Clear(rangeID, 10)
	verify(rangeID, 10, 11, ents[9:], 11)
	verifyMetrics(t, c, 1, 9+int64(partitionSize))
	// Clear again and show that it still works.
	c.Clear(rangeID, 10)
	verify(rangeID, 10, 11, ents[9:], 11)
	verifyMetrics(t, c, 1, 9+int64(partitionSize))
	// Add entries from before and show that they get cached.
	c.Add(rangeID, ents[5:], false)
	verify(rangeID, 6, 11, ents[5:], 11)
	verifyMetrics(t, c, 5, 45+int64(partitionSize))
	// Add a few repeat entries and show that nothing changes.
	c.Add(rangeID, ents[6:8], false)
	verify(rangeID, 6, 11, ents[5:], 11)
	verifyMetrics(t, c, 5, 45+int64(partitionSize))
	// Add a few repeat entries while truncating.
	// Non-overlapping tail should be evicted.
	c.Add(rangeID, ents[6:8], true)
	verify(rangeID, 6, 11, ents[5:8], 9)
	verifyMetrics(t, c, 3, 27+int64(partitionSize))
}

func verifyMetrics(t *testing.T, c *Cache, expectedEntries, expectedBytes int64) {
	t.Helper()
	// NB: Cache gauges are updated asynchronously. In the face of concurrent
	// updates gauges may not reflect the current cache state. Force
	// synchrononization of the metrics before using them to validate cache state.
	c.syncGauges()
	if got := c.Metrics().Size.Value(); got != expectedEntries {
		t.Errorf("expected cache to have %d entries, got %d", expectedEntries, got)
	}
	if got := c.Metrics().Bytes.Value(); got != expectedBytes {
		t.Errorf("expected cache to have %d bytes, got %d", expectedBytes, got)
	}
}

func (c *Cache) syncGauges() {
	c.updateGauges(c.addBytes(0), c.addEntries(0))
}

func TestIgnoredAdd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID := roachpb.RangeID(1)
	c := NewCache(100 + uint64(partitionSize))
	// Show that adding entries which are larger than maxBytes is ignored.
	{
		ignoredEnts := addEntries(c, rangeID, 1, 41)
		require.True(t, 42*ignoredEnts[0].Size() > int(c.maxBytes)) // sanity check
	}
	verifyGet(t, c, rangeID, 1, 41, nil, 1, false)
	requireEqual(t, c, rangeID)
	verifyMetrics(t, c, 0, 0)
	// Add some entries so we can show that a non-overlapping add is ignored.
	ents := addEntries(c, rangeID, 4, 7)
	verifyGet(t, c, rangeID, 4, 7, ents, 7, false)
	requireEqual(t, c, rangeID, 4, 5, 6)
	verifyMetrics(t, c, 3, 27+int64(partitionSize))
	addEntries(c, rangeID, 1, 3) // no-op because [1,2] does not overlap [4,5,6]
	requireEqual(t, c, rangeID, 4, 5, 6)
	verifyMetrics(t, c, 3, 27+int64(partitionSize))

	// Cache has entries 4, 5, 6. Offer an oversize entry at index 7 (which is
	// notably after 6) and request truncation. This should be a no-op.
	c.Add(rangeID, []raftpb.Entry{newEntry(7, uint64(c.maxBytes+1))}, true /* truncate */)
	requireEqual(t, c, rangeID, 4, 5, 6)
	verifyGet(t, c, rangeID, 4, 7, ents, 7, false)

	// Cache has entries 4, 5, 6. Offer an oversize entry at index 6 and request
	// truncation. This should remove index 6 (as requested due to the truncation)
	// without replacing it with the input entry.
	c.Add(rangeID, []raftpb.Entry{newEntry(6, uint64(c.maxBytes+1))}, true /* truncate */)
	requireEqual(t, c, rangeID, 4, 5)
	verifyGet(t, c, rangeID, 4, 7, ents[:len(ents)-1], 6, false)

	// Cache has entries 4, 5. Offer an oversize entry at index 3 (which is
	// notably before 4) and request truncation. This should clear all entries
	// >= 3, i.e. everything.
	c.Add(rangeID, []raftpb.Entry{newEntry(3, uint64(c.maxBytes+1))}, true /* truncate */)
	// And it did.
	requireEqual(t, c, rangeID)
	verifyGet(t, c, rangeID, 0, 0, nil, 0, false)
	verifyMetrics(t, c, 0, int64(partitionSize))

	addEntries(c, rangeID, 10, 13)
	// Now, cache = [10, 11, 12].
	requireEqual(t, c, rangeID, 10, 11, 12)
	verifyMetrics(t, c, 3, 3*9+int64(partitionSize))

	c.Add(rangeID, newEntries(3, 4, 9), true /* truncate */)
	requireEqual(t, c, rangeID, 3)
	verifyMetrics(t, c, 1, 1*9+int64(partitionSize))
}

func TestRingBuffer_truncateFrom(t *testing.T) {
	// NB: this is also exercised through the truncate=true test cases in
	// TestIgnoredAdd, but this tests it more directly.
	rangeID := roachpb.RangeID(1)
	const maxBytes = 100
	c := NewCache(maxBytes)
	// Add one entry.
	c.Add(rangeID, newEntries(100, 101, 9), false /* truncate */)
	ents, _, _, _ := c.Scan(nil, rangeID, 100, 101, noLimit)
	// Entry is actually there.
	require.Len(t, ents, 1)
	p := c.getPartLocked(rangeID, false /* create */, false /* recordUse */)
	require.NotNil(t, p)
	// Truncate range [99, infinity], which should work even though
	// 99 isn't itself in `p`.
	_, numRemovedEntries := p.truncateFrom(99)
	require.EqualValues(t, 1, numRemovedEntries)
	require.Zero(t, p.len)
	ents, _, _, _ = c.Scan(nil, rangeID, 100, 101, noLimit)
	require.Empty(t, ents)
}

func TestRingBuffer_clearTo(t *testing.T) {
	// Ensure that clearTo isn't sensitive to whether its argument actually
	// refers to a cached index.
	rangeID := roachpb.RangeID(1)
	const maxBytes = 100
	c := NewCache(maxBytes)
	// Add one entry.
	c.Add(rangeID, newEntries(100, 101, 9), false /* truncate */)
	ents, _, _, _ := c.Scan(nil, rangeID, 100, 101, noLimit)
	// Entry is actually there.
	require.Len(t, ents, 1)
	p := c.getPartLocked(rangeID, false /* create */, false /* recordUse */)
	require.NotNil(t, p)
	_, numRemovedEntries := p.clearTo(101)
	require.EqualValues(t, 1, numRemovedEntries)
	require.Zero(t, p.len)
	ents, _, _, _ = c.Scan(nil, rangeID, 100, 101, noLimit)
	require.Empty(t, ents)
}

func TestAddAndTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID := roachpb.RangeID(1)
	c := NewCache(200 + uint64(partitionSize))
	ents := addEntries(c, rangeID, 1, 10)
	verifyGet(t, c, rangeID, 1, 10, ents, 10, false)
	verifyMetrics(t, c, 9, 81+int64(partitionSize))
	// Show that, if specified, adding an overlapping set of entries
	// truncates any at a larger index that is not overwritten.
	c.Add(rangeID, ents[2:6], true /* truncate */)
	verifyGet(t, c, rangeID, 1, 10, ents[:6], 7, false)
	verifyMetrics(t, c, 6, 54+int64(partitionSize))
	// Show that even if the addition is ignored due to size, entries
	// with an equal or larger index are truncated.
	largeEnts := newEntries(5, 6, 900)
	c.Add(rangeID, largeEnts, true /* truncate */)
	verifyGet(t, c, rangeID, 1, 10, ents[:4], 5, false)
	verifyMetrics(t, c, 4, 36+int64(partitionSize))
}

func TestDrop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const (
		r1 roachpb.RangeID = 1
		r2 roachpb.RangeID = 2

		sizeOf9Entries = 81
	)
	partitionSize := int64(sizeOf9Entries + partitionSize)
	c := NewCache(1 << 10)
	ents1 := addEntries(c, r1, 1, 10)
	verifyGet(t, c, r1, 1, 10, ents1, 10, false)
	verifyMetrics(t, c, 9, partitionSize)
	ents2 := addEntries(c, r2, 1, 10)
	verifyGet(t, c, r2, 1, 10, ents2, 10, false)
	verifyMetrics(t, c, 18, 2*partitionSize)
	c.Drop(r1)
	verifyMetrics(t, c, 9, partitionSize)
	c.Drop(r2)
	verifyMetrics(t, c, 0, 0)
}

func TestCacheLaterEntries(t *testing.T) {
	c := NewCache(1000)
	rangeID := roachpb.RangeID(1)
	ents := addEntries(c, rangeID, 1, 10)
	verifyGet(t, c, rangeID, 1, 10, ents, 10, false)
	// The previous entries are evicted because they would not have been
	// contiguous with the new entries.
	ents = addEntries(c, rangeID, 11, 21)
	if got, _, _, _ := c.Scan(nil, rangeID, 1, 10, noLimit); len(got) != 0 {
		t.Fatalf("Expected not to get entries from range which preceded new values")
	}
	verifyGet(t, c, rangeID, 11, 21, ents, 21, false)
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
	c.Add(rangeID, []raftpb.Entry{newEntry(20, 9), newEntry(21, 9)}, true)
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

func TestConcurrentEvictions(t *testing.T) {
	// This tests for safety in the face of concurrent updates.
	// The main goroutine randomly chooses a free partition for a read or write.
	// Concurrent operations will lead to evictions. Reads verify that either the
	// data is read and correct or is not read. At the end, all the ranges are
	// cleared and we ensure that the entry count is zero.

	// NB: N is chosen based on the race detector's limit of 8128 goroutines.
	const N = 8000
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
			toAdd = newEntries(lo, hi, 9)
			ents = append(ents[:offset], toAdd...)
		} else {
			lo := uint64(offset + 2)
			hi := lo + uint64(length)
			toAdd = newEntries(lo, hi, 9)
			ents = toAdd
		}
		rangeData[r] = ents
		wg.Add(1)
		go func() {
			time.Sleep(time.Duration(rand.Intn(int(time.Microsecond))))
			c.Add(r, toAdd, true)
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
	// Clear the data and ensure that the cache stats are valid.
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
	// Clear some space at the front of the ringBuf.
	c.Clear(rangeID, 4)
	verifyMetrics(t, c, 4, 36+int64(partitionSize))
	// Fill in space at the front of the ringBuf.
	ents = append(ents[3:4], addEntries(c, rangeID, 5, 20)...)
	verifyMetrics(t, c, 16, 144+int64(partitionSize))
	verifyGet(t, c, rangeID, 4, 20, ents, 20, false)
	// Realloc copying from the wrapped around ringBuf.
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
	c.Add(1, []raftpb.Entry{newEntry(1, 1), newEntry(3, 1)}, true)
}

func TestEntryCacheEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	rangeID, rangeID2 := roachpb.RangeID(1), roachpb.RangeID(2)
	c := NewCache(140 + uint64(partitionSize))
	c.Add(rangeID, []raftpb.Entry{newEntry(1, 40), newEntry(2, 40)}, true)
	ents, _, hi, _ := c.Scan(nil, rangeID, 1, 3, noLimit)
	if len(ents) != 2 || hi != 3 {
		t.Errorf("expected both entries; got %+v, %d", ents, hi)
	}
	if c.entries != 2 {
		t.Errorf("expected size=2; got %d", c.entries)
	}
	// Add another entry to the same range. This will exceed the size limit and
	// lead to eviction.
	c.Add(rangeID, []raftpb.Entry{newEntry(3, 80)}, true)
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
	c.Add(rangeID, []raftpb.Entry{newEntry(3, 9)}, true)
	verifyMetrics(t, c, 1, c.Metrics().Bytes.Value())
	c.Add(rangeID2, []raftpb.Entry{newEntry(20, 9), newEntry(21, 9)}, true)
	ents, _, hi, _ = c.Scan(nil, rangeID2, 20, 22, noLimit)
	if len(ents) != 2 || hi != 22 {
		t.Errorf("expected both entries; got %+v, %d", ents, hi)
	}
	verifyMetrics(t, c, 3, c.Metrics().Bytes.Value())
	// Evict from rangeID by adding more to rangeID 2.
	c.Add(rangeID2, []raftpb.Entry{newEntry(20, 35), newEntry(21, 35)}, true)
	if _, ok := c.Get(rangeID, 3); ok {
		t.Errorf("didn't expect to get evicted entry")
	}
}

// TestConcurrentUpdates ensures that concurrent updates to the same do not
// race with each other.
func TestConcurrentUpdates(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := NewCache(10000)
	const r1 roachpb.RangeID = 1
	ents := []raftpb.Entry{newEntry(20, 35), newEntry(21, 35)}
	// Test using both Clear and Drop to remove the added entries.
	for _, clearMethod := range []struct {
		name  string
		clear func()
	}{
		{"drop", func() { c.Drop(r1) }},
		{"clear", func() { c.Clear(r1, ents[len(ents)-1].Index+1) }},
	} {
		t.Run(clearMethod.name, func(t *testing.T) {
			// NB: N is chosen based on the race detector's limit of 8128 goroutines.
			const N = 8000
			var wg sync.WaitGroup
			wg.Add(N)
			for i := 0; i < N; i++ {
				go func(i int) {
					if i%2 == 1 {
						c.Add(r1, ents, true)
					} else {
						clearMethod.clear()
					}
					wg.Done()
				}(i)
			}
			wg.Wait()
			clearMethod.clear()
			// Clear does not evict the partition struct itself so we expect the cache
			// to have a partition's initial byte size when using Clear and nothing
			// when using Drop.
			switch clearMethod.name {
			case "drop":
				verifyMetrics(t, c, 0, 0)
			case "clear":
				verifyMetrics(t, c, 0, int64(initialSize.bytes()))
			}
		})
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

// TestConcurrentClearAddGet exercises the case where a partition is
// concurrently written to as well as read and evicted from. Partitions are
// created and added to the cache lazily upon calls to Add. Because of the two
// level locking, a partition may be created and added to the cache before the
// Add call proceeds to lock the partition and the entries to the buffer.
// During this period a concurrent read operation may discover the uninitialized
// empty partition. This test attempts to exercise these scenarios and ensure
// that they are safe.
func TestConcurrentAddGetAndEviction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const N = 1000
	var wg sync.WaitGroup
	doAction := func(action func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < N; i++ {
				action()
			}
		}()
	}
	// A cache size of 1000 is chosen relative to the below entry size of 500
	// so that each add operation will lead to the eviction of the other
	// partition.
	c := NewCache(1000)
	ents := []raftpb.Entry{newEntry(1, 500)}
	doAddAndGetToRange := func(rangeID roachpb.RangeID) {
		doAction(func() { c.Add(rangeID, ents, true) })
		doAction(func() { c.Get(rangeID, ents[0].Index) })
	}
	doAddAndGetToRange(1)
	doAddAndGetToRange(2)
	wg.Wait()
}

func BenchmarkEntryCache(b *testing.B) {
	rangeID := roachpb.RangeID(1)
	ents := make([]raftpb.Entry, 1000)
	for i := range ents {
		ents[i] = newEntry(uint64(i+1), 9)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c := NewCache(uint64(15 * len(ents) * len(ents[0].Data)))
		for i := roachpb.RangeID(0); i < 10; i++ {
			if i != rangeID {
				c.Add(i, ents, true)
			}
		}
		b.StartTimer()
		c.Add(rangeID, ents, true)
		_, _, _, _ = c.Scan(nil, rangeID, 0, uint64(len(ents)-10), noLimit)
		c.Clear(rangeID, uint64(len(ents)-10))
	}
}

func BenchmarkEntryCacheClearTo(b *testing.B) {
	rangeID := roachpb.RangeID(1)
	ents := make([]raftpb.Entry, 1000)
	for i := range ents {
		ents[i] = newEntry(uint64(i+1), 9)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		c := NewCache(uint64(10 * len(ents) * len(ents[0].Data)))
		c.Add(rangeID, ents, true)
		b.StartTimer()
		c.Clear(rangeID, uint64(len(ents)-10))
	}
}
