// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulk

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// kvBuf collects []byte key-value pairs in a sortable buffer.
//
// the actual content is stored in a single large slab, instead of individual
// key and value byte slices, reducing the slice header overhead from 48b/pair
// to 16b/pair. The total buffer size cannot be more than 32gb and no one key
// or value may be larger than 512mb.
type kvBuf struct {
	entries []kvBufEntry
	slab    []byte
}

// each entry in the buffer has a key and value -- the actual bytes of these are
// stored in the large slab, so the entry only records the offset and length in
// the slab, packing these together into a uint64 for each. The length is stored
// in the lower `lenBits` and the offset in the higher `64-lenBits`.
type kvBufEntry struct {
	keySpan uint64
	valSpan uint64
}

const entrySizeShift = 4     // sizeof(kvBufEntry) is 16, or shift 4.
const minEntryGrow = 1 << 14 // 16k items or 256KiB of entry size.
const maxEntryGrow = (4 << 20) >> entrySizeShift
const minSlabGrow = 512 << 10
const maxSlabGrow = 64 << 20

const (
	lenBits, lenMask  = 28, 1<<lenBits - 1 // 512mb item limit, 32gb buffer limit.
	maxLen, maxOffset = lenMask, 1<<(64-lenBits) - 1
)

func (b *kvBuf) fits(
	ctx context.Context, toAdd sz, maxUsed sz, acc *mon.EarmarkedBoundAccount,
) bool {
	if len(b.entries) < cap(b.entries) && sz(len(b.slab))+toAdd < sz(cap(b.slab)) {
		return true // fits in current cap, nothing to do.
	}

	used := sz(acc.Used())
	remaining := maxUsed - used

	var entryGrow int
	var slabGrow sz

	if sz(len(b.slab))+toAdd > sz(cap(b.slab)) {
		slabGrow = sz(cap(b.slab))
		if slabGrow < minSlabGrow {
			slabGrow = minSlabGrow
		}
		for slabGrow < toAdd {
			slabGrow += minSlabGrow
		}
		if slabGrow > maxSlabGrow {
			slabGrow = maxSlabGrow
		}
		for slabGrow > remaining && slabGrow > minSlabGrow {
			slabGrow -= minSlabGrow
		}
		// If we can't grow the slab by enough to hit min and fit the new item, then
		// it does not fit.
		if slabGrow < minSlabGrow || slabGrow < toAdd {
			return false
		}
	}

	if len(b.entries) == cap(b.entries) {
		entryGrow = cap(b.entries)
		if entryGrow < minEntryGrow {
			entryGrow = minEntryGrow
		} else if entryGrow > maxEntryGrow {
			entryGrow = maxEntryGrow
		}
	}

	// There's no point in spending capacity on slab if the entries slice cannot
	// grow to point into that new capacity.
	if slabGrow > 0 && len(b.slab) > 0 {
		for {
			fitsInNewSlab := int(float64(slabGrow) / (float64(len(b.slab)) / float64(len(b.entries))))
			if cap(b.entries)+entryGrow >= len(b.entries)+fitsInNewSlab {
				break
			}
			entryGrow += minEntryGrow
			if sz(entryGrow<<entrySizeShift)+slabGrow > remaining {
				slabGrow -= minSlabGrow
			}
		}
	}

	// If above adjustments to the planned growth mean we would not actually fit
	// the bytes being added, then it does not fit.
	if sz(len(b.slab))+toAdd > sz(cap(b.slab))+slabGrow {
		return false
	}

	needed := sz(entryGrow<<entrySizeShift) + slabGrow
	if needed > remaining {
		return false
	}
	if err := acc.Grow(ctx, int64(needed)); err != nil {
		return false
	}
	// We've reserved the additional space so re-alloc and copy over existing data
	// as needed.
	if entryGrow > 0 {
		old := b.entries
		b.entries = make([]kvBufEntry, len(b.entries), cap(b.entries)+entryGrow)
		copy(b.entries, old)
	}
	if slabGrow > 0 {
		old := b.slab
		b.slab = make([]byte, len(b.slab), sz(cap(b.slab))+slabGrow)
		copy(b.slab, old)
	}
	return true
}

func (b *kvBuf) append(k, v []byte) error {
	if len(b.slab) > maxOffset {
		return errors.Errorf("buffer size %d exceeds limit %d", len(b.slab), maxOffset)
	}
	if len(k) > maxLen {
		return errors.Errorf("length %d exceeds limit %d", len(k), maxLen)
	}
	if len(v) > maxLen {
		return errors.Errorf("length %d exceeds limit %d", len(v), maxLen)
	}

	var e kvBufEntry
	e.keySpan = uint64(len(b.slab)<<lenBits) | uint64(len(k)&lenMask)
	b.slab = append(b.slab, k...)
	e.valSpan = uint64(len(b.slab)<<lenBits) | uint64(len(v)&lenMask)
	b.slab = append(b.slab, v...)

	b.entries = append(b.entries, e)
	return nil
}

func (b *kvBuf) read(span uint64) []byte {
	length := span & lenMask
	if length == 0 {
		return nil
	}
	offset := span >> lenBits
	return b.slab[offset : offset+length]
}

func (b *kvBuf) Key(i int) roachpb.Key {
	return b.read(b.entries[i].keySpan)
}

func (b *kvBuf) Value(i int) []byte {
	return b.read(b.entries[i].valSpan)
}

// Len implements sort.Interface.
func (b *kvBuf) Len() int {
	return len(b.entries)
}

// Less implements sort.Interface.
func (b *kvBuf) Less(i, j int) bool {

	return bytes.Compare(b.read(b.entries[i].keySpan), b.read(b.entries[j].keySpan)) < 0
}

// Swap implements sort.Interface.
func (b *kvBuf) Swap(i, j int) {
	b.entries[i], b.entries[j] = b.entries[j], b.entries[i]
}

func (b *kvBuf) MemSize() sz {
	return sz(cap(b.entries)<<entrySizeShift) + sz(cap(b.slab))
}

func (b *kvBuf) KVSize() sz {
	return sz(len(b.slab))
}

func (b *kvBuf) unusedCap() (unusedEntryCap sz, unusedSlabCap sz) {
	unusedEntryCap = sz((cap(b.entries) - len(b.entries)) << entrySizeShift)
	unusedSlabCap = sz(cap(b.slab) - len(b.slab))
	return
}

func (b *kvBuf) Reset() {
	// We could reset sorted to true here but in practice, if we saw any unsorted
	// keys before, the rest are almost always unsorted as well, so we don't even
	// bother checking.
	b.slab = b.slab[:0]
	b.entries = b.entries[:0]
}
