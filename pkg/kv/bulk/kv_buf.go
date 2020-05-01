// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	MemSize int // size of buffered data including per-entry overhead
}

// each entry in the buffer has a key and value -- the actual bytes of these are
// stored in the large slab, so the entry only records the offset and length in
// the slab, packing these together into a uint64 for each. The length is stored
// in the lower `lenBits` and the offset in the higher `64-lenBits`.
type kvBufEntry struct {
	keySpan uint64
	valSpan uint64
}

// entryOverhead is the slice header overhead per KV pair
const entryOverhead = 16

const (
	lenBits, lenMask  = 28, 1<<lenBits - 1 // 512mb item limit, 32gb buffer limit.
	maxLen, maxOffset = lenMask, 1<<(64-lenBits) - 1
)

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

	b.MemSize += len(k) + len(v) + entryOverhead
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

func (b *kvBuf) Reset() {
	b.slab = b.slab[:0]
	b.entries = b.entries[:0]
	b.MemSize = 0
}
