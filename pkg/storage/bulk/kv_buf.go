// Copyright 2019 The Cockroach Authors.
//
/// Licensed under the Apache License, Version 2.0 (the "License");
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

package bulk

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

const entryOverhead = 16

const (
	lenBits, lenMask  = 28, 1<<lenBits - 1 // 512mb item limit, 32gb buffer limit.
	maxLen, maxOffset = lenMask, 1<<(64-lenBits) - 1
)

func (b *kvBuf) append(k, v []byte) {
	if len(b.slab) > maxOffset {
		panic(fmt.Sprintf("buffer size %d exceeds limit %d", len(b.slab), maxOffset))
	}
	if len(k) > maxLen {
		panic(fmt.Sprintf("length %d exceeds limit %d", len(k), maxLen))
	}
	if len(v) > maxLen {
		panic(fmt.Sprintf("length %d exceeds limit %d", len(v), maxLen))
	}

	b.MemSize += len(k) + len(v) + entryOverhead
	var e kvBufEntry
	e.keySpan = uint64(len(b.slab)<<lenBits) | uint64(len(k)&lenMask)
	b.slab = append(b.slab, k...)
	e.valSpan = uint64(len(b.slab)<<lenBits) | uint64(len(v)&lenMask)
	b.slab = append(b.slab, v...)

	b.entries = append(b.entries, e)
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
