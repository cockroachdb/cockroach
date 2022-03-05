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
	"encoding/binary"
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/golang/snappy"
)

type iterable interface {
	Next() bool
	Key() roachpb.Key
	Value() []byte
	MemSize() sz
	Len() int
}

// kvBuf collects []byte key-value pairs in a sortable buffer.
//
// the actual content is stored in a single large slab, instead of individual
// key and value byte slices, reducing the slice header overhead from 48b/pair
// to 16b/pair. The total buffer size cannot be more than 32gb and no one key
// or value may be larger than 512mb.
type kvBuf struct {
	entries []kvBufEntry
	slab    []byte
	memSize sz // size of buffered data including per-entry overhead
	pos     int
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

	b.memSize += sz(len(k) + len(v) + entryOverhead)
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

func (b *kvBuf) Key() roachpb.Key {
	return b.read(b.entries[b.pos].keySpan)
}

func (b *kvBuf) Value() []byte {
	return b.read(b.entries[b.pos].valSpan)
}

func (b *kvBuf) Next() bool {
	b.pos++
	return b.pos < len(b.entries)
}

func (b *kvBuf) KeyAt(i int) roachpb.Key {
	return b.read(b.entries[i].keySpan)
}

func (b *kvBuf) ValueAt(i int) []byte {
	return b.read(b.entries[i].valSpan)
}

func (b kvBuf) MemSize() sz {
	return b.memSize
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
	// We could reset sorted to true here but in practice, if we saw any unsorted
	// keys before, the rest are almost always unsorted as well, so we don't even
	// bother checking.
	b.slab = b.slab[:0]
	b.entries = b.entries[:0]
	b.memSize = 0
	b.pos = -1
}

// sortedBuffers wraps some number of sorted iterables into a single merged
// iterable that returns results in sorted order, yielding the KV from whichever
// iterable has the minimum key first. If multiple iterables yield the same key
// the order in which they are returned is undefined.
type sortedBuffers struct {
	iters   []iterable
	min     int
	memSize sz
	len     int
}

func (m *sortedBuffers) Next() bool {
	if m.min != -1 {
		if !m.iters[m.min].Next() {
			m.iters[m.min] = m.iters[len(m.iters)-1]
			m.iters = m.iters[:len(m.iters)-1]
		}
	}

	if len(m.iters) == 0 {
		return false
	}

	m.min = 0

	if len(m.iters) > 1 {
		for i := range m.iters {
			if i != m.min {
				if bytes.Compare(m.iters[m.min].Key(), m.iters[i].Key()) == 1 {
					m.min = i
				}
			}
		}
	}
	return true
}

func (m *sortedBuffers) Key() roachpb.Key {
	return m.iters[m.min].Key()
}

func (m *sortedBuffers) Value() []byte {
	return m.iters[m.min].Value()
}

func (m *sortedBuffers) add(sorted iterable) error {
	if !sorted.Next() {
		return io.ErrUnexpectedEOF
	}
	m.iters = append(m.iters, sorted)
	m.memSize += sorted.MemSize()
	m.len += sorted.Len()
	return nil
}

func (m *sortedBuffers) reset() {
	m.iters = nil
	m.memSize = 0
	m.min = -1
}

func (m sortedBuffers) MemSize() sz {
	return m.memSize
}

func (m sortedBuffers) Len() int {
	return m.len
}

// compressedBuffer encodes a sequence of key/value pairs as encoded-length+val
// which are then compressed for storage in memory and decompressed in frames as
// they are iterated back. Unlike kvBuf, it does not allow random access to the
// i'th key, so it is generally only useful for holding already-sorted kv pairs.
type compressedBuffer struct {
	r        *snappy.Reader
	memSize  sz
	len      int
	key, val []byte
}

// snappyReaderOverhead reflects the buffers sizes in a snappy.Reader, taken
// from snappy.go.
const snappyReaderOverhead = 65536 + 76490

func compressBuffer(sorted *kvBuf) (iterable, error) {
	var underlying bytes.Buffer
	w := snappy.NewBufferedWriter(&underlying)
	buf := make([]byte, binary.MaxVarintLen64*2)
	for sorted.Next() {
		k := sorted.Key()
		v := sorted.Value()
		n := binary.PutUvarint(buf, uint64(len(k)))
		n += binary.PutUvarint(buf[n:], uint64(len(v)))
		if _, err := w.Write(buf[:n]); err != nil {
			return nil, err
		}
		if _, err := w.Write(k); err != nil {
			return nil, err
		}
		if _, err := w.Write(v); err != nil {
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	res := underlying.Bytes()
	// TODO(dt): if cap >>> len, realloc and copy to shrink.
	return &compressedBuffer{
		r:       snappy.NewReader(bytes.NewReader(res)),
		memSize: sz(cap(res) + snappyReaderOverhead),
		len:     sorted.Len(),
	}, nil
}

func (i *compressedBuffer) Next() bool {
	k, err := binary.ReadUvarint(i.r)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			panic(err)
		}
		return false
	}
	v, err := binary.ReadUvarint(i.r)
	if err != nil {
		panic(err)
	}

	if uint64(cap(i.key)) < k {
		i.key = make([]byte, k)
	}
	i.key = i.key[:k]
	if _, err := io.ReadFull(i.r, i.key); err != nil {
		panic(err)
	}
	if uint64(cap(i.val)) < v {
		i.val = make([]byte, v)
	}
	i.val = i.val[:v]
	if _, err := io.ReadFull(i.r, i.val); err != nil {
		panic(err)
	}
	return true
}

func (i *compressedBuffer) Key() roachpb.Key {
	return i.key
}

func (i *compressedBuffer) Value() []byte {
	return i.val
}

func (i *compressedBuffer) MemSize() sz {
	return i.memSize
}

func (i *compressedBuffer) Len() int {
	return i.len
}
