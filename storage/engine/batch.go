// Copyright 2014 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package engine

import "github.com/cockroachdb/cockroach/util/hlc"

const (
	batchTypeDeletion byte = 0x0
	batchTypeValue         = 0x1
	batchTypeMerge         = 0x2

	// The batch header is composed of an 8-byte sequence number (all zeroes) and
	// 4-byte count of the number of entries in the batch.
	headerSize       int = 12
	initialBatchSize     = 1 << 10
	maxVarintLen32       = 5
)

// A rocksDBBatchBuilder constructs a RocksDB batch representation. From the
// RocksDB code, the representation of a batch is:
//
//   WriteBatch::rep_ :=
//      sequence: fixed64
//      count: fixed32
//      data: record[count]
//   record :=
//      kTypeValue varstring varstring
//      kTypeDeletion varstring
//      kTypeSingleDeletion varstring
//      kTypeMerge varstring varstring
//      kTypeColumnFamilyValue varint32 varstring varstring
//      kTypeColumnFamilyDeletion varint32 varstring varstring
//      kTypeColumnFamilySingleDeletion varint32 varstring varstring
//      kTypeColumnFamilyMerge varint32 varstring varstring
//   varstring :=
//      len: varint32
//      data: uint8[len]
//
// The rocksDBBatchBuilder code currently only supports kTypeValue
// (batchTypeValue), kTypeDeletion (batchTypeDeletion)and kTypeMerge
// (batchTypeMerge) operations. Before a batch is written to the RocksDB
// write-ahead-log, the sequence number is 0. The "fixed32" format is little
// endian.
//
// The keys encoded into the batch or MVCC keys: a string key with a timestamp
// suffix. MVCC keys are encoded as:
//
//   <key>[<wall_time>[<logical>]]<#timestamp-bytes>
//
// The <wall_time> and <logical> portions of the key are encoded as 64 and
// 32-bit big-endian integers. A custom RocksDB comparator is used to maintain
// the desired ordering as these keys do not sort lexicographically
// correctly. Note that the encoding of these keys needs to match up with the
// encoding in rocksdb/db.cc:EncodeKey().
type rocksDBBatchBuilder struct {
	repr  []byte
	count int
}

func (b *rocksDBBatchBuilder) maybeInit() {
	if b.repr == nil {
		b.repr = make([]byte, headerSize, initialBatchSize)
	}
}

// Finish returns the constructed batch representation. After calling Finish,
// the builder may be used to construct another batch, but the returned []byte
// is only valid until the next builder method is called.
func (b *rocksDBBatchBuilder) Finish() []byte {
	buf := b.repr[8:headerSize]
	v := uint32(b.count)
	buf[0] = byte(v)
	buf[1] = byte(v >> 8)
	buf[2] = byte(v >> 16)
	buf[3] = byte(v >> 24)
	repr := b.repr
	b.repr = b.repr[:headerSize]
	b.count = 0
	return repr
}

func (b *rocksDBBatchBuilder) grow(n int) {
	newSize := len(b.repr) + n
	if newSize > cap(b.repr) {
		newCap := 2 * cap(b.repr)
		for newCap < newSize {
			newCap *= 2
		}
		newRepr := make([]byte, len(b.repr), newCap)
		copy(newRepr, b.repr)
		b.repr = newRepr
	}
	b.repr = b.repr[:newSize]
}

func putUvarint32(buf []byte, x uint32) int {
	i := 0
	for x >= 0x80 {
		buf[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	buf[i] = byte(x)
	return i + 1
}

func putUint32(b []byte, v uint32) {
	b[0] = byte(v >> 24)
	b[1] = byte(v >> 16)
	b[2] = byte(v >> 8)
	b[3] = byte(v)
}

func putUint64(b []byte, v uint64) {
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
}

// encodeKey encodes an MVCC key into the batch, reserving extra bytes in
// b.repr for use in encoding a value as well. This encoding must match with
// the encoding in engine/db.cc:EncodeKey().
func (b *rocksDBBatchBuilder) encodeKey(key MVCCKey, extra int) {
	length := 1 + len(key.Key)
	timestampLength := 0
	if key.Timestamp != hlc.ZeroTimestamp {
		timestampLength = 1 + 8
		if key.Timestamp.Logical != 0 {
			timestampLength += 4
		}
	}
	length += timestampLength

	pos := 1 + len(b.repr)
	b.grow(1 + maxVarintLen32 + length + extra)
	n := putUvarint32(b.repr[pos:], uint32(length))
	b.repr = b.repr[:len(b.repr)-(maxVarintLen32-n)]
	pos += n
	copy(b.repr[pos:], key.Key)
	if timestampLength > 0 {
		pos += len(key.Key)
		b.repr[pos] = 0
		pos++
		putUint64(b.repr[pos:], uint64(key.Timestamp.WallTime))
		if key.Timestamp.Logical != 0 {
			pos += 8
			putUint32(b.repr[pos:], uint32(key.Timestamp.Logical))
		}
	}
	b.repr[len(b.repr)-1-extra] = byte(timestampLength)
}

func (b *rocksDBBatchBuilder) encodeKeyValue(key MVCCKey, value []byte, tag byte) {
	b.maybeInit()
	b.count++

	l := uint32(len(value))
	extra := int(l) + maxVarintLen32

	pos := len(b.repr)
	b.encodeKey(key, extra)
	b.repr[pos] = tag

	pos = len(b.repr) - extra
	n := putUvarint32(b.repr[pos:], l)
	b.repr = b.repr[:len(b.repr)-(maxVarintLen32-n)]
	copy(b.repr[pos+n:], value)
}

func (b *rocksDBBatchBuilder) Put(key MVCCKey, value []byte) {
	b.encodeKeyValue(key, value, batchTypeValue)
}

func (b *rocksDBBatchBuilder) Merge(key MVCCKey, value []byte) {
	b.encodeKeyValue(key, value, batchTypeMerge)
}

func (b *rocksDBBatchBuilder) Clear(key MVCCKey) {
	b.maybeInit()
	b.count++
	pos := len(b.repr)
	b.encodeKey(key, 0)
	b.repr[pos] = batchTypeDeletion
}
