// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"encoding/binary"

	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/pebble"
	"github.com/pkg/errors"
)

// BatchType represents the type of an entry in an encoded RocksDB batch.
type BatchType byte

// These constants come from rocksdb/db/dbformat.h.
const (
	BatchTypeDeletion BatchType = 0x0
	BatchTypeValue    BatchType = 0x1
	BatchTypeMerge    BatchType = 0x2
	BatchTypeLogData  BatchType = 0x3
	// BatchTypeColumnFamilyDeletion       BatchType = 0x4
	// BatchTypeColumnFamilyValue          BatchType = 0x5
	// BatchTypeColumnFamilyMerge          BatchType = 0x6
	BatchTypeSingleDeletion BatchType = 0x7
	// BatchTypeColumnFamilySingleDeletion BatchType = 0x8
	// BatchTypeBeginPrepareXID            BatchType = 0x9
	// BatchTypeEndPrepareXID              BatchType = 0xA
	// BatchTypeCommitXID                  BatchType = 0xB
	// BatchTypeRollbackXID                BatchType = 0xC
	// BatchTypeNoop                       BatchType = 0xD
	// BatchTypeColumnFamilyRangeDeletion  BatchType = 0xE
	BatchTypeRangeDeletion BatchType = 0xF
	// BatchTypeColumnFamilyBlobIndex      BatchType = 0x10
	// BatchTypeBlobIndex                  BatchType = 0x11
	// BatchMaxValue                       BatchType = 0x7F
)

const (
	// The batch header is composed of an 8-byte sequence number (all zeroes) and
	// 4-byte count of the number of entries in the batch.
	headerSize       int = 12
	countPos             = 8
	initialBatchSize     = 1 << 10 // 1 KB
)

// RocksDBBatchBuilder is used to construct the RocksDB batch representation.
// From the RocksDB code, the representation of a batch is:
//
//   WriteBatch::rep_ :=
//      sequence: fixed64
//      count: fixed32
//      data: record[count]
//   record :=
//      kTypeValue varstring varstring
//      kTypeDeletion varstring
//      [...] (see BatchType)
//   varstring :=
//      len: varint32
//      data: uint8[len]
//
// The RocksDBBatchBuilder code currently only supports kTypeValue
// (BatchTypeValue), kTypeDeletion (BatchTypeDeletion), kTypeMerge
// (BatchTypeMerge), and kTypeSingleDeletion (BatchTypeSingleDeletion)
// operations. Before a batch is written to the RocksDB write-ahead-log,
// the sequence number is 0. The "fixed32" format is little endian.
//
// The keys encoded into the batch are MVCC keys: a string key with a timestamp
// suffix. MVCC keys are encoded as:
//
//   <key>[<wall_time>[<logical>]]<#timestamp-bytes>
//
// The <wall_time> and <logical> portions of the key are encoded as 64 and
// 32-bit big-endian integers. A custom RocksDB comparator is used to maintain
// the desired ordering as these keys do not sort lexicographically correctly.
// Note that the encoding of these keys needs to match up with the encoding in
// rocksdb/db.cc:EncodeKey().
type RocksDBBatchBuilder struct {
	batch   pebble.Batch
	logData bool
}

func (b *RocksDBBatchBuilder) reset() {
	b.batch.Reset()
	b.logData = false
}

// Finish returns the constructed batch representation. After calling Finish,
// the builder may be used to construct another batch, but the returned []byte
// is only valid until the next builder method is called.
func (b *RocksDBBatchBuilder) Finish() []byte {
	repr := b.batch.Repr()
	b.reset()

	return repr
}

// Len returns the number of bytes currently in the under construction repr.
func (b *RocksDBBatchBuilder) Len() int {
	return len(b.batch.Repr())
}

var _ = (*RocksDBBatchBuilder).Len

// getRepr constructs the batch representation and returns it.
func (b *RocksDBBatchBuilder) getRepr() []byte {
	return b.batch.Repr()
}

// Put sets the given key to the value provided.
//
// It is safe to modify the contents of the arguments after Put returns.
func (b *RocksDBBatchBuilder) Put(key MVCCKey, value []byte) {
	keyLen := key.Len()
	deferredOp := b.batch.SetDeferred(keyLen, len(value))
	encodeKeyToBuf(deferredOp.Key, key, keyLen)
	copy(deferredOp.Value, value)
	// NB: the batch is not indexed, obviating the need to call
	// deferredOp.Finish.
}

// Merge is a high-performance write operation used for values which are
// accumulated over several writes. Multiple values can be merged sequentially
// into a single key; a subsequent read will return a "merged" value which is
// computed from the original merged values.
//
// It is safe to modify the contents of the arguments after Merge returns.
func (b *RocksDBBatchBuilder) Merge(key MVCCKey, value []byte) {
	keyLen := key.Len()
	deferredOp := b.batch.MergeDeferred(keyLen, len(value))
	encodeKeyToBuf(deferredOp.Key, key, keyLen)
	copy(deferredOp.Value, value)
	// NB: the batch is not indexed, obviating the need to call
	// deferredOp.Finish.
}

// Clear removes the item from the db with the given key.
//
// It is safe to modify the contents of the arguments after Clear returns.
func (b *RocksDBBatchBuilder) Clear(key MVCCKey) {
	keyLen := key.Len()
	deferredOp := b.batch.DeleteDeferred(keyLen)
	encodeKeyToBuf(deferredOp.Key, key, keyLen)
	// NB: the batch is not indexed, obviating the need to call
	// deferredOp.Finish.
}

// SingleClear removes the most recent item from the db with the given key.
//
// It is safe to modify the contents of the arguments after SingleClear returns.
func (b *RocksDBBatchBuilder) SingleClear(key MVCCKey) {
	keyLen := key.Len()
	deferredOp := b.batch.SingleDeleteDeferred(keyLen)
	encodeKeyToBuf(deferredOp.Key, key, keyLen)
	// NB: the batch is not indexed, obviating the need to call
	// deferredOp.Finish.
}

// LogData adds a blob of log data to the batch. It will be written to the WAL,
// but otherwise uninterpreted by RocksDB.
//
// It is safe to modify the contents of the arguments after LogData returns.
func (b *RocksDBBatchBuilder) LogData(data []byte) {
	_ = b.batch.LogData(data, nil)
	b.logData = true
}

// ApplyRepr applies the mutations in repr to the current batch.
//
// It is safe to modify the contents of the arguments after ApplyRepr
// returns.
func (b *RocksDBBatchBuilder) ApplyRepr(repr []byte) error {
	b2 := &pebble.Batch{}
	if err := b2.SetRepr(repr); err != nil {
		return err
	}

	return b.batch.Apply(b2, nil)
}

// Count returns the count of memtable-modifying operations in this batch.
func (b *RocksDBBatchBuilder) Count() uint32 {
	return b.batch.Count()
}

// EncodeKey encodes an engine.MVCC key into the RocksDB representation. This
// encoding must match with the encoding in engine/db.cc:EncodeKey().
func EncodeKey(key MVCCKey) []byte {
	keyLen := key.Len()
	buf := make([]byte, keyLen)
	encodeKeyToBuf(buf, key, keyLen)
	return buf
}

// EncodeKeyToBuf encodes an engine.MVCC key into the RocksDB representation.
// This encoding must match with the encoding in engine/db.cc:EncodeKey().
func EncodeKeyToBuf(buf []byte, key MVCCKey) []byte {
	keyLen := key.Len()
	if cap(buf) < keyLen {
		buf = make([]byte, keyLen)
	} else {
		buf = buf[:keyLen]
	}
	encodeKeyToBuf(buf, key, keyLen)
	return buf
}

func encodeKeyToBuf(buf []byte, key MVCCKey, keyLen int) {
	const (
		timestampSentinelLen = 1
		walltimeEncodedLen   = 8
		logicalEncodedLen    = 4
	)

	copy(buf, key.Key)

	pos := len(key.Key)
	timestampLength := keyLen - pos - 1
	if timestampLength > 0 {
		buf[pos] = 0
		pos += timestampSentinelLen
		binary.BigEndian.PutUint64(buf[pos:], uint64(key.Timestamp.WallTime))
		pos += walltimeEncodedLen
		if key.Timestamp.Logical != 0 {
			binary.BigEndian.PutUint32(buf[pos:], uint32(key.Timestamp.Logical))
			pos += logicalEncodedLen
		}
	}
	buf[len(buf)-1] = byte(timestampLength)
}

func encodeTimestamp(ts hlc.Timestamp) []byte {
	_, encodedTS, _ := enginepb.SplitMVCCKey(EncodeKey(MVCCKey{Timestamp: ts}))
	return encodedTS
}

// DecodeMVCCKey decodes an engine.MVCCKey from its serialized representation. This
// decoding must match engine/db.cc:DecodeKey().
func DecodeMVCCKey(encodedKey []byte) (MVCCKey, error) {
	k, ts, err := enginepb.DecodeKey(encodedKey)
	return MVCCKey{k, ts}, err
}

// Decode the header of RocksDB batch repr, returning both the count of the
// entries in the batch and the suffix of data remaining in the batch.
func rocksDBBatchDecodeHeader(repr []byte) (count int, orepr pebble.BatchReader, err error) {
	if len(repr) < headerSize {
		return 0, nil, errors.Errorf("batch repr too small: %d < %d", len(repr), headerSize)
	}
	seq := binary.LittleEndian.Uint64(repr[:countPos])
	if seq != 0 {
		return 0, nil, errors.Errorf("bad sequence: expected 0, but found %d", seq)
	}
	count = int(binary.LittleEndian.Uint32(repr[countPos:headerSize]))
	return count, pebble.MakeBatchReader(repr), nil
}

// RocksDBBatchReader is used to iterate the entries in a RocksDB batch
// representation.
//
// Example:
// r, err := NewRocksDBBatchReader(...)
// if err != nil {
//   return err
// }
// for r.Next() {
// 	 switch r.BatchType() {
// 	 case BatchTypeDeletion:
// 	   fmt.Printf("delete(%x)", r.Key())
// 	 case BatchTypeValue:
// 	   fmt.Printf("put(%x,%x)", r.Key(), r.Value())
// 	 case BatchTypeMerge:
// 	   fmt.Printf("merge(%x,%x)", r.Key(), r.Value())
//   case BatchTypeSingleDeletion:
// 	   fmt.Printf("single_delete(%x)", r.Key())
//   case BatchTypeRangeDeletion:
// 	   fmt.Printf("delete_range(%x,%x)", r.Key(), r.Value())
// 	 }
// }
// if err := r.Error(); err != nil {
//   return err
// }
type RocksDBBatchReader struct {
	batchReader pebble.BatchReader

	// The error encountered during iterator, if any
	err error

	// The total number of entries, decoded from the batch header
	count int

	// The following all represent the current entry and are updated by Next.
	// `value` is not applicable for BatchTypeDeletion or BatchTypeSingleDeletion.
	// `value` indicates the end key for BatchTypeRangeDeletion.
	typ   BatchType
	key   []byte
	value []byte
}

// NewRocksDBBatchReader creates a RocksDBBatchReader from the given repr and
// verifies the header.
func NewRocksDBBatchReader(repr []byte) (*RocksDBBatchReader, error) {
	count, batchReader, err := rocksDBBatchDecodeHeader(repr)
	if err != nil {
		return nil, err
	}
	return &RocksDBBatchReader{batchReader: batchReader, count: count}, nil
}

// Count returns the declared number of entries in the batch.
func (r *RocksDBBatchReader) Count() int {
	return r.count
}

// Error returns the error, if any, which the iterator encountered.
func (r *RocksDBBatchReader) Error() error {
	return r.err
}

// BatchType returns the type of the current batch entry.
func (r *RocksDBBatchReader) BatchType() BatchType {
	return r.typ
}

// Key returns the key of the current batch entry.
func (r *RocksDBBatchReader) Key() []byte {
	return r.key
}

func decodeMVCCKey(k []byte) (MVCCKey, error) {
	k, ts, err := enginepb.DecodeKey(k)
	return MVCCKey{k, ts}, err
}

// MVCCKey returns the MVCC key of the current batch entry.
func (r *RocksDBBatchReader) MVCCKey() (MVCCKey, error) {
	return decodeMVCCKey(r.Key())
}

// Value returns the value of the current batch entry. Value panics if the
// BatchType is BatchTypeDeleted.
func (r *RocksDBBatchReader) Value() []byte {
	if r.typ == BatchTypeDeletion || r.typ == BatchTypeSingleDeletion {
		panic("cannot call Value on a deletion entry")
	}
	return r.value
}

// MVCCEndKey returns the MVCC end key of the current batch entry.
func (r *RocksDBBatchReader) MVCCEndKey() (MVCCKey, error) {
	if r.typ != BatchTypeRangeDeletion {
		panic("cannot only call Value on a range deletion entry")
	}
	return decodeMVCCKey(r.Value())
}

// Next advances to the next entry in the batch, returning false when the batch
// is empty.
func (r *RocksDBBatchReader) Next() bool {
	kind, ukey, value, ok := r.batchReader.Next()

	r.typ = BatchType(kind)
	r.key = ukey
	r.value = value

	return ok
}

// RocksDBBatchCount provides an efficient way to get the count of mutations
// in a RocksDB Batch representation.
func RocksDBBatchCount(repr []byte) (int, error) {
	if len(repr) < headerSize {
		return 0, errors.Errorf("batch repr too small: %d < %d", len(repr), headerSize)
	}
	return int(binary.LittleEndian.Uint32(repr[countPos:headerSize])), nil
}
