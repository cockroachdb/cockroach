// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"encoding/binary"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
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
	headerSize int = 12
	countPos   int = 8
)

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
	r, c := pebble.ReadBatch(repr)
	if c > math.MaxInt32 {
		return 0, nil, errors.Errorf("count %d would overflow max int", c)
	}
	return int(c), r, nil
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

// MVCCKey returns the MVCC key of the current batch entry.
func (r *RocksDBBatchReader) MVCCKey() (MVCCKey, error) {
	return DecodeMVCCKey(r.Key())
}

// EngineKey returns the EngineKey for the current batch entry.
func (r *RocksDBBatchReader) EngineKey() (EngineKey, error) {
	key, ok := DecodeEngineKey(r.Key())
	if !ok {
		return key, errors.Errorf("invalid encoded engine key: %x", r.Key())
	}
	return key, nil
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
//lint:ignore U1001 unused
func (r *RocksDBBatchReader) MVCCEndKey() (MVCCKey, error) {
	if r.typ != BatchTypeRangeDeletion {
		panic("can only ask for EndKey on a range deletion entry")
	}
	return DecodeMVCCKey(r.Value())
}

// EngineEndKey returns the engine end key of the current batch entry.
func (r *RocksDBBatchReader) EngineEndKey() (EngineKey, error) {
	if r.typ != BatchTypeRangeDeletion {
		panic("can only ask for EndKey on a range deletion entry")
	}
	key, ok := DecodeEngineKey(r.Value())
	if !ok {
		return key, errors.Errorf("invalid encoded engine key: %x", r.Value())
	}
	return key, nil
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
