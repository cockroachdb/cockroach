// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"encoding/binary"
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/rangekey"
)

// Ensure that we always update the batch reader to consider any necessary
// updates when a new key kind is introduced. To do this, we assert
// InternalKeyKindMax=23, ensuring that compilation will fail if it's not.
// Unfortunately, this doesn't protect against reusing a currently unused
// RocksDB key kind.
const _ = uint(pebble.InternalKeyKindDeleteSized - pebble.InternalKeyKindMax)

const (
	// The batch header is composed of an 8-byte sequence number (all zeroes) and
	// 4-byte count of the number of entries in the batch.
	headerSize int = 12
	countPos   int = 8
)

// decodeBatchHeader decodes the header of Pebble batch represenation, returning
// both the count of the entries in the batch and the suffix of data remaining
// in the batch.
func decodeBatchHeader(repr []byte) (count int, orepr pebble.BatchReader, err error) {
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

// BatchReader is used to iterate the entries in a Pebble batch
// representation.
//
// Example:
//
//	r, err := NewBatchReader(...)
//	if err != nil {
//	  return err
//	}
//	for r.Next() {
//		switch r.KeyKind() {
//		case pebble.InternalKeyKindDelete:
//			fmt.Printf("delete(%x)", r.Key())
//		case pebble.InternalKeyKindSet:
//			fmt.Printf("put(%x,%x)", r.Key(), r.Value())
//		case pebble.InternalKeyKindMerge:
//			fmt.Printf("merge(%x,%x)", r.Key(), r.Value())
//		case pebble.InternalKeyKindSingleDelete:
//			fmt.Printf("single_delete(%x)", r.Key())
//		case pebble.InternalKeyKindRangeDelete:
//			fmt.Printf("delete_range(%x,%x)", r.Key(), r.Value())
//		 }
//	}
//
//	if err := r.Error(); err != nil {
//	  return err
//	}
type BatchReader struct {
	batchReader pebble.BatchReader

	// The error encountered during iterator, if any
	err error

	// The total number of entries, decoded from the batch header
	count int

	// The following all represent the current entry and are updated by Next.
	// `value` is not applicable for all key kinds. For RangeDelete, value
	// indicates the end key for the range deletion.
	kind  pebble.InternalKeyKind
	key   []byte
	value []byte
}

// NewBatchReader creates a BatchReader from the given batch repr and
// verifies the header.
func NewBatchReader(repr []byte) (*BatchReader, error) {
	count, batchReader, err := decodeBatchHeader(repr)
	if err != nil {
		return nil, err
	}
	return &BatchReader{batchReader: batchReader, count: count}, nil
}

// Count returns the declared number of entries in the batch.
func (r *BatchReader) Count() int {
	return r.count
}

// Error returns the error, if any, which the iterator encountered.
func (r *BatchReader) Error() error {
	return r.err
}

// KeyKind returns the kind of the current entry.
func (r *BatchReader) KeyKind() pebble.InternalKeyKind {
	return r.kind
}

// Key returns the key of the current batch entry.
func (r *BatchReader) Key() []byte {
	return r.key
}

// MVCCKey returns the MVCC key of the current batch entry.
func (r *BatchReader) MVCCKey() (MVCCKey, error) {
	return DecodeMVCCKey(r.Key())
}

// EngineKey returns the EngineKey for the current batch entry.
func (r *BatchReader) EngineKey() (EngineKey, error) {
	key, ok := DecodeEngineKey(r.Key())
	if !ok {
		return key, errors.Errorf("invalid encoded engine key: %x", r.Key())
	}
	return key, nil
}

// Value returns the value of the current batch entry. Value panics if the
// kind is a point key deletion.
func (r *BatchReader) Value() []byte {
	switch r.kind {
	case pebble.InternalKeyKindDelete, pebble.InternalKeyKindSingleDelete:
		panic("cannot call Value on a deletion entry")
	default:
		return r.value
	}
}

// EndKey returns the raw end key of the current ranged batch entry.
func (r *BatchReader) EndKey() ([]byte, error) {
	var rawKey []byte
	switch r.kind {
	case pebble.InternalKeyKindRangeDelete:
		rawKey = r.Value()

	case pebble.InternalKeyKindRangeKeySet, pebble.InternalKeyKindRangeKeyUnset, pebble.InternalKeyKindRangeKeyDelete:
		rangeKeys, err := r.rangeKeys()
		if err != nil {
			return nil, err
		}
		rawKey = rangeKeys.End

	default:
		return nil, errors.AssertionFailedf(
			"can only ask for EndKey on a ranged entry, got %v", r.kind)
	}
	return rawKey, nil
}

// EngineEndKey returns the engine end key of the current ranged batch entry.
func (r *BatchReader) EngineEndKey() (EngineKey, error) {
	rawKey, err := r.EndKey()
	if err != nil {
		return EngineKey{}, err
	}

	key, ok := DecodeEngineKey(rawKey)
	if !ok {
		return key, errors.Errorf("invalid encoded engine key: %x", rawKey)
	}
	return key, nil
}

// RawRangeKeys returns the raw range key values at the current entry.
func (r *BatchReader) RawRangeKeys() ([]rangekey.Key, error) {
	switch r.kind {
	case pebble.InternalKeyKindRangeKeySet, pebble.InternalKeyKindRangeKeyUnset:
	default:
		return nil, errors.AssertionFailedf(
			"can only ask for range keys on a range key entry, got %v", r.kind)
	}
	rangeKeys, err := r.rangeKeys()
	if err != nil {
		return nil, err
	}
	return rangeKeys.Keys, nil
}

// EngineRangeKeys returns the engine range key values at the current entry.
func (r *BatchReader) EngineRangeKeys() ([]EngineRangeKeyValue, error) {
	switch r.kind {
	case pebble.InternalKeyKindRangeKeySet, pebble.InternalKeyKindRangeKeyUnset:
	default:
		return nil, errors.AssertionFailedf(
			"can only ask for range keys on a range key entry, got %v", r.kind)
	}
	rangeKeys, err := r.rangeKeys()
	if err != nil {
		return nil, err
	}
	rkvs := make([]EngineRangeKeyValue, 0, len(rangeKeys.Keys))
	for _, rk := range rangeKeys.Keys {
		rkvs = append(rkvs, EngineRangeKeyValue{Version: rk.Suffix, Value: rk.Value})
	}
	return rkvs, nil
}

// rangeKeys decodes and returns the current Pebble range key.
func (r *BatchReader) rangeKeys() (rangekey.Span, error) {
	return rangekey.Decode(pebble.InternalKey{UserKey: r.key, Trailer: uint64(r.kind)}, r.value, nil)
}

// Next advances to the next entry in the batch, returning false when the batch
// is empty or the next entry fails to decode.
func (r *BatchReader) Next() bool {
	var ok bool
	r.kind, r.key, r.value, ok, r.err = r.batchReader.Next()
	return ok
}

// BatchCount provides an efficient way to get the count of mutations in a batch
// representation.
func BatchCount(repr []byte) (int, error) {
	if len(repr) < headerSize {
		return 0, errors.Errorf("batch repr too small: %d < %d", len(repr), headerSize)
	}
	return int(binary.LittleEndian.Uint32(repr[countPos:headerSize])), nil
}
