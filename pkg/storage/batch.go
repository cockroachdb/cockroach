// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"math"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/batchrepr"
	"github.com/cockroachdb/pebble/rangekey"
)

// Ensure that we always update the batch reader to consider any necessary
// updates when a new key kind is introduced. To do this, we assert that the
// latest key we considered equals InternalKeyKindMax, ensuring that compilation
// will fail if it's not. Unfortunately, this doesn't protect against reusing a
// currently unused RocksDB key kind.
const _ = uint(pebble.InternalKeyKindExcise - pebble.InternalKeyKindMax)

// decodeBatchHeader decodes the header of Pebble batch representation,
// returning the parsed header and a batchrepr.Reader into the contents of the
// batch.
func decodeBatchHeader(repr []byte) (h batchrepr.Header, r batchrepr.Reader, err error) {
	h, ok := batchrepr.ReadHeader(repr)
	switch {
	case !ok:
		return batchrepr.Header{}, nil, errors.Errorf("batch invalid: too small: %d bytes", len(repr))
	case h.SeqNum != 0:
		return batchrepr.Header{}, nil, errors.Errorf("batch invalid: bad sequence: expected 0, but found %d", h.SeqNum)
	case h.Count > math.MaxInt32:
		return batchrepr.Header{}, nil, errors.Errorf("batch invalid: count %d would overflow 32-bit signed int", h.Count)
	}
	return h, batchrepr.Read(repr), nil
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
	header batchrepr.Header
	reader batchrepr.Reader

	// The error encountered during iterator, if any
	err error

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
	h, r, err := decodeBatchHeader(repr)
	if err != nil {
		return nil, err
	}
	return &BatchReader{header: h, reader: r}, nil
}

// Count returns the declared number of entries in the batch.
func (r *BatchReader) Count() int {
	return int(r.header.Count)
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
	return rangekey.Decode(pebble.MakeInternalKey(r.key, 0 /* seqNum */, r.kind), r.value, nil)
}

// Next advances to the next entry in the batch, returning false when the batch
// is empty.
func (r *BatchReader) Next() bool {
	var ok bool
	r.kind, r.key, r.value, ok, r.err = r.reader.Next()
	return ok
}

// BatchCount provides an efficient way to get the count of mutations in a batch
// representation.
func BatchCount(repr []byte) (int, error) {
	h, ok := batchrepr.ReadHeader(repr)
	if !ok {
		return 0, errors.Errorf("invalid batch: batch repr too small: %d bytes", len(repr))
	}
	return int(h.Count), nil
}
