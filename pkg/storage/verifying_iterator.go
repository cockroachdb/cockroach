// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import "github.com/cockroachdb/pebble"

// verifyingMVCCIterator is an MVCC iterator that wraps a pebbleIterator and
// verifies roachpb.Value checksums for encountered values.
type verifyingMVCCIterator struct {
	*pebbleIterator // concrete type to avoid dynamic dispatch

	valid    bool
	err      error
	key      MVCCKey
	value    []byte
	hasPoint bool
	hasRange bool
}

// newVerifyingMVCCIterator creates a new VerifyingMVCCIterator.
func newVerifyingMVCCIterator(iter *pebbleIterator) MVCCIterator {
	return &verifyingMVCCIterator{pebbleIterator: iter}
}

// saveAndVerify fetches the current key and value, saves them in the iterator,
// and verifies the value.
func (i *verifyingMVCCIterator) saveAndVerify() {
	if i.valid, i.err = i.pebbleIterator.Valid(); !i.valid || i.err != nil {
		return
	}
	i.key = i.pebbleIterator.UnsafeKey()
	i.hasPoint, i.hasRange = i.pebbleIterator.HasPointAndRange()
	if i.hasPoint {
		i.value, _ = i.pebbleIterator.UnsafeValue()
		if i.key.IsValue() {
			mvccValue, err := decodeMVCCValueIgnoringHeader(i.value)
			if err == nil {
				err = mvccValue.Value.Verify(i.key.Key)
			}
			if err != nil {
				i.err = err
				i.valid = false
				return
			}
		}
	}
}

// Next implements MVCCIterator.
func (i *verifyingMVCCIterator) Next() {
	i.pebbleIterator.Next()
	i.saveAndVerify()
}

// NextKey implements MVCCIterator.
func (i *verifyingMVCCIterator) NextKey() {
	i.pebbleIterator.NextKey()
	i.saveAndVerify()
}

// Prev implements MVCCIterator.
func (i *verifyingMVCCIterator) Prev() {
	i.pebbleIterator.Prev()
	i.saveAndVerify()
}

// SeekGE implements MVCCIterator.
func (i *verifyingMVCCIterator) SeekGE(key MVCCKey) {
	i.pebbleIterator.SeekGE(key)
	i.saveAndVerify()
}

// SeekLT implements MVCCIterator.
func (i *verifyingMVCCIterator) SeekLT(key MVCCKey) {
	i.pebbleIterator.SeekLT(key)
	i.saveAndVerify()
}

// UnsafeKey implements MVCCIterator.
func (i *verifyingMVCCIterator) UnsafeKey() MVCCKey {
	return i.key
}

// UnsafeValue implements MVCCIterator.
func (i *verifyingMVCCIterator) UnsafeValue() ([]byte, error) {
	return i.value, nil
}

// MVCCValueLenAndIsTombstone implements MVCCIterator.
func (i *verifyingMVCCIterator) MVCCValueLenAndIsTombstone() (int, bool, error) {
	isTombstone, err := EncodedMVCCValueIsTombstone(i.value)
	if err != nil {
		return 0, false, err
	}
	return len(i.value), isTombstone, nil
}

// ValueLen implements MVCCIterator.
func (i *verifyingMVCCIterator) ValueLen() int {
	return len(i.value)
}

// Valid implements MVCCIterator.
func (i *verifyingMVCCIterator) Valid() (bool, error) {
	return i.valid, i.err
}

// HasPointAndRange implements MVCCIterator.
func (i *verifyingMVCCIterator) HasPointAndRange() (bool, bool) {
	return i.hasPoint, i.hasRange
}

// UnsafeLazyValue implements MVCCIterator.
func (i *verifyingMVCCIterator) UnsafeLazyValue() pebble.LazyValue {
	return pebble.LazyValue{ValueOrHandle: i.value}
}
