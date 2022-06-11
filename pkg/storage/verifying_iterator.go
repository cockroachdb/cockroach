// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

// VerifyingMVCCIterator is an MVCC iterator that wraps an arbitrary MVCC
// iterator and verifies roachpb.Value checksums for encountered values.
type VerifyingMVCCIterator struct {
	SimpleMVCCIterator

	valid bool
	err   error
	key   MVCCKey
	value []byte
}

// NewVerifyingMVCCIterator creates a new VerifyingMVCCIterator.
func NewVerifyingMVCCIterator(iter SimpleMVCCIterator) SimpleMVCCIterator {
	return &VerifyingMVCCIterator{SimpleMVCCIterator: iter}
}

// Next implements SimpleMVCCIterator.
func (i *VerifyingMVCCIterator) Next() {
	i.SimpleMVCCIterator.Next()
	i.saveAndVerify()
}

// NextKey implements SimpleMVCCIterator.
func (i *VerifyingMVCCIterator) NextKey() {
	i.SimpleMVCCIterator.NextKey()
	i.saveAndVerify()
}

// SeekGE implements SimpleMVCCIterator.
func (i *VerifyingMVCCIterator) SeekGE(key MVCCKey) {
	i.SimpleMVCCIterator.SeekGE(key)
	i.saveAndVerify()
}

func (i *VerifyingMVCCIterator) UnsafeKey() MVCCKey {
	return i.key
}

func (i *VerifyingMVCCIterator) UnsafeValue() []byte {
	return i.value
}

// Valid implements SimpleMVCCIterator.
func (i *VerifyingMVCCIterator) Valid() (bool, error) {
	return i.valid, i.err
}

// saveAndVerify fetches the current key and value, saves them in the iterator,
// and verifies the value.
func (i *VerifyingMVCCIterator) saveAndVerify() {
	if ok, err := i.SimpleMVCCIterator.Valid(); !ok || err != nil {
		i.valid = false
		i.err = err
		i.key = MVCCKey{}
		i.value = nil
		return
	}
	key := i.SimpleMVCCIterator.UnsafeKey()
	value := i.SimpleMVCCIterator.UnsafeValue()
	if i.key.IsValue() {
		mvccValue, ok, err := tryDecodeSimpleMVCCValue(value)
		if !ok && err == nil {
			mvccValue, err = decodeExtendedMVCCValue(value)
		}
		if err == nil {
			err = mvccValue.Value.Verify(key.Key)
		}
		if err != nil {
			i.err = err
			i.valid = false
			return
		}
	}
	i.key = key
	i.value = value
	i.valid = true
	i.err = nil
}
