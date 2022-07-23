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

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// VerifyingMVCCIterator is an MVCC iterator that wraps an arbitrary MVCC
// iterator and verifies roachpb.Value checksums for encountered values.
type VerifyingMVCCIterator struct {
	MVCCIterator

	valid bool
	err   error
	key   MVCCKey
	value []byte
}

// NewVerifyingMVCCIterator creates a new VerifyingMVCCIterator.
func NewVerifyingMVCCIterator(iter MVCCIterator) MVCCIterator {
	return &VerifyingMVCCIterator{MVCCIterator: iter}
}

// saveAndVerify fetches the current key and value, saves them in the iterator,
// and verifies the value.
func (i *VerifyingMVCCIterator) saveAndVerify() {
	if i.valid, i.err = i.MVCCIterator.Valid(); !i.valid || i.err != nil {
		return
	}
	i.key = i.MVCCIterator.UnsafeKey()
	if hasPoint, _ := i.MVCCIterator.HasPointAndRange(); hasPoint {
		i.value = i.MVCCIterator.UnsafeValue()
		if i.key.IsValue() {
			mvccValue, ok, err := tryDecodeSimpleMVCCValue(i.value)
			if !ok && err == nil {
				mvccValue, err = decodeExtendedMVCCValue(i.value)
			}
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
func (i *VerifyingMVCCIterator) Next() {
	i.MVCCIterator.Next()
	i.saveAndVerify()
}

// NextKey implements MVCCIterator.
func (i *VerifyingMVCCIterator) NextKey() {
	i.MVCCIterator.NextKey()
	i.saveAndVerify()
}

// Prev implements MVCCIterator.
func (i *VerifyingMVCCIterator) Prev() {
	i.MVCCIterator.Prev()
	i.saveAndVerify()
}

// SeekGE implements MVCCIterator.
func (i *VerifyingMVCCIterator) SeekGE(key MVCCKey) {
	i.MVCCIterator.SeekGE(key)
	i.saveAndVerify()
}

// SeekIntentGE implements MVCCIterator.
func (i *VerifyingMVCCIterator) SeekIntentGE(key roachpb.Key, txnUUID uuid.UUID) {
	i.MVCCIterator.SeekIntentGE(key, txnUUID)
	i.saveAndVerify()
}

// SeekLT implements MVCCIterator.
func (i *VerifyingMVCCIterator) SeekLT(key MVCCKey) {
	i.MVCCIterator.SeekLT(key)
	i.saveAndVerify()
}

// UnsafeKey implements MVCCIterator.
func (i *VerifyingMVCCIterator) UnsafeKey() MVCCKey {
	return i.key
}

// UnsafeValue implements MVCCIterator.
func (i *VerifyingMVCCIterator) UnsafeValue() []byte {
	return i.value
}

// Valid implements MVCCIterator.
func (i *VerifyingMVCCIterator) Valid() (bool, error) {
	return i.valid, i.err
}

// HasPointAndRange implements MVCCIterator.
func (i *VerifyingMVCCIterator) HasPointAndRange() (bool, bool) {
	return i.MVCCIterator.HasPointAndRange()
}
