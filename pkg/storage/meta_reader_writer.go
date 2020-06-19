// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// Configuration information used here to decide how to wrap Reader and Writer.
// For now, our goal is make all tests pass with (disallow: true).
//
// TODO(sumeer): make this externally configurable.
//
// State transitions are
// (disallow: true) => (disallow: false, enabled: false) <=> (disallow: false, enabled: true)
// The transition to (disallow: false, enabled: false) happens after the cluster has transitioned to
// Pebble permanently. The transition to enabled: true can be rolled back.
//
// Eventually the cluster will be finalized in state (disallow: false, enabled: true), at
// which point there is no rollback. Additionally, we will need to force all remaining
// interleaved intents to be rewritten (these may potentially be of committed transactions
// whose intent resolution did not happen yet).
const disallowSeparatedIntents = true
const enabledSeparatedIntents = false

// Intent writer interfaces and implementations.
//
// disallow: true: no wrappedIntentWriter.
// disallow: false: wrap in intentDemuxWriter.

// A wrappedIntentWriter uses a wrappableIntentWriter to access lower-level
// functions.
type wrappableIntentWriter interface {
	Writer
	clearStorageKey(key StorageKey) error
	singleClearStorageKey(key StorageKey) error
}

// These functions in wrappedIntentWriter match those that are part of Writer.
type wrappedIntentWriter interface {
	ClearMVCCMeta(
		key roachpb.Key, state PrecedingIntentState, possiblyUpdated bool, txnUUID uuid.UUID) error
	PutMVCCMeta(
		key roachpb.Key, value []byte, state PrecedingIntentState, possiblyUpdated bool,
		txnUUID uuid.UUID) error
	ClearMVCCRangeAndIntents(start, end roachpb.Key) error
}

func possiblyMakeWrappedIntentWriter(w wrappableIntentWriter) wrappedIntentWriter {
	if disallowSeparatedIntents {
		return nil
	}
	return intentDemuxWriter{w: w}
}

type intentDemuxWriter struct {
	w wrappableIntentWriter
}

var _ wrappedIntentWriter = intentDemuxWriter{}

func (idw intentDemuxWriter) ClearMVCCMeta(
	key roachpb.Key, state PrecedingIntentState, possiblyUpdated bool, txnUUID uuid.UUID,
) error {
	switch state {
	case ExistingIntentInterleaved:
		return idw.w.ClearKeyWithEmptyTimestamp(key)
	case ExistingIntentSeparated:
		ltKey := StorageKey{
			Key:    keys.LockTableKeyExclusive(key),
			Suffix: txnUUID[:],
		}
		if possiblyUpdated {
			return idw.w.clearStorageKey(ltKey)
		}
		return idw.w.singleClearStorageKey(ltKey)
	default:
		panic("bug: caller used invalid state")
	}
}

func (idw intentDemuxWriter) PutMVCCMeta(
	key roachpb.Key,
	value []byte,
	state PrecedingIntentState,
	precedingPossiblyUpdated bool,
	txnUUID uuid.UUID,
) error {
	var ltKey StorageKey
	if state == ExistingIntentSeparated || enabledSeparatedIntents {
		ltKey = StorageKey{
			Key:    keys.LockTableKeyExclusive(key),
			Suffix: txnUUID[:],
		}
	}
	if state == ExistingIntentSeparated && !enabledSeparatedIntents {
		// Switching this intent to be interleaved.
		if precedingPossiblyUpdated {
			if err := idw.w.clearStorageKey(ltKey); err != nil {
				return err
			}
		} else {
			if err := idw.w.singleClearStorageKey(ltKey); err != nil {
				return err
			}
		}
	} else if state == ExistingIntentInterleaved && enabledSeparatedIntents {
		// Switching this intent to be separate.
		if err := idw.w.ClearKeyWithEmptyTimestamp(key); err != nil {
			return err
		}
	}
	// Write intent
	if enabledSeparatedIntents {
		return idw.w.PutStorage(ltKey, value)
	}
	return idw.w.PutKeyWithEmptyTimestamp(key, value)
}

func (idw intentDemuxWriter) ClearMVCCRangeAndIntents(start, end roachpb.Key) error {
	err := idw.w.ClearNonMVCCRange(start, end)
	if err != nil {
		return err
	}
	err = idw.w.ClearNonMVCCRange(keys.MakeLockTableKeyPrefix(start),
		keys.MakeLockTableKeyPrefix(end))
	return err
}

// wrappableReader is used to implement a wrapped Reader. A wrapped Reader
// should be used and immediately discarded. It maintains no state of its own
// between calls.
// Why do we not keep the wrapped reader as a member in the caller? Because
// different methods on Reader can need different wrappings depending on what
// they want to observe.
//
// TODO(sumeer): for allocation optimization we could expose a scratch
// space struct that the caller keeps on behalf of the wrapped reader. But
// can only do such an optimization when know that the wrappableReader will
// be used with external synchronization that prevents preallocated buffers
// from being modified concurrently. pebbleBatc.{Get,GetProto} have MVCCKey
// serialization allocation optimizations which we can't do below. But those
// are probably not performance sensitive.
type wrappableReader interface {
	Reader
	rawGet(key []byte) (value []byte, err error)
	// Only MVCCKeyIterKind and StorageKeyIterKind are permitted.
	// This iterator will not make separated locks appear as interleaved.
	newIterator(opts IterOptions, iterKind IterKind) Iterator
}

func possiblyWrapReader(r wrappableReader, iterKind IterKind) (Reader, bool) {
	if disallowSeparatedIntents || iterKind == MVCCKeyIterKind || iterKind == StorageKeyIterKind {
		return r, false
	}
	if iterKind != StorageKeyIterKind {
		panic("unknown iterKind")
	}
	return interleaveSeparatedMetaReader{wrappableReader: r}, true
}

type interleaveSeparatedMetaReader struct {
	wrappableReader
}

var _ Reader = interleaveSeparatedMetaReader{}

// Get implements the Reader interface.
func (imr interleaveSeparatedMetaReader) Get(key MVCCKey) ([]byte, error) {
	val, err := imr.wrappableReader.rawGet(EncodeMVCCKey(key))
	if val != nil || err != nil || key.Timestamp != (hlc.Timestamp{}) {
		return val, err
	}
	// The meta could be in the lock table. Constructing an Iterator for each
	// Get is not efficient, but this function is deprecated and only used for
	// tests, so we don't care.
	iter := imr.wrappableReader.newIterator(
		IterOptions{Prefix: true, LowerBound: key.Key}, StorageKeyIterKind)
	iter.SeekStorageGE(StorageKey{Key: key.Key})
	valid, err := iter.Valid()
	if !valid || err != nil {
		return nil, err
	}
	val = iter.Value()
	iter.Close()
	return val, nil
}

func (imr interleaveSeparatedMetaReader) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	return pebbleGetProto(imr, key, msg)
}

func (imr interleaveSeparatedMetaReader) NewIterator(opts IterOptions, iterKind IterKind) Iterator {
	if !opts.MinTimestampHint.IsEmpty() || !opts.MaxTimestampHint.IsEmpty() {
		// Creating a time bound iterator, so don't need to see intents.
		if iterKind == MVCCKeyAndIntentsIterKind {
			iterKind = MVCCKeyIterKind
		}
		return imr.wrappableReader.newIterator(opts, iterKind)
	}
	return newIntentInterleavingIterator(imr.wrappableReader, opts)
}
