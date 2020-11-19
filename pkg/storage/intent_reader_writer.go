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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// Configuration information for enabling/disabling separated intents. Eventually, these
// will not be constants defined in this file, and will be externally configurable.
// For now, our goal is make all tests pass with disallowSeparatedIntents=true.
//
// TODO(sumeer): once all integration changes are complete, ensure all tests pass
// with (disallow=false, enabled=false), (disallow=false, enabled=true) and with
// these values changing from one to the other in the middle of a test.
//
// TODO(sumeer): replace these constants with externally configurable values.
//
// State transitions in a running cluster are
// (disallow=true) => (disallow=false, enabled=false) <=> (disallow=false, enabled=true)
//
// The transition to (disallow=false, enabled=false) happens after the cluster
// has transitioned to Pebble permanently AND all nodes are running 21.1. The
// transition to enabled=true can be rolled back.
//
// Eventually the cluster will be finalized in state (disallow=false,
// enabled=true), at which point there is no rollback. Additionally, we will
// force all remaining interleaved intents to be rewritten (these may
// potentially be of committed transactions whose intent resolution did not
// happen yet).
const disallowSeparatedIntents = true
const enabledSeparatedIntents = false

// This file defines wrappers for Reader and Writer, and functions to do the
// wrapping, which depend on the configuration settings above.

// intentDemuxWriter implements 3 methods from the Writer interface:
// PutIntent, ClearIntent, ClearMVCCRangeAndIntents.
type intentDemuxWriter struct {
	w Writer
}

func tryWrapIntentWriter(w Writer) (idw intentDemuxWriter, wrapped bool) {
	if disallowSeparatedIntents {
		return intentDemuxWriter{}, false
	}
	return intentDemuxWriter{w: w}, true
}

func (idw intentDemuxWriter) ClearIntent(
	key roachpb.Key, state PrecedingIntentState, possiblyUpdated bool, txnUUID uuid.UUID,
) error {
	switch state {
	case ExistingIntentInterleaved:
		return idw.w.ClearUnversioned(key)
	case ExistingIntentSeparated:
		engineKey := LockTableKey{
			Key:      key,
			Strength: lock.Exclusive,
			TxnUUID:  txnUUID[:],
		}.ToEngineKey()
		if possiblyUpdated {
			return idw.w.ClearEngineKey(engineKey)
		}
		return idw.w.SingleClearEngineKey(engineKey)
	default:
		return errors.AssertionFailedf("ClearIntent: invalid preceding state %s", state)
	}
}

func (idw intentDemuxWriter) PutIntent(
	key roachpb.Key,
	value []byte,
	state PrecedingIntentState,
	precedingPossiblyUpdated bool,
	txnUUID uuid.UUID,
) error {
	var engineKey EngineKey
	if state == ExistingIntentSeparated || enabledSeparatedIntents {
		engineKey = LockTableKey{
			Key:      key,
			Strength: lock.Exclusive,
			TxnUUID:  txnUUID[:],
		}.ToEngineKey()
	}
	if state == ExistingIntentSeparated && !enabledSeparatedIntents {
		// Switching this intent from separated to to interleaved.
		if precedingPossiblyUpdated {
			if err := idw.w.ClearEngineKey(engineKey); err != nil {
				return err
			}
		} else {
			if err := idw.w.SingleClearEngineKey(engineKey); err != nil {
				return err
			}
		}
	} else if state == ExistingIntentInterleaved && enabledSeparatedIntents {
		// Switching this intent from interleaved to separated.
		if err := idw.w.ClearUnversioned(key); err != nil {
			return err
		}
	}
	// Else, staying separated or staying interleaved or there was no preceding
	// intent, so don't need to explicitly clear.

	// Write intent
	if enabledSeparatedIntents {
		return idw.w.PutEngineKey(engineKey, value)
	}
	return idw.w.PutUnversioned(key, value)
}

func (idw intentDemuxWriter) ClearMVCCRangeAndIntents(start, end roachpb.Key) error {
	err := idw.w.ClearRawRange(start, end)
	if err != nil {
		return err
	}
	lstart, _ := keys.LockTableSingleKey(start, nil)
	lend, _ := keys.LockTableSingleKey(end, nil)
	err = idw.w.ClearRawRange(lstart, lend)
	return err
}

// wrappableReader is used to implement a wrapped Reader. A wrapped Reader
// should be used and immediately discarded. It maintains no state of its own
// between calls.
// Why do we not keep the wrapped reader as a member in the caller? Because
// different methods on Reader can need different wrappings depending on what
// they want to observe.
//
// TODO(sumeer): for allocation optimization we could expose a scratch space
// struct that the caller keeps on behalf of the wrapped reader. But can only
// do such an optimization when know that the wrappableReader will be used
// with external synchronization that prevents preallocated buffers from being
// modified concurrently. pebbleBatch.{MVCCGet,MVCCGetProto} have MVCCKey
// serialization allocation optimizations which we can't do below. But those
// are probably not performance sensitive, since the performance sensitive
// code probably uses an MVCCIterator.
type wrappableReader interface {
	Reader
	rawGet(key []byte) (value []byte, err error)
}

func tryWrapReader(r wrappableReader, iterKind MVCCIterKind) (reader Reader, wrapped bool) {
	if disallowSeparatedIntents || iterKind == MVCCKeyIterKind {
		return r, false
	}
	return intentInterleavingReader{wrappableReader: r}, true
}

type intentInterleavingReader struct {
	wrappableReader
}

var _ Reader = intentInterleavingReader{}

// Get implements the Reader interface.
func (imr intentInterleavingReader) MVCCGet(key MVCCKey) ([]byte, error) {
	val, err := imr.wrappableReader.rawGet(EncodeKey(key))
	if val != nil || err != nil || !key.Timestamp.IsEmpty() {
		return val, err
	}
	// The meta could be in the lock table. Constructing an Iterator for each
	// Get is not efficient, but this function is deprecated and only used for
	// tests, so we don't care.
	ltKey, _ := keys.LockTableSingleKey(key.Key, nil)
	iter := imr.wrappableReader.NewEngineIterator(IterOptions{Prefix: true, LowerBound: ltKey})
	valid, err := iter.SeekEngineKeyGE(EngineKey{Key: ltKey})
	if !valid || err != nil {
		return nil, err
	}
	val = iter.Value()
	iter.Close()
	return val, nil
}

// MVCCGetProto implements the Reader interface.
func (imr intentInterleavingReader) MVCCGetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	return pebbleGetProto(imr, key, msg)
}

// NewMVCCIterator implements the Reader interface.
func (imr intentInterleavingReader) NewMVCCIterator(
	iterKind MVCCIterKind, opts IterOptions,
) MVCCIterator {
	if iterKind != MVCCKeyAndIntentsIterKind {
		panic("intentInterleavingReader should only be used with MVCCKeyAndIntentsIterKing")
	}
	return newIntentInterleavingIterator(imr.wrappableReader, opts)
}
