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
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// SeparatedIntentsEnabled controls whether separated intents are written. A
// true setting is also gated on clusterversion.SeparatedIntents. After all
// nodes in a cluster are at or beyond clusterversion.SeparatedIntents,
// different nodes will see the version state transition at different times.
// Even nodes that have not yet seen the transition need to be able to read
// separated intents and to write over separated intents (due to a lease
// transfer from a node that has seen the transition to one that has not).
// Therefore, the clusterversion and the value of this setting do not affect
// whether intentDemuxWriter or intentInterleavingReader are used. They only
// affect whether intentDemuxWriter will write separated intents. As expected,
// this value can be set to false to disable writing of separated intents.
//
// Currently there is no long-running migration to replace all interleaved
// intents with separated intents, but we expect that when a cluster has been
// running with this flag set to true for some time, most ranges will only
// have separated intents. Similarly, setting this to false will gradually
// cause most ranges to only have interleaved intents.
var SeparatedIntentsEnabled = settings.RegisterBoolSetting(
	"storage.transaction.separated_intents.enabled",
	"if enabled, intents will be written to a separate lock table, instead of being "+
		"interleaved with MVCC values",
	true,
)

// This file defines wrappers for Reader and Writer, and functions to do the
// wrapping, which depend on the configuration settings above.

// intentDemuxWriter implements 3 methods from the Writer interface:
// PutIntent, ClearIntent, ClearMVCCRangeAndIntents.
type intentDemuxWriter struct {
	w Writer
	// Must be non-nil if this intentDemuxWriter is used. We do the checking
	// lazily when methods are called since the clients of intentDemuxWriter
	// initialize it up-front, but don't know if they are being used by code
	// that cares about intents (e.g. a temporary Engine used for disk-spilling
	// during query execution will never read-write intents).
	settings *cluster.Settings
}

func wrapIntentWriter(w Writer, settings *cluster.Settings) intentDemuxWriter {
	return intentDemuxWriter{w: w, settings: settings}
}

// ClearIntent has the same behavior as Writer.ClearIntent. buf is used as
// scratch-space to avoid allocations -- its contents will be overwritten and
// not appended to, and a possibly different buf returned.
func (idw intentDemuxWriter) ClearIntent(
	key roachpb.Key,
	state PrecedingIntentState,
	txnDidNotUpdateMeta bool,
	txnUUID uuid.UUID,
	buf []byte,
) ([]byte, error) {
	if idw.settings == nil {
		return nil, errors.AssertionFailedf("intentDemuxWriter not configured with cluster.Setttings")
	}
	switch state {
	case ExistingIntentInterleaved:
		return buf, idw.w.ClearUnversioned(key)
	case ExistingIntentSeparated:
		var engineKey EngineKey
		engineKey, buf = LockTableKey{
			Key:      key,
			Strength: lock.Exclusive,
			TxnUUID:  txnUUID[:],
		}.ToEngineKey(buf)
		if txnDidNotUpdateMeta {
			return buf, idw.w.SingleClearEngineKey(engineKey)
		}
		return buf, idw.w.ClearEngineKey(engineKey)
	default:
		return buf, errors.AssertionFailedf("ClearIntent: invalid preceding state %d", state)
	}
}

// PutIntent has the same behavior as Writer.PutIntent. buf is used as
// scratch-space to avoid allocations -- its contents will be overwritten and
// not appended to, and a possibly different buf returned.
func (idw intentDemuxWriter) PutIntent(
	ctx context.Context,
	key roachpb.Key,
	value []byte,
	state PrecedingIntentState,
	txnDidNotUpdateMeta bool,
	txnUUID uuid.UUID,
	buf []byte,
) ([]byte, error) {
	if idw.settings == nil {
		return nil, errors.AssertionFailedf("intentDemuxWriter not configured with cluster.Setttings")
	}
	writeSeparatedIntents :=
		idw.settings.Version.IsActive(ctx, clusterversion.SeparatedIntents) &&
			SeparatedIntentsEnabled.Get(&idw.settings.SV)
	var engineKey EngineKey
	if state == ExistingIntentSeparated || writeSeparatedIntents {
		engineKey, buf = LockTableKey{
			Key:      key,
			Strength: lock.Exclusive,
			TxnUUID:  txnUUID[:],
		}.ToEngineKey(buf)
	}
	if state == ExistingIntentSeparated && !writeSeparatedIntents {
		// Switching this intent from separated to interleaved.
		if txnDidNotUpdateMeta {
			if err := idw.w.SingleClearEngineKey(engineKey); err != nil {
				return buf, err
			}
		} else {
			if err := idw.w.ClearEngineKey(engineKey); err != nil {
				return buf, err
			}
		}
	} else if state == ExistingIntentInterleaved && writeSeparatedIntents {
		// Switching this intent from interleaved to separated.
		if err := idw.w.ClearUnversioned(key); err != nil {
			return buf, err
		}
	}
	// Else, staying separated or staying interleaved or there was no preceding
	// intent, so don't need to explicitly clear.

	// Write intent
	if writeSeparatedIntents {
		return buf, idw.w.PutEngineKey(engineKey, value)
	}
	return buf, idw.w.PutUnversioned(key, value)
}

// ClearMVCCRangeAndIntents has the same behavior as
// Writer.ClearMVCCRangeAndIntents. buf is used as scratch-space to avoid
// allocations -- its contents will be overwritten and not appended to, and a
// possibly different buf returned.
func (idw intentDemuxWriter) ClearMVCCRangeAndIntents(
	start, end roachpb.Key, buf []byte,
) ([]byte, error) {
	if idw.settings == nil {
		return nil, errors.AssertionFailedf("intentDemuxWriter not configured with cluster.Setttings")
	}
	err := idw.w.ClearRawRange(start, end)
	if err != nil {
		return buf, err
	}
	lstart, buf := keys.LockTableSingleKey(start, buf)
	lend, _ := keys.LockTableSingleKey(end, nil)
	return buf, idw.w.ClearRawRange(lstart, lend)
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
	if iterKind == MVCCKeyIterKind {
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
	defer iter.Close()
	valid, err := iter.SeekEngineKeyGE(EngineKey{Key: ltKey})
	if !valid || err != nil {
		return nil, err
	}
	val = iter.Value()
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
	if (!opts.MinTimestampHint.IsEmpty() || !opts.MaxTimestampHint.IsEmpty()) &&
		iterKind == MVCCKeyAndIntentsIterKind {
		panic("cannot ask for interleaved intents when specifying timestamp hints")
	}
	if iterKind == MVCCKeyIterKind {
		return imr.wrappableReader.NewMVCCIterator(MVCCKeyIterKind, opts)
	}
	return newIntentInterleavingIterator(imr.wrappableReader, opts)
}
