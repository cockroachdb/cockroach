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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// This file defines wrappers for Reader and Writer, and functions to do the
// wrapping, which depend on the configuration settings above.

// intentDemuxWriter implements 3 methods from the Writer interface:
// PutIntent, ClearIntent, ClearMVCCRangeAndIntents.
type intentDemuxWriter struct {
	w Writer
}

func wrapIntentWriter(ctx context.Context, w Writer) intentDemuxWriter {
	idw := intentDemuxWriter{w: w}
	return idw
}

// ClearIntent has the same behavior as Writer.ClearIntent. buf is used as
// scratch-space to avoid allocations -- its contents will be overwritten and
// not appended to, and a possibly different buf returned.
func (idw intentDemuxWriter) ClearIntent(
	key roachpb.Key, txnDidNotUpdateMeta bool, txnUUID uuid.UUID, buf []byte,
) (_ []byte, _ error) {
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
}

// PutIntent has the same behavior as Writer.PutIntent. buf is used as
// scratch-space to avoid allocations -- its contents will be overwritten and
// not appended to, and a possibly different buf returned.
func (idw intentDemuxWriter) PutIntent(
	ctx context.Context, key roachpb.Key, value []byte, txnUUID uuid.UUID, buf []byte,
) (_ []byte, _ error) {
	var engineKey EngineKey
	engineKey, buf = LockTableKey{
		Key:      key,
		Strength: lock.Exclusive,
		TxnUUID:  txnUUID[:],
	}.ToEngineKey(buf)
	return buf, idw.w.PutEngineKey(engineKey, value)
}

// ClearMVCCRangeAndIntents has the same behavior as
// Writer.ClearMVCCRangeAndIntents. buf is used as scratch-space to avoid
// allocations -- its contents will be overwritten and not appended to, and a
// possibly different buf returned.
func (idw intentDemuxWriter) ClearMVCCRangeAndIntents(
	start, end roachpb.Key, buf []byte,
) ([]byte, error) {
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
	// rawMVCCGet is only used for Reader.MVCCGet which is deprecated and not
	// performance sensitive.
	rawMVCCGet(key []byte) (value []byte, err error)
}

// wrapReader wraps the provided reader, to return an implementation of MVCCIterator
// that supports MVCCKeyAndIntentsIterKind.
func wrapReader(r wrappableReader) *intentInterleavingReader {
	iiReader := intentInterleavingReaderPool.Get().(*intentInterleavingReader)
	*iiReader = intentInterleavingReader{wrappableReader: r}
	return iiReader
}

type intentInterleavingReader struct {
	wrappableReader
}

var _ Reader = &intentInterleavingReader{}

var intentInterleavingReaderPool = sync.Pool{
	New: func() interface{} {
		return &intentInterleavingReader{}
	},
}

// Get implements the Reader interface.
func (imr *intentInterleavingReader) MVCCGet(key MVCCKey) ([]byte, error) {
	val, err := imr.wrappableReader.rawMVCCGet(EncodeMVCCKey(key))
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
func (imr *intentInterleavingReader) MVCCGetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	return pebbleGetProto(imr, key, msg)
}

// NewMVCCIterator implements the Reader interface. The
// intentInterleavingReader can be freed once this method returns.
func (imr *intentInterleavingReader) NewMVCCIterator(
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

func (imr *intentInterleavingReader) Free() {
	*imr = intentInterleavingReader{}
	intentInterleavingReaderPool.Put(imr)
}
