// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// Wrapper struct around a pebble.Batch.
type pebbleBatch struct {
	db    *pebble.DB
	batch *pebble.Batch
	buf   []byte
	// The iterator reuse optimization in pebbleBatch is for servicing a
	// BatchRequest, such that the iterators get reused across different
	// requests in the batch.
	// Reuse iterators for {normal,prefix} x {MVCCKey,EngineKey} iteration. We
	// need separate iterators for EngineKey and MVCCKey iteration since
	// iterators that make separated locks/intents look as interleaved need to
	// use both simultaneously.
	// When the first iterator is initialized, or when
	// PinEngineStateForIterators is called (whichever happens first), the
	// underlying *pebble.Iterator is stashed in iter, so that subsequent
	// iterator initialization can use Iterator.Clone to use the same underlying
	// engine state. This relies on the fact that all pebbleIterators created
	// here are marked as reusable, which causes pebbleIterator.Close to not
	// close iter. iter will be closed when pebbleBatch.Close is called.
	prefixIter       pebbleIterator
	normalIter       pebbleIterator
	prefixEngineIter pebbleIterator
	normalEngineIter pebbleIterator

	iter              *pebble.Iterator
	iterUsed          bool // avoids cloning after PinEngineStateForIterators()
	writeOnly         bool
	containsRangeKeys bool
	closed            bool

	wrappedIntentWriter intentDemuxWriter
	// scratch space for wrappedIntentWriter.
	scratch []byte

	settings                         *cluster.Settings
	shouldWriteLocalTimestamps       bool
	shouldWriteLocalTimestampsCached bool
}

var _ Batch = &pebbleBatch{}

var pebbleBatchPool = sync.Pool{
	New: func() interface{} {
		return &pebbleBatch{}
	},
}

// Instantiates a new pebbleBatch.
func newPebbleBatch(
	db *pebble.DB, batch *pebble.Batch, writeOnly bool, settings *cluster.Settings,
) *pebbleBatch {
	pb := pebbleBatchPool.Get().(*pebbleBatch)
	*pb = pebbleBatch{
		db:    db,
		batch: batch,
		buf:   pb.buf,
		prefixIter: pebbleIterator{
			lowerBoundBuf: pb.prefixIter.lowerBoundBuf,
			upperBoundBuf: pb.prefixIter.upperBoundBuf,
			reusable:      true,
		},
		normalIter: pebbleIterator{
			lowerBoundBuf: pb.normalIter.lowerBoundBuf,
			upperBoundBuf: pb.normalIter.upperBoundBuf,
			reusable:      true,
		},
		prefixEngineIter: pebbleIterator{
			lowerBoundBuf: pb.prefixEngineIter.lowerBoundBuf,
			upperBoundBuf: pb.prefixEngineIter.upperBoundBuf,
			reusable:      true,
		},
		normalEngineIter: pebbleIterator{
			lowerBoundBuf: pb.normalEngineIter.lowerBoundBuf,
			upperBoundBuf: pb.normalEngineIter.upperBoundBuf,
			reusable:      true,
		},
		writeOnly: writeOnly,
		settings:  settings,
	}
	pb.wrappedIntentWriter = wrapIntentWriter(pb)
	return pb
}

// Close implements the Batch interface.
func (p *pebbleBatch) Close() {
	if p.closed {
		panic("closing an already-closed pebbleBatch")
	}
	p.closed = true

	if p.iter != nil && !p.iterUsed {
		if err := p.iter.Close(); err != nil {
			panic(err)
		}
	}

	// Setting iter to nil is sufficient since it will be closed by one of the
	// subsequent destroy calls.
	p.iter = nil
	// Destroy the iterators before closing the batch.
	p.prefixIter.destroy()
	p.normalIter.destroy()
	p.prefixEngineIter.destroy()
	p.normalEngineIter.destroy()

	_ = p.batch.Close()
	p.batch = nil

	pebbleBatchPool.Put(p)
}

// Closed implements the Batch interface.
func (p *pebbleBatch) Closed() bool {
	return p.closed
}

// Get implements the Batch interface.
func (p *pebbleBatch) MVCCGet(key MVCCKey) ([]byte, error) {
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}
	r := wrapReader(p)
	// Doing defer r.Free() does not inline.
	v, err := r.MVCCGet(key)
	r.Free()
	return v, err
}

func (p *pebbleBatch) rawMVCCGet(key []byte) ([]byte, error) {
	r := pebble.Reader(p.batch)
	if p.writeOnly {
		panic("write-only batch")
	}
	if !p.batch.Indexed() {
		r = p.db
	}

	ret, closer, err := r.Get(key)
	if closer != nil {
		retCopy := make([]byte, len(ret))
		copy(retCopy, ret)
		ret = retCopy
		closer.Close()
	}
	if errors.Is(err, pebble.ErrNotFound) || len(ret) == 0 {
		return nil, nil
	}
	return ret, err
}

// MVCCGetProto implements the Batch interface.
func (p *pebbleBatch) MVCCGetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	return pebbleGetProto(p, key, msg)
}

// MVCCIterate implements the Batch interface.
func (p *pebbleBatch) MVCCIterate(
	start, end roachpb.Key, iterKind MVCCIterKind, f func(MVCCKeyValue) error,
) error {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		err := iterateOnReader(r, start, end, iterKind, f)
		r.Free()
		return err
	}
	return iterateOnReader(p, start, end, iterKind, f)
}

// NewMVCCIterator implements the Batch interface.
func (p *pebbleBatch) NewMVCCIterator(iterKind MVCCIterKind, opts IterOptions) MVCCIterator {
	if p.writeOnly {
		panic("write-only batch")
	}

	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		iter := r.NewMVCCIterator(iterKind, opts)
		r.Free()
		if util.RaceEnabled {
			iter = wrapInUnsafeIter(iter)
		}
		return iter
	}

	iter := &p.normalIter
	if opts.Prefix {
		iter = &p.prefixIter
	}
	handle := pebble.Reader(p.batch)
	if !p.batch.Indexed() {
		handle = p.db
	}
	if iter.inuse {
		return newPebbleIteratorByCloning(p.iter, opts, StandardDurability, p.SupportsRangeKeys())
	}

	if iter.iter != nil {
		iter.setOptions(opts, StandardDurability)
	} else {
		iter.initReuseOrCreate(
			handle, p.iter, p.iterUsed, opts, StandardDurability, p.SupportsRangeKeys())
		if p.iter == nil {
			// For future cloning.
			p.iter = iter.iter
		}
		p.iterUsed = true
	}

	iter.inuse = true
	var rv MVCCIterator = iter
	if util.RaceEnabled {
		rv = wrapInUnsafeIter(rv)
	}
	return rv
}

// NewEngineIterator implements the Batch interface.
func (p *pebbleBatch) NewEngineIterator(opts IterOptions) EngineIterator {
	if p.writeOnly {
		panic("write-only batch")
	}

	iter := &p.normalEngineIter
	if opts.Prefix {
		iter = &p.prefixEngineIter
	}
	handle := pebble.Reader(p.batch)
	if !p.batch.Indexed() {
		handle = p.db
	}
	if iter.inuse {
		return newPebbleIteratorByCloning(p.iter, opts, StandardDurability, p.SupportsRangeKeys())
	}

	if iter.iter != nil {
		iter.setOptions(opts, StandardDurability)
	} else {
		iter.initReuseOrCreate(
			handle, p.iter, p.iterUsed, opts, StandardDurability, p.SupportsRangeKeys())
		if p.iter == nil {
			// For future cloning.
			p.iter = iter.iter
		}
		p.iterUsed = true
	}

	iter.inuse = true
	return iter
}

// ConsistentIterators implements the Batch interface.
func (p *pebbleBatch) ConsistentIterators() bool {
	return true
}

// SupportsRangeKeys implements the Batch interface.
func (p *pebbleBatch) SupportsRangeKeys() bool {
	return p.db.FormatMajorVersion() >= pebble.FormatRangeKeys
}

// PinEngineStateForIterators implements the Batch interface.
func (p *pebbleBatch) PinEngineStateForIterators() error {
	if p.iter == nil {
		if p.batch.Indexed() {
			p.iter = p.batch.NewIter(nil)
		} else {
			p.iter = p.db.NewIter(nil)
		}
		// NB: p.iterUsed == false avoids cloning this in NewMVCCIterator(). We've
		// just created it, so cloning it would just be overhead.
	}
	return nil
}

// NewMVCCIterator implements the Batch interface.
func (p *pebbleBatch) ApplyBatchRepr(repr []byte, sync bool) error {
	var batch pebble.Batch
	if err := batch.SetRepr(repr); err != nil {
		return err
	}

	return p.batch.Apply(&batch, nil)
}

// ClearMVCC implements the Batch interface.
func (p *pebbleBatch) ClearMVCC(key MVCCKey) error {
	if key.Timestamp.IsEmpty() {
		panic("ClearMVCC timestamp is empty")
	}
	return p.clear(key)
}

// ClearUnversioned implements the Batch interface.
func (p *pebbleBatch) ClearUnversioned(key roachpb.Key) error {
	return p.clear(MVCCKey{Key: key})
}

// ClearIntent implements the Batch interface.
func (p *pebbleBatch) ClearIntent(
	key roachpb.Key, txnDidNotUpdateMeta bool, txnUUID uuid.UUID,
) error {
	var err error
	p.scratch, err = p.wrappedIntentWriter.ClearIntent(key, txnDidNotUpdateMeta, txnUUID, p.scratch)
	return err
}

// ClearEngineKey implements the Batch interface.
func (p *pebbleBatch) ClearEngineKey(key EngineKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	p.buf = key.EncodeToBuf(p.buf[:0])
	return p.batch.Delete(p.buf, nil)
}

func (p *pebbleBatch) clear(key MVCCKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = EncodeMVCCKeyToBuf(p.buf[:0], key)
	return p.batch.Delete(p.buf, nil)
}

// SingleClearEngineKey implements the Batch interface.
func (p *pebbleBatch) SingleClearEngineKey(key EngineKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = key.EncodeToBuf(p.buf[:0])
	return p.batch.SingleDelete(p.buf, nil)
}

// ClearRawRange implements the Batch interface.
func (p *pebbleBatch) ClearRawRange(start, end roachpb.Key) error {
	p.buf = EncodeMVCCKeyToBuf(p.buf[:0], MVCCKey{Key: start})
	if err := p.batch.DeleteRange(p.buf, EncodeMVCCKey(MVCCKey{Key: end}), nil); err != nil {
		return err
	}
	return p.ClearAllRangeKeys(start, end)
}

// ClearMVCCRange implements the Batch interface.
func (p *pebbleBatch) ClearMVCCRange(start, end roachpb.Key) error {
	var err error
	p.scratch, err = p.wrappedIntentWriter.ClearMVCCRange(start, end, p.scratch)
	return err
}

// ClearMVCCVersions implements the Batch interface.
func (p *pebbleBatch) ClearMVCCVersions(start, end MVCCKey) error {
	p.buf = EncodeMVCCKeyToBuf(p.buf[:0], start)
	return p.batch.DeleteRange(p.buf, EncodeMVCCKey(end), nil)
}

// ClearIterRange implements the Batch interface.
func (p *pebbleBatch) ClearMVCCIteratorRange(start, end roachpb.Key) error {
	iter := p.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
		LowerBound: start,
		UpperBound: end,
	})
	defer iter.Close()

	for iter.SeekGE(MVCCKey{Key: start}); ; iter.Next() {
		if valid, err := iter.Valid(); err != nil {
			return err
		} else if !valid {
			break
		}

		// NB: UnsafeRawKey could be a serialized lock table key, and not just an
		// MVCCKey.
		if err := p.batch.Delete(iter.UnsafeRawKey(), nil); err != nil {
			return err
		}
	}
	return p.ClearAllRangeKeys(start, end)
}

// ClearMVCCRangeKey implements the Engine interface.
func (p *pebbleBatch) ClearMVCCRangeKey(rangeKey MVCCRangeKey) error {
	if !p.SupportsRangeKeys() {
		return nil // noop
	}
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	return p.batch.RangeKeyUnset(
		EncodeMVCCKeyPrefix(rangeKey.StartKey),
		EncodeMVCCKeyPrefix(rangeKey.EndKey),
		EncodeMVCCTimestampSuffix(rangeKey.Timestamp),
		nil)
}

// ClearAllRangeKeys implements the Engine interface.
func (p *pebbleBatch) ClearAllRangeKeys(start, end roachpb.Key) error {
	if !p.SupportsRangeKeys() {
		return nil // noop
	}
	rangeKey := MVCCRangeKey{StartKey: start, EndKey: end, Timestamp: hlc.MinTimestamp}
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	// Look for any range keys in the span, and use the smallest possible span
	// that covers them, to avoid dropping range tombstones across unnecessary
	// spans. We don't worry about races here, because this is a non-MVCC
	// operation where the caller must guarantee appropriate isolation.
	//
	// If we're using an unindexed batch, then we have to read from the database.
	// However, if the unindexed batch itself contains range keys then we can't
	// know where they are, so we have to delete the full span. This seems
	// unlikely to ever happen.
	clearFrom, clearTo := EncodeMVCCKeyPrefix(start), EncodeMVCCKeyPrefix(end)
	if p.batch.Indexed() || !p.containsRangeKeys {
		var err error
		r := pebble.Reader(p.batch)
		if !p.batch.Indexed() {
			r = p.db
		}
		clearFrom, clearTo, err = pebbleFindRangeKeySpan(r, clearFrom, clearTo)
		if err != nil {
			return err
		} else if clearFrom == nil || clearTo == nil {
			return nil
		}
	}
	return p.batch.RangeKeyDelete(clearFrom, clearTo, nil)
}

// PutMVCCRangeKey implements the Batch interface.
func (p *pebbleBatch) PutMVCCRangeKey(rangeKey MVCCRangeKey, value MVCCValue) error {
	// NB: all MVCC APIs currently assume all range keys are range tombstones.
	if !value.IsTombstone() {
		return errors.New("range keys can only be MVCC range tombstones")
	}
	valueRaw, err := EncodeMVCCValue(value)
	if err != nil {
		return errors.Wrapf(err, "failed to encode MVCC value for range key %s", rangeKey)
	}
	return p.PutRawMVCCRangeKey(rangeKey, valueRaw)
}

// PutRawMVCCRangeKey implements the Batch interface.
func (p *pebbleBatch) PutRawMVCCRangeKey(rangeKey MVCCRangeKey, value []byte) error {
	if !p.SupportsRangeKeys() {
		return errors.Errorf("range keys not supported by Pebble database version %s",
			p.db.FormatMajorVersion())
	}
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	if err := p.batch.RangeKeySet(
		EncodeMVCCKeyPrefix(rangeKey.StartKey),
		EncodeMVCCKeyPrefix(rangeKey.EndKey),
		EncodeMVCCTimestampSuffix(rangeKey.Timestamp),
		value,
		nil); err != nil {
		return err
	}
	// Mark the batch as containing range keys. See ClearAllRangeKeys for why.
	p.containsRangeKeys = true
	return nil
}

// PutEngineRangeKey implements the Engine interface.
func (p *pebbleBatch) PutEngineRangeKey(start, end roachpb.Key, suffix, value []byte) error {
	if !p.SupportsRangeKeys() {
		return errors.Errorf("range keys not supported by Pebble database version %s",
			p.db.FormatMajorVersion())
	}
	rangeKey := MVCCRangeKey{StartKey: start, EndKey: end, Timestamp: hlc.MinTimestamp}
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	if err := p.batch.RangeKeySet(
		EngineKey{Key: start}.Encode(),
		EngineKey{Key: end}.Encode(),
		suffix,
		value,
		nil,
	); err != nil {
		return err
	}
	// Mark the batch as containing range keys. See ClearAllRangeKeys for why.
	p.containsRangeKeys = true
	return nil
}

// Merge implements the Batch interface.
func (p *pebbleBatch) Merge(key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = EncodeMVCCKeyToBuf(p.buf[:0], key)
	return p.batch.Merge(p.buf, value, nil)
}

// PutMVCC implements the Batch interface.
func (p *pebbleBatch) PutMVCC(key MVCCKey, value MVCCValue) error {
	if key.Timestamp.IsEmpty() {
		panic("PutMVCC timestamp is empty")
	}
	encValue, err := EncodeMVCCValue(value)
	if err != nil {
		return err
	}
	return p.put(key, encValue)
}

// PutRawMVCC implements the Batch interface.
func (p *pebbleBatch) PutRawMVCC(key MVCCKey, value []byte) error {
	if key.Timestamp.IsEmpty() {
		panic("PutRawMVCC timestamp is empty")
	}
	return p.put(key, value)
}

// PutUnversioned implements the Batch interface.
func (p *pebbleBatch) PutUnversioned(key roachpb.Key, value []byte) error {
	return p.put(MVCCKey{Key: key}, value)
}

// PutIntent implements the Batch interface.
func (p *pebbleBatch) PutIntent(
	ctx context.Context, key roachpb.Key, value []byte, txnUUID uuid.UUID,
) error {
	var err error
	p.scratch, err = p.wrappedIntentWriter.PutIntent(ctx, key, value, txnUUID, p.scratch)
	return err
}

// PutEngineKey implements the Batch interface.
func (p *pebbleBatch) PutEngineKey(key EngineKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = key.EncodeToBuf(p.buf[:0])
	return p.batch.Set(p.buf, value, nil)
}

func (p *pebbleBatch) put(key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = EncodeMVCCKeyToBuf(p.buf[:0], key)
	return p.batch.Set(p.buf, value, nil)
}

// LogData implements the Batch interface.
func (p *pebbleBatch) LogData(data []byte) error {
	return p.batch.LogData(data, nil)
}

func (p *pebbleBatch) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op.
}

// Commit implements the Batch interface.
func (p *pebbleBatch) Commit(sync bool) error {
	opts := pebble.NoSync
	if sync {
		opts = pebble.Sync
	}
	if p.batch == nil {
		panic("called with nil batch")
	}
	err := p.batch.Commit(opts)
	if err != nil {
		panic(err)
	}
	return err
}

// Empty implements the Batch interface.
func (p *pebbleBatch) Empty() bool {
	return p.batch.Count() == 0
}

// Count implements the Batch interface.
func (p *pebbleBatch) Count() uint32 {
	return p.batch.Count()
}

// Len implements the Batch interface.
func (p *pebbleBatch) Len() int {
	return len(p.batch.Repr())
}

// Repr implements the Batch interface.
func (p *pebbleBatch) Repr() []byte {
	// Repr expects a "safe" byte slice as its output. The return value of
	// p.batch.Repr() is an unsafe byte slice owned by p.batch. Since we could be
	// sending this slice over the wire, we need to make a copy.
	repr := p.batch.Repr()
	reprCopy := make([]byte, len(repr))
	copy(reprCopy, repr)
	return reprCopy
}

// ShouldWriteLocalTimestamps implements the Writer interface.
func (p *pebbleBatch) ShouldWriteLocalTimestamps(ctx context.Context) bool {
	// pebbleBatch is short-lived, so cache the value for performance.
	if !p.shouldWriteLocalTimestampsCached {
		p.shouldWriteLocalTimestamps = shouldWriteLocalTimestamps(ctx, p.settings)
		p.shouldWriteLocalTimestampsCached = true
	}
	return p.shouldWriteLocalTimestamps
}
