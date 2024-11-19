// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/pebbleiter"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/rangekey"
)

var (
	writeBatchPool = sync.Pool{
		New: func() interface{} {
			return &writeBatch{}
		},
	}
	readWriteBatchPool = sync.Pool{
		New: func() interface{} {
			return &pebbleBatch{}
		},
	}
)

// Instantiates a new writeBatch.
func newWriteBatch(
	db *pebble.DB,
	batch *pebble.Batch,
	settings *cluster.Settings,
	parent *Pebble,
	batchStatsReporter batchStatsReporter,
) *writeBatch {
	wb := writeBatchPool.Get().(*writeBatch)
	*wb = writeBatch{
		db:                 db,
		batch:              batch,
		buf:                wb.buf,
		parent:             parent,
		batchStatsReporter: batchStatsReporter,
		settings:           settings,
	}
	return wb
}

// A writeBatch wraps a pebble.Batch, omitting any facilities for reading. It's
// used when only a WriteBatch is needed.
type writeBatch struct {
	db                               *pebble.DB
	batch                            *pebble.Batch
	buf                              []byte
	parent                           *Pebble
	batchStatsReporter               batchStatsReporter
	settings                         *cluster.Settings
	closed                           bool
	shouldWriteLocalTimestamps       bool
	shouldWriteLocalTimestampsCached bool
}

var _ WriteBatch = (*writeBatch)(nil)

type batchStatsReporter interface {
	aggregateBatchCommitStats(stats BatchCommitStats)
}

// ApplyBatchRepr implements the Writer interface.
func (wb *writeBatch) ApplyBatchRepr(repr []byte, sync bool) error {
	var batch pebble.Batch
	if err := batch.SetRepr(repr); err != nil {
		return err
	}
	return wb.batch.Apply(&batch, nil)
}

// ClearMVCC implements the Writer interface.
func (wb *writeBatch) ClearMVCC(key MVCCKey, opts ClearOptions) error {
	if key.Timestamp.IsEmpty() {
		panic("ClearMVCC timestamp is empty")
	}
	return wb.clear(key, opts)
}

// ClearUnversioned implements the Writer interface.
func (wb *writeBatch) ClearUnversioned(key roachpb.Key, opts ClearOptions) error {
	return wb.clear(MVCCKey{Key: key}, opts)
}

// ClearEngineKey implements the Writer interface.
func (wb *writeBatch) ClearEngineKey(key EngineKey, opts ClearOptions) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	wb.buf = key.EncodeToBuf(wb.buf[:0])
	if !opts.ValueSizeKnown {
		return wb.batch.Delete(wb.buf, nil)
	}
	return wb.batch.DeleteSized(wb.buf, opts.ValueSize, nil)
}

// ClearMVCCIteratorRange implements the Batch interface.
func (wb *writeBatch) ClearMVCCIteratorRange(
	start, end roachpb.Key, pointKeys, rangeKeys bool,
) error {
	// TODO(jackson): Remove this method. See the TODO in its definition within
	// the Writer interface.
	panic("batch is write-only")
}

func (wb *writeBatch) clear(key MVCCKey, opts ClearOptions) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	wb.buf = EncodeMVCCKeyToBuf(wb.buf[:0], key)
	if !opts.ValueSizeKnown {
		return wb.batch.Delete(wb.buf, nil)
	}
	return wb.batch.DeleteSized(wb.buf, opts.ValueSize, nil)
}

// SingleClearEngineKey implements the Writer interface.
func (wb *writeBatch) SingleClearEngineKey(key EngineKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	wb.buf = key.EncodeToBuf(wb.buf[:0])
	return wb.batch.SingleDelete(wb.buf, nil)
}

// ClearRawRange implements the Writer interface.
func (wb *writeBatch) ClearRawRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	wb.buf = EngineKey{Key: start}.EncodeToBuf(wb.buf[:0])
	endRaw := EngineKey{Key: end}.Encode()
	if pointKeys {
		if err := wb.batch.DeleteRange(wb.buf, endRaw, pebble.Sync); err != nil {
			return err
		}
	}
	if rangeKeys {
		if err := wb.batch.RangeKeyDelete(wb.buf, endRaw, pebble.Sync); err != nil {
			return err
		}
	}
	return nil
}

// ClearMVCCRange implements the Writer interface.
func (wb *writeBatch) ClearMVCCRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	if err := wb.ClearRawRange(start, end, pointKeys, rangeKeys); err != nil {
		return err
	}
	// The lock table only contains point keys, so only clear it when point keys
	// are requested, and don't clear range keys in it.
	if !pointKeys {
		return nil
	}
	lstart, _ := keys.LockTableSingleKey(start, nil)
	lend, _ := keys.LockTableSingleKey(end, nil)
	return wb.ClearRawRange(lstart, lend, true /* pointKeys */, false /* rangeKeys */)
}

// ClearMVCCVersions implements the Writer interface.
func (wb *writeBatch) ClearMVCCVersions(start, end MVCCKey) error {
	wb.buf = EncodeMVCCKeyToBuf(wb.buf[:0], start)
	return wb.batch.DeleteRange(wb.buf, EncodeMVCCKey(end), nil)
}

// ClearMVCCRangeKey implements the Writer interface.
func (wb *writeBatch) ClearMVCCRangeKey(rangeKey MVCCRangeKey) error {
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	// If the range key holds an encoded timestamp as it was read from storage,
	// write the tombstone to clear it using the same encoding of the timestamp.
	// See #129592.
	if len(rangeKey.EncodedTimestampSuffix) > 0 {
		return wb.ClearEngineRangeKey(
			rangeKey.StartKey, rangeKey.EndKey, rangeKey.EncodedTimestampSuffix)
	}
	return wb.ClearEngineRangeKey(
		rangeKey.StartKey, rangeKey.EndKey, EncodeMVCCTimestampSuffix(rangeKey.Timestamp))
}

// BufferedSize implements the Writer interface.
func (wb *writeBatch) BufferedSize() int {
	return wb.Len()
}

// PutMVCCRangeKey implements the Writer interface.
func (wb *writeBatch) PutMVCCRangeKey(rangeKey MVCCRangeKey, value MVCCValue) error {
	// NB: all MVCC APIs currently assume all range keys are range tombstones.
	if !value.IsTombstone() {
		return errors.New("range keys can only be MVCC range tombstones")
	}
	valueRaw, err := EncodeMVCCValue(value)
	if err != nil {
		return errors.Wrapf(err, "failed to encode MVCC value for range key %s", rangeKey)
	}
	return wb.PutRawMVCCRangeKey(rangeKey, valueRaw)
}

// PutRawMVCCRangeKey implements the Writer interface.
func (wb *writeBatch) PutRawMVCCRangeKey(rangeKey MVCCRangeKey, value []byte) error {
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	// NB: We deliberately do not use rangeKey.EncodedTimestampSuffix even if
	// it's present, because we explicitly do NOT want to write range keys with
	// the synthetic bit set.
	return wb.PutEngineRangeKey(
		rangeKey.StartKey, rangeKey.EndKey, EncodeMVCCTimestampSuffix(rangeKey.Timestamp), value)
}

// PutEngineRangeKey implements the Writer interface.
func (wb *writeBatch) PutEngineRangeKey(start, end roachpb.Key, suffix, value []byte) error {
	return wb.batch.RangeKeySet(
		EngineKey{Key: start}.Encode(), EngineKey{Key: end}.Encode(), suffix, value, nil)
}

// ClearRawEncodedRange implements the InternalWriter interface.
func (wb *writeBatch) ClearRawEncodedRange(start, end []byte) error {
	return wb.batch.DeleteRange(start, end, pebble.Sync)
}

// PutInternalRangeKey implements the InternalWriter interface.
func (wb *writeBatch) PutInternalRangeKey(start, end []byte, key rangekey.Key) error {
	switch key.Kind() {
	case pebble.InternalKeyKindRangeKeyUnset:
		return wb.batch.RangeKeyUnset(start, end, key.Suffix, nil /* writeOptions */)
	case pebble.InternalKeyKindRangeKeySet:
		return wb.batch.RangeKeySet(start, end, key.Suffix, key.Value, nil /* writeOptions */)
	case pebble.InternalKeyKindRangeKeyDelete:
		return wb.batch.RangeKeyDelete(start, end, nil /* writeOptions */)
	default:
		panic("unexpected range key kind")
	}
}

// PutInternalPointKey implements the InternalWriter interface.
func (wb *writeBatch) PutInternalPointKey(key *pebble.InternalKey, value []byte) error {
	if len(key.UserKey) == 0 {
		return emptyKeyError()
	}
	return wb.batch.AddInternalKey(key, value, nil /* writeOptions */)
}

// ClearEngineRangeKey implements the Engine interface.
func (wb *writeBatch) ClearEngineRangeKey(start, end roachpb.Key, suffix []byte) error {
	return wb.batch.RangeKeyUnset(
		EngineKey{Key: start}.Encode(), EngineKey{Key: end}.Encode(), suffix, nil)
}

// Merge implements the Writer interface.
func (wb *writeBatch) Merge(key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	wb.buf = EncodeMVCCKeyToBuf(wb.buf[:0], key)
	return wb.batch.Merge(wb.buf, value, nil)
}

// PutMVCC implements the Writer interface.
func (wb *writeBatch) PutMVCC(key MVCCKey, value MVCCValue) error {
	if key.Timestamp.IsEmpty() {
		panic("PutMVCC timestamp is empty")
	}
	return wb.putMVCC(key, value)
}

// PutRawMVCC implements the Writer interface.
func (wb *writeBatch) PutRawMVCC(key MVCCKey, value []byte) error {
	if key.Timestamp.IsEmpty() {
		panic("PutRawMVCC timestamp is empty")
	}
	return wb.put(key, value)
}

// PutUnversioned implements the Writer interface.
func (wb *writeBatch) PutUnversioned(key roachpb.Key, value []byte) error {
	return wb.put(MVCCKey{Key: key}, value)
}

// PutEngineKey implements the Writer interface.
func (wb *writeBatch) PutEngineKey(key EngineKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	wb.buf = key.EncodeToBuf(wb.buf[:0])
	return wb.batch.Set(wb.buf, value, nil)
}

func (wb *writeBatch) putMVCC(key MVCCKey, value MVCCValue) error {
	// For performance, this method uses the pebble Batch's deferred operation
	// API to avoid an extra memcpy. We:
	// - determine the length of the encoded MVCC key and MVCC value
	// - reserve space in the pebble Batch using SetDeferred
	// - encode the MVCC key and MVCC value directly into the Batch
	// - call Finish on the deferred operation (which will index the key if
	//   wb.batch is indexed)
	valueLen, isExtended := mvccValueSize(value)
	keyLen := encodedMVCCKeyLength(key)
	o := wb.batch.SetDeferred(keyLen, valueLen)
	encodeMVCCKeyToBuf(o.Key, key, keyLen)
	if !isExtended {
		// Fast path; we don't need to use the extended encoding and can copy
		// RawBytes in verbatim.
		copy(o.Value, value.Value.RawBytes)
	} else {
		// Slow path; we need the MVCC value header.
		err := encodeExtendedMVCCValueToSizedBuf(value, o.Value)
		if err != nil {
			return err
		}
	}
	return o.Finish()
}

func (wb *writeBatch) put(key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	keyLen := encodedMVCCKeyLength(key)
	o := wb.batch.SetDeferred(keyLen, len(value))
	encodeMVCCKeyToBuf(o.Key, key, keyLen)
	copy(o.Value, value)
	return o.Finish()
}

// LogData implements the Writer interface.
func (wb *writeBatch) LogData(data []byte) error {
	return wb.batch.LogData(data, nil)
}

func (wb *writeBatch) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op.
}

// Commit implements the WriteBatch interface.
func (wb *writeBatch) Commit(sync bool) error {
	opts := pebble.NoSync
	if sync {
		opts = pebble.Sync
	}
	if wb.batch == nil {
		panic("called with nil batch")
	}
	err := wb.batch.Commit(opts)
	if err != nil {
		// TODO(storage): ensure that these errors are only ever due to invariant
		// violations and never due to unrecoverable Pebble states. Then switch to
		// returning the error instead of panicking.
		//
		// Once we do that, document on the storage.Batch interface the meaning of
		// an error returned from this method and the guarantees that callers have
		// or don't have after they receive an error from this method.
		panic(err)
	}
	wb.batchStatsReporter.aggregateBatchCommitStats(
		BatchCommitStats{wb.batch.CommitStats()})
	return err
}

// CommitNoSyncWait implements the WriteBatch interface.
func (wb *writeBatch) CommitNoSyncWait() error {
	if wb.batch == nil {
		panic("called with nil batch")
	}
	err := wb.db.ApplyNoSyncWait(wb.batch, pebble.Sync)
	if err != nil {
		// TODO(storage): ensure that these errors are only ever due to invariant
		// violations and never due to unrecoverable Pebble states. Then switch to
		// returning the error instead of panicking.
		//
		// Once we do that, document on the storage.Batch interface the meaning of
		// an error returned from this method and the guarantees that callers have
		// or don't have after they receive an error from this method.
		panic(err)
	}
	return err
}

// SyncWait implements the WriteBatch interface.
func (wb *writeBatch) SyncWait() error {
	if wb.batch == nil {
		panic("called with nil batch")
	}
	err := wb.batch.SyncWait()
	if err != nil {
		// TODO(storage): ensure that these errors are only ever due to invariant
		// violations and never due to unrecoverable Pebble states. Then switch to
		// returning the error instead of panicking.
		//
		// Once we do that, document on the storage.Batch interface the meaning of
		// an error returned from this method and the guarantees that callers have
		// or don't have after they receive an error from this method.
		panic(err)
	}
	wb.batchStatsReporter.aggregateBatchCommitStats(
		BatchCommitStats{wb.batch.CommitStats()})
	return err
}

// Empty implements the WriteBatch interface.
func (wb *writeBatch) Empty() bool {
	return wb.batch.Count() == 0
}

// Count implements the WriteBatch interface.
func (wb *writeBatch) Count() uint32 {
	return wb.batch.Count()
}

// Len implements the WriteBatch interface.
func (wb *writeBatch) Len() int {
	return len(wb.batch.Repr())
}

// Repr implements the WriteBatch interface.
func (wb *writeBatch) Repr() []byte {
	// Repr expects a "safe" byte slice as its output. The return value of
	// p.batch.Repr() is an unsafe byte slice owned by p.batch. Since we could be
	// sending this slice over the wire, we need to make a copy.
	repr := wb.batch.Repr()
	reprCopy := make([]byte, len(repr))
	copy(reprCopy, repr)
	return reprCopy
}

// CommitStats implements the WriteBatch interface.
func (wb *writeBatch) CommitStats() BatchCommitStats {
	return BatchCommitStats{BatchCommitStats: wb.batch.CommitStats()}
}

// ShouldWriteLocalTimestamps implements the WriteBatch interface.
func (wb *writeBatch) ShouldWriteLocalTimestamps(ctx context.Context) bool {
	// pebbleBatch is short-lived, so cache the value for performance.
	if !wb.shouldWriteLocalTimestampsCached {
		wb.shouldWriteLocalTimestamps = shouldWriteLocalTimestamps(ctx, wb.settings)
		wb.shouldWriteLocalTimestampsCached = true
	}
	return wb.shouldWriteLocalTimestamps
}

// Close implements the WriteBatch interface.
func (wb *writeBatch) Close() {
	wb.close()
	writeBatchPool.Put(wb)
}

func (wb *writeBatch) close() {
	if wb.closed {
		panic("closing an already-closed writeBatch")
	}
	wb.closed = true
	_ = wb.batch.Close()
	wb.batch = nil
}

// Wrapper struct around a pebble.Batch.
type pebbleBatch struct {
	writeBatch
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

	iter     pebbleiter.Iterator
	iterUsed bool // avoids cloning after PinEngineStateForIterators()
}

var _ Batch = (*pebbleBatch)(nil)

// Instantiates a new pebbleBatch.
func newPebbleBatch(
	db *pebble.DB,
	batch *pebble.Batch,
	settings *cluster.Settings,
	parent *Pebble,
	batchStatsReporter batchStatsReporter,
) *pebbleBatch {
	pb := readWriteBatchPool.Get().(*pebbleBatch)
	*pb = pebbleBatch{
		writeBatch: writeBatch{
			db:                 db,
			batch:              batch,
			buf:                pb.buf,
			parent:             parent,
			batchStatsReporter: batchStatsReporter,
			settings:           settings,
		},
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
	}
	return pb
}

// Close implements the Batch interface.
func (p *pebbleBatch) Close() {
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
	p.writeBatch.close()
	readWriteBatchPool.Put(p)
}

// Closed implements the Batch interface.
func (p *pebbleBatch) Closed() bool {
	return p.closed
}

// MVCCIterate implements the Batch interface.
func (p *pebbleBatch) MVCCIterate(
	ctx context.Context,
	start, end roachpb.Key,
	iterKind MVCCIterKind,
	keyTypes IterKeyType,
	readCategory fs.ReadCategory,
	f func(MVCCKeyValue, MVCCRangeKeyStack) error,
) error {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		err := iterateOnReader(ctx, r, start, end, iterKind, keyTypes, readCategory, f)
		r.Free()
		return err
	}
	return iterateOnReader(ctx, p, start, end, iterKind, keyTypes, readCategory, f)
}

// NewMVCCIterator implements the Batch interface.
func (p *pebbleBatch) NewMVCCIterator(
	ctx context.Context, iterKind MVCCIterKind, opts IterOptions,
) (MVCCIterator, error) {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		iter, err := r.NewMVCCIterator(ctx, iterKind, opts)
		r.Free()
		if err != nil {
			return nil, err
		}
		return maybeWrapInUnsafeIter(iter), nil
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
		return newPebbleIteratorByCloning(ctx, CloneContext{
			rawIter: p.iter,
			engine:  p.parent,
		}, opts, StandardDurability), nil
	}

	if iter.iter != nil {
		iter.setOptions(ctx, opts, StandardDurability)
	} else {
		if err := iter.initReuseOrCreate(
			ctx, handle, p.iter, p.iterUsed, opts, StandardDurability, p.parent); err != nil {
			return nil, err
		}
		if p.iter == nil {
			// For future cloning.
			p.iter = iter.iter
		}
		p.iterUsed = true
	}

	iter.inuse = true
	return maybeWrapInUnsafeIter(iter), nil
}

// NewBatchOnlyMVCCIterator implements the Batch interface.
func (p *pebbleBatch) NewBatchOnlyMVCCIterator(
	ctx context.Context, opts IterOptions,
) (MVCCIterator, error) {
	if !p.batch.Indexed() {
		panic("unindexed batch")
	}
	var err error
	iter := pebbleIterPool.Get().(*pebbleIterator)
	iter.reusable = false // defensive
	iter.init(ctx, nil, opts, StandardDurability, p.parent)
	boIter, err := p.batch.NewBatchOnlyIter(ctx, &iter.options)
	if err != nil {
		iter.Close()
		panic(err)
	}
	iter.iter = pebbleiter.MaybeWrap(boIter)
	return iter, nil
}

// NewEngineIterator implements the Batch interface.
func (p *pebbleBatch) NewEngineIterator(
	ctx context.Context, opts IterOptions,
) (EngineIterator, error) {
	iter := &p.normalEngineIter
	if opts.Prefix {
		iter = &p.prefixEngineIter
	}
	handle := pebble.Reader(p.batch)
	if !p.batch.Indexed() {
		handle = p.db
	}
	if iter.inuse {
		return newPebbleIteratorByCloning(ctx, CloneContext{
			rawIter: p.iter,
			engine:  p.parent,
		}, opts, StandardDurability), nil
	}

	if iter.iter != nil {
		iter.setOptions(ctx, opts, StandardDurability)
	} else {
		if err := iter.initReuseOrCreate(
			ctx, handle, p.iter, p.iterUsed, opts, StandardDurability, p.parent); err != nil {
			return nil, err
		}
		if p.iter == nil {
			// For future cloning.
			p.iter = iter.iter
		}
		p.iterUsed = true
	}

	iter.inuse = true
	return iter, nil
}

// ScanInternal implements the Reader interface.
func (p *pebbleBatch) ScanInternal(
	ctx context.Context,
	lower, upper roachpb.Key,
	visitPointKey func(key *pebble.InternalKey, value pebble.LazyValue, info pebble.IteratorLevel) error,
	visitRangeDel func(start []byte, end []byte, seqNum pebble.SeqNum) error,
	visitRangeKey func(start []byte, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
	visitExternalFile func(sst *pebble.ExternalFile) error,
) error {
	panic("ScanInternal only supported on Engine and Snapshot.")
}

// ConsistentIterators implements the Batch interface.
func (p *pebbleBatch) ConsistentIterators() bool {
	return true
}

// PinEngineStateForIterators implements the Batch interface.
func (p *pebbleBatch) PinEngineStateForIterators(readCategory fs.ReadCategory) error {
	var err error
	if p.iter == nil {
		var iter *pebble.Iterator
		o := &pebble.IterOptions{Category: readCategory.PebbleCategory()}
		if p.batch.Indexed() {
			iter, err = p.batch.NewIter(o)
		} else {
			iter, err = p.db.NewIter(o)
		}
		if err != nil {
			return err
		}
		p.iter = pebbleiter.MaybeWrap(iter)
		// NB: p.iterUsed == false avoids cloning this in NewMVCCIterator(). We've
		// just created it, so cloning it would just be overhead.
	}
	return nil
}

// ClearMVCCIteratorRange implements the Batch interface.
func (p *pebbleBatch) ClearMVCCIteratorRange(
	start, end roachpb.Key, pointKeys, rangeKeys bool,
) error {
	clearPointKeys := func(start, end roachpb.Key) error {
		iter, err := p.NewMVCCIterator(context.Background(), MVCCKeyAndIntentsIterKind, IterOptions{
			KeyTypes:   IterKeyTypePointsOnly,
			LowerBound: start,
			UpperBound: end,
		})
		if err != nil {
			return err
		}
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
		return nil
	}

	if pointKeys {
		if err := clearPointKeys(start, end); err != nil {
			return err
		}
	}

	clearRangeKeys := func(start, end roachpb.Key) error {
		iter, err := p.NewMVCCIterator(context.Background(), MVCCKeyIterKind, IterOptions{
			KeyTypes:   IterKeyTypeRangesOnly,
			LowerBound: start,
			UpperBound: end,
		})
		if err != nil {
			return err
		}
		defer iter.Close()
		for iter.SeekGE(MVCCKey{Key: start}); ; iter.Next() {
			if valid, err := iter.Valid(); err != nil {
				return err
			} else if !valid {
				break
			}
			// TODO(erikgrinaker): We should consider reusing a buffer for the
			// encoding here, but we don't expect to see many range keys.
			rangeKeys := iter.RangeKeys()
			startRaw := EncodeMVCCKey(MVCCKey{Key: rangeKeys.Bounds.Key})
			endRaw := EncodeMVCCKey(MVCCKey{Key: rangeKeys.Bounds.EndKey})
			for _, v := range rangeKeys.Versions {
				if err := p.batch.RangeKeyUnset(startRaw, endRaw, v.EncodedTimestampSuffix, nil); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if rangeKeys {
		if err := clearRangeKeys(start, end); err != nil {
			return err
		}
	}
	return nil
}
