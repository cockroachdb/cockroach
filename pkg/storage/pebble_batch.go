// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/pebbleiter"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/rangekey"
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

	iter      pebbleiter.Iterator
	iterUsed  bool // avoids cloning after PinEngineStateForIterators()
	writeOnly bool
	closed    bool

	parent                           *Pebble
	batchStatsReporter               batchStatsReporter
	settings                         *cluster.Settings
	mayWriteSizedDeletes             bool
	shouldWriteLocalTimestamps       bool
	shouldWriteLocalTimestampsCached bool
}

var _ Batch = &pebbleBatch{}

var pebbleBatchPool = sync.Pool{
	New: func() interface{} {
		return &pebbleBatch{}
	},
}

type batchStatsReporter interface {
	aggregateBatchCommitStats(stats BatchCommitStats)
}

// Instantiates a new pebbleBatch.
func newPebbleBatch(
	db *pebble.DB,
	batch *pebble.Batch,
	writeOnly bool,
	settings *cluster.Settings,
	parent *Pebble,
	batchStatsReporter batchStatsReporter,
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
		writeOnly:          writeOnly,
		parent:             parent,
		batchStatsReporter: batchStatsReporter,
		settings:           settings,
		// NB: We do not use settings.Version.IsActive because we do not
		// generally have a guarantee that the cluster version has been
		// initialized. As a part of initializing a store, we use a Batch to
		// write the store identifer key; this is written before any cluster
		// version has been initialized.
		mayWriteSizedDeletes: settings.Version.ActiveVersionOrEmpty(context.TODO()).
			IsActive(clusterversion.V23_2_UseSizedPebblePointTombstones),
	}
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

// MVCCIterate implements the Batch interface.
func (p *pebbleBatch) MVCCIterate(
	start, end roachpb.Key,
	iterKind MVCCIterKind,
	keyTypes IterKeyType,
	f func(MVCCKeyValue, MVCCRangeKeyStack) error,
) error {
	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		err := iterateOnReader(r, start, end, iterKind, keyTypes, f)
		r.Free()
		return err
	}
	return iterateOnReader(p, start, end, iterKind, keyTypes, f)
}

// NewMVCCIterator implements the Batch interface.
func (p *pebbleBatch) NewMVCCIterator(
	iterKind MVCCIterKind, opts IterOptions,
) (MVCCIterator, error) {
	if p.writeOnly {
		panic("write-only batch")
	}

	if iterKind == MVCCKeyAndIntentsIterKind {
		r := wrapReader(p)
		// Doing defer r.Free() does not inline.
		iter, err := r.NewMVCCIterator(iterKind, opts)
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
		return newPebbleIteratorByCloning(CloneContext{
			rawIter: p.iter,
			engine:  p.parent,
		}, opts, StandardDurability), nil
	}

	if iter.iter != nil {
		iter.setOptions(opts, StandardDurability)
	} else {
		if err := iter.initReuseOrCreate(handle, p.iter, p.iterUsed, opts, StandardDurability, p.parent); err != nil {
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

// NewEngineIterator implements the Batch interface.
func (p *pebbleBatch) NewEngineIterator(opts IterOptions) (EngineIterator, error) {
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
		return newPebbleIteratorByCloning(CloneContext{
			rawIter: p.iter,
			engine:  p.parent,
		}, opts, StandardDurability), nil
	}

	if iter.iter != nil {
		iter.setOptions(opts, StandardDurability)
	} else {
		if err := iter.initReuseOrCreate(handle, p.iter, p.iterUsed, opts, StandardDurability, p.parent); err != nil {
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
	visitRangeDel func(start []byte, end []byte, seqNum uint64) error,
	visitRangeKey func(start []byte, end []byte, keys []rangekey.Key) error,
	visitSharedFile func(sst *pebble.SharedSSTMeta) error,
) error {
	panic("ScanInternal only supported on Engine and Snapshot.")
}

// ClearRawEncodedRange implements the InternalWriter interface.
func (p *pebbleBatch) ClearRawEncodedRange(start, end []byte) error {
	return p.batch.DeleteRange(start, end, pebble.Sync)
}

// ConsistentIterators implements the Batch interface.
func (p *pebbleBatch) ConsistentIterators() bool {
	return true
}

// PinEngineStateForIterators implements the Batch interface.
func (p *pebbleBatch) PinEngineStateForIterators() error {
	var err error
	if p.iter == nil {
		var iter *pebble.Iterator
		if p.batch.Indexed() {
			iter, err = p.batch.NewIter(nil)
		} else {
			iter, err = p.db.NewIter(nil)
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

// NewMVCCIterator implements the Batch interface.
func (p *pebbleBatch) ApplyBatchRepr(repr []byte, sync bool) error {
	var batch pebble.Batch
	if err := batch.SetRepr(repr); err != nil {
		return err
	}

	return p.batch.Apply(&batch, nil)
}

// ClearMVCC implements the Batch interface.
func (p *pebbleBatch) ClearMVCC(key MVCCKey, opts ClearOptions) error {
	if key.Timestamp.IsEmpty() {
		panic("ClearMVCC timestamp is empty")
	}
	return p.clear(key, opts)
}

// ClearUnversioned implements the Batch interface.
func (p *pebbleBatch) ClearUnversioned(key roachpb.Key, opts ClearOptions) error {
	return p.clear(MVCCKey{Key: key}, opts)
}

// ClearEngineKey implements the Batch interface.
func (p *pebbleBatch) ClearEngineKey(key EngineKey, opts ClearOptions) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	p.buf = key.EncodeToBuf(p.buf[:0])
	if !opts.ValueSizeKnown || !p.mayWriteSizedDeletes {
		return p.batch.Delete(p.buf, nil)
	}
	return p.batch.DeleteSized(p.buf, opts.ValueSize, nil)
}

func (p *pebbleBatch) clear(key MVCCKey, opts ClearOptions) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = EncodeMVCCKeyToBuf(p.buf[:0], key)
	if !opts.ValueSizeKnown || !p.mayWriteSizedDeletes {
		return p.batch.Delete(p.buf, nil)
	}
	return p.batch.DeleteSized(p.buf, opts.ValueSize, nil)
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
func (p *pebbleBatch) ClearRawRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	p.buf = EngineKey{Key: start}.EncodeToBuf(p.buf[:0])
	endRaw := EngineKey{Key: end}.Encode()
	if pointKeys {
		if err := p.batch.DeleteRange(p.buf, endRaw, pebble.Sync); err != nil {
			return err
		}
	}
	if rangeKeys {
		if err := p.batch.RangeKeyDelete(p.buf, endRaw, pebble.Sync); err != nil {
			return err
		}
	}
	return nil
}

// ClearMVCCRange implements the Batch interface.
func (p *pebbleBatch) ClearMVCCRange(start, end roachpb.Key, pointKeys, rangeKeys bool) error {
	if err := p.ClearRawRange(start, end, pointKeys, rangeKeys); err != nil {
		return err
	}
	// The lock table only contains point keys, so only clear it when point keys
	// are requested, and don't clear range keys in it.
	if !pointKeys {
		return nil
	}
	lstart, _ := keys.LockTableSingleKey(start, nil)
	lend, _ := keys.LockTableSingleKey(end, nil)
	return p.ClearRawRange(lstart, lend, true /* pointKeys */, false /* rangeKeys */)
}

// ClearMVCCVersions implements the Batch interface.
func (p *pebbleBatch) ClearMVCCVersions(start, end MVCCKey) error {
	p.buf = EncodeMVCCKeyToBuf(p.buf[:0], start)
	return p.batch.DeleteRange(p.buf, EncodeMVCCKey(end), nil)
}

// ClearMVCCIteratorRange implements the Batch interface.
func (p *pebbleBatch) ClearMVCCIteratorRange(
	start, end roachpb.Key, pointKeys, rangeKeys bool,
) error {
	clearPointKeys := func(start, end roachpb.Key) error {
		iter, err := p.NewMVCCIterator(MVCCKeyAndIntentsIterKind, IterOptions{
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
		iter, err := p.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
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
				if err := p.batch.RangeKeyUnset(startRaw, endRaw,
					EncodeMVCCTimestampSuffix(v.Timestamp), nil); err != nil {
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

// ClearMVCCRangeKey implements the Engine interface.
func (p *pebbleBatch) ClearMVCCRangeKey(rangeKey MVCCRangeKey) error {
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	return p.ClearEngineRangeKey(
		rangeKey.StartKey, rangeKey.EndKey, EncodeMVCCTimestampSuffix(rangeKey.Timestamp))
}

// BufferedSize implements the Engine interface.
func (p *pebbleBatch) BufferedSize() int {
	return p.Len()
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
	if err := rangeKey.Validate(); err != nil {
		return err
	}
	return p.PutEngineRangeKey(
		rangeKey.StartKey, rangeKey.EndKey, EncodeMVCCTimestampSuffix(rangeKey.Timestamp), value)
}

// PutEngineRangeKey implements the Engine interface.
func (p *pebbleBatch) PutEngineRangeKey(start, end roachpb.Key, suffix, value []byte) error {
	return p.batch.RangeKeySet(
		EngineKey{Key: start}.Encode(), EngineKey{Key: end}.Encode(), suffix, value, nil)
}

// PutInternalRangeKey implements the InternalWriter interface.
func (p *pebbleBatch) PutInternalRangeKey(start, end []byte, key rangekey.Key) error {
	switch key.Kind() {
	case pebble.InternalKeyKindRangeKeyUnset:
		return p.batch.RangeKeyUnset(start, end, key.Suffix, nil /* writeOptions */)
	case pebble.InternalKeyKindRangeKeySet:
		return p.batch.RangeKeySet(start, end, key.Suffix, key.Value, nil /* writeOptions */)
	case pebble.InternalKeyKindRangeKeyDelete:
		return p.batch.RangeKeyDelete(start, end, nil /* writeOptions */)
	default:
		panic("unexpected range key kind")
	}
}

// ClearEngineRangeKey implements the Engine interface.
func (p *pebbleBatch) ClearEngineRangeKey(start, end roachpb.Key, suffix []byte) error {
	return p.batch.RangeKeyUnset(
		EngineKey{Key: start}.Encode(), EngineKey{Key: end}.Encode(), suffix, nil)
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

// PutEngineKey implements the Batch interface.
func (p *pebbleBatch) PutEngineKey(key EngineKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = key.EncodeToBuf(p.buf[:0])
	return p.batch.Set(p.buf, value, nil)
}

// PutInternalPointKey implements the WriteBatch interface.
func (p *pebbleBatch) PutInternalPointKey(key *pebble.InternalKey, value []byte) error {
	if len(key.UserKey) == 0 {
		return emptyKeyError()
	}
	return p.batch.AddInternalKey(key, value, nil /* writeOptions */)
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
		// TODO(storage): ensure that these errors are only ever due to invariant
		// violations and never due to unrecoverable Pebble states. Then switch to
		// returning the error instead of panicking.
		//
		// Once we do that, document on the storage.Batch interface the meaning of
		// an error returned from this method and the guarantees that callers have
		// or don't have after they receive an error from this method.
		panic(err)
	}
	p.batchStatsReporter.aggregateBatchCommitStats(
		BatchCommitStats{p.batch.CommitStats()})
	return err
}

// CommitNoSyncWait implements the Batch interface.
func (p *pebbleBatch) CommitNoSyncWait() error {
	if p.batch == nil {
		panic("called with nil batch")
	}
	err := p.db.ApplyNoSyncWait(p.batch, pebble.Sync)
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

// SyncWait implements the Batch interface.
func (p *pebbleBatch) SyncWait() error {
	if p.batch == nil {
		panic("called with nil batch")
	}
	err := p.batch.SyncWait()
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
	p.batchStatsReporter.aggregateBatchCommitStats(
		BatchCommitStats{p.batch.CommitStats()})

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

// CommitStats implements the Batch interface.
func (p *pebbleBatch) CommitStats() BatchCommitStats {
	return BatchCommitStats{BatchCommitStats: p.batch.CommitStats()}
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
