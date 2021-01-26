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
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	// When the first iterator is initialized, the underlying *pebble.Iterator
	// is stashed in iter, so that subsequent iterator initialization can use
	// Iterator.Clone to use the same underlying engine state. This relies on
	// the fact that all pebbleIterators created here are marked as reusable,
	// which causes pebbleIterator.Close to not close iter. iter will be closed
	// when pebbleBatch.Close is called.
	prefixIter          pebbleIterator
	normalIter          pebbleIterator
	prefixEngineIter    pebbleIterator
	normalEngineIter    pebbleIterator
	iter                cloneableIter
	unIndexedReadFromDB bool
	closed              bool

	useWrappedIntentWriter bool
	wrappedIntentWriter    intentDemuxWriter
	// scratch space for wrappedIntentWriter.
	scratch []byte
}

var _ Batch = &pebbleBatch{}

var pebbleBatchPool = sync.Pool{
	New: func() interface{} {
		return &pebbleBatch{}
	},
}

// Instantiates a new pebbleBatch.
func newPebbleBatch(db *pebble.DB, batch *pebble.Batch, unIndexedReadFromDB bool) *pebbleBatch {
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
		unIndexedReadFromDB: unIndexedReadFromDB,
	}
	pb.wrappedIntentWriter, pb.useWrappedIntentWriter = tryWrapIntentWriter(pb)
	return pb
}

// Close implements the Batch interface.
func (p *pebbleBatch) Close() {
	if p.closed {
		panic("closing an already-closed pebbleBatch")
	}
	p.closed = true

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

// ExportMVCCToSst is part of the engine.Reader interface.
func (p *pebbleBatch) ExportMVCCToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	useTBI bool,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	panic("unimplemented")
}

// Get implements the Batch interface.
func (p *pebbleBatch) MVCCGet(key MVCCKey) ([]byte, error) {
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}
	if r, wrapped := tryWrapReader(p, MVCCKeyAndIntentsIterKind); wrapped {
		return r.MVCCGet(key)
	}
	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.rawGet(p.buf)
}

func (p *pebbleBatch) rawGet(key []byte) ([]byte, error) {
	r := pebble.Reader(p.batch)
	indexed := p.batch.Indexed()
	if !indexed && !p.unIndexedReadFromDB {
		panic("write-only batch")
	}
	if !indexed {
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
	r, _ := tryWrapReader(p, iterKind)
	return iterateOnReader(r, start, end, iterKind, f)
}

// NewMVCCIterator implements the Batch interface.
func (p *pebbleBatch) NewMVCCIterator(iterKind MVCCIterKind, opts IterOptions) MVCCIterator {
	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}

	indexed := p.batch.Indexed()
	if !indexed && !p.unIndexedReadFromDB {
		panic("write-only batch")
	}

	if iterKind == MVCCKeyAndIntentsIterKind {
		if r, wrapped := tryWrapReader(p, iterKind); wrapped {
			return r.NewMVCCIterator(iterKind, opts)
		}
	}

	if !opts.MinTimestampHint.IsEmpty() {
		// MVCCIterators that specify timestamp bounds cannot be cached.
		return newPebbleIterator(p.batch, nil, opts)
	}

	iter := &p.normalIter
	if opts.Prefix {
		iter = &p.prefixIter
	}
	if iter.inuse {
		panic("iterator already in use")
	}

	if iter.iter != nil {
		iter.setOptions(opts)
	} else {
		if indexed {
			iter.init(p.batch, p.iter, opts)
		} else {
			iter.init(p.db, p.iter, opts)
		}
		// The timestamp hints should be empty given the earlier code, but we are
		// being defensive.
		if p.iter == nil && opts.MaxTimestampHint.IsEmpty() && opts.MinTimestampHint.IsEmpty() {
			// For future cloning.
			p.iter = iter.iter
		}
	}

	iter.inuse = true
	return iter
}

// NewEngineIterator implements the Batch interface.
func (p *pebbleBatch) NewEngineIterator(opts IterOptions) EngineIterator {
	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}

	indexed := p.batch.Indexed()
	if !indexed && !p.unIndexedReadFromDB {
		panic("write-only batch")
	}

	iter := &p.normalEngineIter
	if opts.Prefix {
		iter = &p.prefixEngineIter
	}
	if iter.inuse {
		panic("iterator already in use")
	}

	if iter.iter != nil {
		iter.setOptions(opts)
	} else {
		if indexed {
			iter.init(p.batch, p.iter, opts)
		} else {
			iter.init(p.db, p.iter, opts)
		}
		// The timestamp hints should be empty given this is an EngineIterator,
		// but we are being defensive.
		if p.iter == nil && opts.MaxTimestampHint.IsEmpty() && opts.MinTimestampHint.IsEmpty() {
			// For future cloning.
			p.iter = iter.iter
		}
	}

	iter.inuse = true
	return iter
}

// ConsistentIterators implements the Batch interface.
func (p *pebbleBatch) ConsistentIterators() bool {
	return true
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
	key roachpb.Key, state PrecedingIntentState, txnDidNotUpdateMeta bool, txnUUID uuid.UUID,
) error {
	if p.useWrappedIntentWriter {
		var err error
		p.scratch, err =
			p.wrappedIntentWriter.ClearIntent(key, state, txnDidNotUpdateMeta, txnUUID, p.scratch)
		return err
	}
	return p.clear(MVCCKey{Key: key})
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

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
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
	return p.clearRange(MVCCKey{Key: start}, MVCCKey{Key: end})
}

// ClearMVCCRangeAndIntents implements the Batch interface.
func (p *pebbleBatch) ClearMVCCRangeAndIntents(start, end roachpb.Key) error {
	if p.useWrappedIntentWriter {
		var err error
		p.scratch, err = p.wrappedIntentWriter.ClearMVCCRangeAndIntents(start, end, p.scratch)
		return err
	}
	return p.clearRange(MVCCKey{Key: start}, MVCCKey{Key: end})
}

// ClearMVCCRange implements the Batch interface.
func (p *pebbleBatch) ClearMVCCRange(start, end MVCCKey) error {
	return p.clearRange(start, end)
}

func (p *pebbleBatch) clearRange(start, end MVCCKey) error {
	p.buf = EncodeKeyToBuf(p.buf[:0], start)
	buf2 := EncodeKey(end)
	return p.batch.DeleteRange(p.buf, buf2, nil)
}

// Clear implements the Batch interface.
func (p *pebbleBatch) ClearIterRange(iter MVCCIterator, start, end roachpb.Key) error {
	// Note that this method has the side effect of modifying iter's bounds.
	// Since all calls to `ClearIterRange` are on new throwaway iterators with no
	// lower bounds, calling SetUpperBound should be sufficient and safe.
	// Furthermore, the start and end keys are always metadata keys (i.e.
	// have zero timestamps), so we can ignore the bounds' MVCC timestamps.
	iter.SetUpperBound(end)
	iter.SeekGE(MakeMVCCMetadataKey(start))

	for ; ; iter.Next() {
		valid, err := iter.Valid()
		if err != nil {
			return err
		} else if !valid {
			break
		}
		// NB: UnsafeRawKey could be a serialized lock table key, and not just an
		// MVCCKey.
		err = p.batch.Delete(iter.UnsafeRawKey(), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// Merge implements the Batch interface.
func (p *pebbleBatch) Merge(key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.batch.Merge(p.buf, value, nil)
}

// PutMVCC implements the Batch interface.
func (p *pebbleBatch) PutMVCC(key MVCCKey, value []byte) error {
	if key.Timestamp.IsEmpty() {
		panic("PutMVCC timestamp is empty")
	}
	return p.put(key, value)
}

// PutUnversioned implements the Batch interface.
func (p *pebbleBatch) PutUnversioned(key roachpb.Key, value []byte) error {
	return p.put(MVCCKey{Key: key}, value)
}

// PutIntent implements the Batch interface.
func (p *pebbleBatch) PutIntent(
	key roachpb.Key,
	value []byte,
	state PrecedingIntentState,
	txnDidNotUpdateMeta bool,
	txnUUID uuid.UUID,
) error {
	if p.useWrappedIntentWriter {
		var err error
		p.scratch, err =
			p.wrappedIntentWriter.PutIntent(key, value, state, txnDidNotUpdateMeta, txnUUID, p.scratch)
		return err
	}
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

func (p *pebbleBatch) put(key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
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
