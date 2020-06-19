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
	db             *pebble.DB
	batch          *pebble.Batch
	buf            []byte
	prefixIter     pebbleIterator
	normalIter     pebbleIterator
	prefixLockIter pebbleIterator
	normalLockIter pebbleIterator
	closed         bool
	isDistinct     bool
	distinctOpen   bool
	parentBatch    *pebbleBatch

	wrappedIntentWriter wrappedIntentWriter
}

var _ Batch = &pebbleBatch{}

var pebbleBatchPool = sync.Pool{
	New: func() interface{} {
		return &pebbleBatch{}
	},
}

// Instantiates a new pebbleBatch.
func newPebbleBatch(db *pebble.DB, batch *pebble.Batch) *pebbleBatch {
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
	}
	pb.wrappedIntentWriter = possiblyMakeWrappedIntentWriter(pb)
	return pb
}

// Close implements the Batch interface.
func (p *pebbleBatch) Close() {
	if p.closed {
		panic("closing an already-closed pebbleBatch")
	}
	p.closed = true

	// Destroy the iterators before closing the batch.
	p.prefixIter.destroy()
	p.normalIter.destroy()

	if !p.isDistinct {
		_ = p.batch.Close()
		p.batch = nil
	} else {
		p.parentBatch.distinctOpen = false
		p.isDistinct = false
	}

	pebbleBatchPool.Put(p)
}

// Closed implements the Batch interface.
func (p *pebbleBatch) Closed() bool {
	return p.closed
}

// ExportToSst is part of the engine.Reader interface.
func (p *pebbleBatch) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	panic("unimplemented")
}

// Get implements the Batch interface.
func (p *pebbleBatch) Get(key MVCCKey) ([]byte, error) {
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}
	reader, wrapped := possiblyWrapReader(p, MVCCKeyAndIntentsIterKind)
	if wrapped {
		return reader.Get(key)
	}
	p.buf = EncodeMVCCKeyToBuf(p.buf[:0], key)
	return p.rawGet(p.buf)
}

func (p *pebbleBatch) rawGet(key []byte) ([]byte, error) {
	r := pebble.Reader(p.batch)
	if !p.isDistinct {
		if !p.batch.Indexed() {
			panic("write-only batch")
		}
		if p.distinctOpen {
			panic("distinct batch open")
		}
	} else if !p.batch.Indexed() {
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

// GetProto implements the Batch interface.
func (p *pebbleBatch) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	return pebbleGetProto(p, key, msg)
}

// Iterate implements the Batch interface.
func (p *pebbleBatch) Iterate(
	start, end roachpb.Key, seeIntents bool, f func(MVCCKeyValue) (stop bool, err error),
) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}
	iterKind := MVCCKeyAndIntentsIterKind
	if !seeIntents {
		iterKind = MVCCKeyIterKind
	}
	r, _ := possiblyWrapReader(p, iterKind)
	return iterateOnReader(r, start, end, iterKind, f)
}

// NewIterator implements the Batch interface.
func (p *pebbleBatch) NewIterator(opts IterOptions, iterKind IterKind) Iterator {
	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}

	if !p.batch.Indexed() && !p.isDistinct {
		panic("write-only batch")
	}
	if p.distinctOpen {
		panic("distinct batch open")
	}

	r, wrapped := possiblyWrapReader(p, iterKind)
	if wrapped {
		return r.NewIterator(opts, iterKind)
	}
	return p.newIterator(opts, iterKind)
}

func (p *pebbleBatch) newIterator(opts IterOptions, iterKind IterKind) Iterator {
	if opts.MinTimestampHint != (hlc.Timestamp{}) {
		// Iterators that specify timestamp bounds cannot be cached.
		return newPebbleIterator(p.batch, opts)
	}

	var iter *pebbleIterator
	if iterKind == StorageKeyIterKind {
		iter = &p.normalLockIter
		if opts.Prefix {
			iter = &p.prefixLockIter
		}
	} else {
		iter = &p.normalIter
		if opts.Prefix {
			iter = &p.prefixIter
		}
	}
	if iter.inuse {
		panic("iterator already in use")
	}

	if iter.iter != nil {
		iter.setOptions(opts)
	} else if p.batch.Indexed() {
		iter.init(p.batch, opts)
	} else {
		iter.init(p.db, opts)
	}

	iter.inuse = true
	return iter
}

// NewIterator implements the Batch interface.
func (p *pebbleBatch) ApplyBatchRepr(repr []byte, sync bool) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}

	var batch pebble.Batch
	if err := batch.SetRepr(repr); err != nil {
		return err
	}

	return p.batch.Apply(&batch, nil)
}

// Clear implements the Batch interface.
func (p *pebbleBatch) Clear(key MVCCKey) error {
	if key.Timestamp == (hlc.Timestamp{}) {
		panic("")
	}
	return p.clear(key)
}

func (p *pebbleBatch) clear(key MVCCKey) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = EncodeMVCCKeyToBuf(p.buf[:0], key)
	return p.batch.Delete(p.buf, nil)
}

// ClearKeyWithEmptyTimestamp implements the Batch interface.
func (p *pebbleBatch) ClearKeyWithEmptyTimestamp(key roachpb.Key) error {
	return p.clear(MVCCKey{Key: key})
}

// ClearMVCCMeta implements the Batch interface.
func (p *pebbleBatch) ClearMVCCMeta(
	key roachpb.Key, state PrecedingIntentState, possiblyUpdated bool, txnUUID uuid.UUID,
) error {
	if p.wrappedIntentWriter == nil {
		return p.clear(MVCCKey{Key: key})
	}
	return p.wrappedIntentWriter.ClearMVCCMeta(key, state, possiblyUpdated, txnUUID)
}

// Clear implements the Batch interface.
func (p *pebbleBatch) clearStorageKey(key StorageKey) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = key.EncodeToBuf(p.buf[:0])
	return p.batch.Delete(p.buf, nil)
}

// SingleClear implements the Batch interface.
func (p *pebbleBatch) SingleClear(key MVCCKey) error {
	if key.Timestamp == (hlc.Timestamp{}) {
		panic("")
	}
	return p.singleClear(key)
}

func (p *pebbleBatch) singleClear(key MVCCKey) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = EncodeMVCCKeyToBuf(p.buf[:0], key)
	return p.batch.SingleDelete(p.buf, nil)
}

// SingleClearWithEmptyTimestamp implements the Batch interface.
func (p *pebbleBatch) SingleClearKeyWithEmptyTimestamp(key roachpb.Key) error {
	return p.singleClear(MVCCKey{Key: key})
}

// singleClearStorageKey implements the wrappableIntentWriter interface
func (p *pebbleBatch) singleClearStorageKey(key StorageKey) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = key.EncodeToBuf(p.buf[:0])
	return p.batch.SingleDelete(p.buf, nil)
}

// ClearNonMVCCRange implements the Batch interface.
func (p *pebbleBatch) ClearNonMVCCRange(start, end roachpb.Key) error {
	return p.ClearMVCCRange(MVCCKey{Key: start}, MVCCKey{Key: end})
}

// ClearMVCCRangeAndIntents implements the Batch interface.
func (p *pebbleBatch) ClearMVCCRangeAndIntents(start, end roachpb.Key) error {
	if p.wrappedIntentWriter == nil {
		p.ClearMVCCRange(MVCCKey{Key: start}, MVCCKey{Key: end})
	}
	return p.wrappedIntentWriter.ClearMVCCRangeAndIntents(start, end)
}

// ClearMVCCRange implements the Batch interface.
func (p *pebbleBatch) ClearMVCCRange(start, end MVCCKey) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}

	p.buf = EncodeMVCCKeyToBuf(p.buf[:0], start)
	buf2 := EncodeMVCCKey(end)
	return p.batch.DeleteRange(p.buf, buf2, nil)
}

// ClearIterMVCCRangeAndIntents implements the Batch interface.
func (p *pebbleBatch) ClearIterMVCCRangeAndIntents(iter Iterator, start, end roachpb.Key) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}

	// Note that this method has the side effect of modifying iter's bounds.
	// Since all calls to `ClearIterMVCCRangeAndIntents` are on new throwaway iterators with no
	// lower bounds, calling SetUpperBound should be sufficient and safe.
	iter.SetUpperBound(end)
	iter.SeekGE(MakeMVCCMetadataKey(start))

	for ; ; iter.Next() {
		valid, err := iter.Valid()
		if err != nil {
			return err
		} else if !valid {
			break
		}

		// The key may be a separated intent that was interleaved, or one
		// could be clearing a non-MVCC range (this is a mess!).
		// Use the raw key so that we actually delete the intent.
		err = p.batch.Delete(iter.UnsafeRawKeyDangerous(), nil)
		if err != nil {
			return err
		}
	}
	return nil
}

// Merge implements the Batch interface.
func (p *pebbleBatch) Merge(key MVCCKey, value []byte) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = EncodeMVCCKeyToBuf(p.buf[:0], key)
	return p.batch.Merge(p.buf, value, nil)
}

// Put implements the Batch interface.
func (p *pebbleBatch) Put(key MVCCKey, value []byte) error {
	if key.Timestamp == (hlc.Timestamp{}) {
		panic("")
	}
	return p.put(key, value)
}

func (p *pebbleBatch) put(key MVCCKey, value []byte) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = EncodeMVCCKeyToBuf(p.buf[:0], key)
	return p.batch.Set(p.buf, value, nil)
}

// PutKeyWithEmptyTimestamp implements the Batch interface.
func (p *pebbleBatch) PutKeyWithEmptyTimestamp(key roachpb.Key, value []byte) error {
	return p.put(MVCCKey{Key: key}, value)
}

// PutMVCCMeta implements the Batch interface.
func (p *pebbleBatch) PutMVCCMeta(
	key roachpb.Key,
	value []byte,
	state PrecedingIntentState,
	precedingPossiblyUpdated bool,
	txnUUID uuid.UUID,
) error {
	if p.wrappedIntentWriter == nil {
		return p.put(MVCCKey{Key: key}, value)
	}
	return p.wrappedIntentWriter.PutMVCCMeta(key, value, state, precedingPossiblyUpdated, txnUUID)
}

// PutStorage implements the Batch interface.
func (p *pebbleBatch) PutStorage(key StorageKey, value []byte) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = key.EncodeToBuf(p.buf[:0])
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

// Distinct implements the Batch interface.
func (p *pebbleBatch) Distinct() ReadWriter {
	if p.distinctOpen {
		panic("distinct batch already open")
	}
	// Distinct batches are regular batches with isDistinct set to true. The
	// parent batch is stored in parentBatch, and all writes on it are disallowed
	// while the distinct batch is open. Both the distinct batch and the parent
	// batch share the same underlying pebble.Batch instance.
	//
	// The need for distinct batches is distinctly less in Pebble than
	// RocksDB. In RocksDB, a distinct batch allows reading from a batch without
	// flushing the buffered writes which is a significant performance
	// optimization. In Pebble we're still using the same underlying batch and if
	// it is indexed we'll still be indexing it as we Go.
	p.distinctOpen = true
	d := newPebbleBatch(p.db, p.batch)
	d.parentBatch = p
	d.isDistinct = true
	return d
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
