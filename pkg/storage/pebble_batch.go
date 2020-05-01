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
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// Wrapper struct around a pebble.Batch.
type pebbleBatch struct {
	db           *pebble.DB
	batch        *pebble.Batch
	buf          []byte
	prefixIter   pebbleIterator
	normalIter   pebbleIterator
	closed       bool
	isDistinct   bool
	distinctOpen bool
	parentBatch  *pebbleBatch
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
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}
	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	ret, closer, err := r.Get(p.buf)
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
	if len(key.Key) == 0 {
		return false, 0, 0, emptyKeyError()
	}
	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	val, closer, err := r.Get(p.buf)
	if closer != nil {
		if msg != nil {
			err = protoutil.Unmarshal(val, msg)
		}
		keyBytes = int64(len(p.buf))
		valBytes = int64(len(val))
		closer.Close()
		return true, keyBytes, valBytes, err
	}
	if errors.Is(err, pebble.ErrNotFound) {
		return false, 0, 0, nil
	}
	return false, 0, 0, err
}

// Iterate implements the Batch interface.
func (p *pebbleBatch) Iterate(
	start, end roachpb.Key, f func(MVCCKeyValue) (stop bool, err error),
) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}
	return iterateOnReader(p, start, end, f)
}

// NewIterator implements the Batch interface.
func (p *pebbleBatch) NewIterator(opts IterOptions) Iterator {
	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}

	if !p.batch.Indexed() && !p.isDistinct {
		panic("write-only batch")
	}
	if p.distinctOpen {
		panic("distinct batch open")
	}

	if opts.MinTimestampHint != (hlc.Timestamp{}) {
		// Iterators that specify timestamp bounds cannot be cached.
		return newPebbleIterator(p.batch, opts)
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
	if p.distinctOpen {
		panic("distinct batch open")
	}
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.batch.Delete(p.buf, nil)
}

// SingleClear implements the Batch interface.
func (p *pebbleBatch) SingleClear(key MVCCKey) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.batch.SingleDelete(p.buf, nil)
}

// ClearRange implements the Batch interface.
func (p *pebbleBatch) ClearRange(start, end MVCCKey) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], start)
	buf2 := EncodeKey(end)
	return p.batch.DeleteRange(p.buf, buf2, nil)
}

// Clear implements the Batch interface.
func (p *pebbleBatch) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}

	type unsafeRawKeyGetter interface{ unsafeRawKey() []byte }
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

		err = p.batch.Delete(iter.(unsafeRawKeyGetter).unsafeRawKey(), nil)
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

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.batch.Merge(p.buf, value, nil)
}

// Put implements the Batch interface.
func (p *pebbleBatch) Put(key MVCCKey, value []byte) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}
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
