// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// Wrapper struct around a pebble.Batch.
type pebbleBatch struct {
	parent       *Pebble
	batch        *pebble.Batch
	buf          []byte
	iter         pebbleBatchIterator
	closed       bool
	isDistinct   bool
	writeOnly    bool
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
func newPebbleBatch(p *Pebble, writeOnly bool) *pebbleBatch {
	var batch *pebble.Batch
	if writeOnly {
		batch = p.db.NewBatch()
	} else {
		batch = p.db.NewIndexedBatch()
	}
	pb := pebbleBatchPool.Get().(*pebbleBatch)
	*pb = pebbleBatch{
		parent:    p,
		batch:     batch,
		closed:    false,
		writeOnly: writeOnly,
	}

	return pb
}

// Close implements the Batch interface.
func (p *pebbleBatch) Close() {
	if p.iter.iter != nil {
		p.iter.iter.Close()
		p.iter.destroy()
	}
	if !p.isDistinct {
		p.batch.Close()
		p.batch = nil
	} else {
		p.parentBatch.distinctOpen = false
		p.isDistinct = false
	}
	p.closed = true
	p.parent = nil
	pebbleBatchPool.Put(p)
}

// Closed implements the Batch interface.
func (p *pebbleBatch) Closed() bool {
	return p.closed
}

// Get implements the Batch interface.
func (p *pebbleBatch) Get(key MVCCKey) ([]byte, error) {
	if p.writeOnly {
		panic("read operation called on write-only batch")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	ret, err := p.batch.Get(p.buf)
	if err == pebble.ErrNotFound || len(ret) == 0 {
		return nil, nil
	}
	return ret, err
}

// GetProto implements the Batch interface.
func (p *pebbleBatch) GetProto(key MVCCKey, msg protoutil.Message) (ok bool, keyBytes, valBytes int64, err error) {
	if p.writeOnly {
		panic("read operation called on write-only batch")
	}

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	val, err := p.batch.Get(p.buf)
	if err != nil || val == nil {
		return
	}

	ok = true
	if msg != nil {
		err = protoutil.Unmarshal(val, msg)
	}
	keyBytes = int64(len(p.buf))
	valBytes = int64(len(val))
	return
}

// Iterate implements the Batch interface.
func (p *pebbleBatch) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (stop bool, err error)) error {
	if p.closed {
		return errors.New("cannot call Iterate on a closed batch")
	}

	if !start.Less(end) {
		return nil
	}

	it := p.NewIterator(IterOptions{UpperBound: end.Key})
	defer it.Close()

	it.Seek(start)
	for ; ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
			return err
		} else if !ok {
			break
		}

		k := it.Key()
		if !k.Less(end) {
			break
		}
		if done, err := f(MVCCKeyValue{Key: k, Value: it.Value()}); done || err != nil {
			return err
		}
	}
	return nil
}

// NewIterator implements the Batch interface.
func (p *pebbleBatch) NewIterator(opts IterOptions) Iterator {
	if p.writeOnly {
		panic("read operation called on write-only batch")
	}
	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}

	// Use the cached iterator.
	//
	// TODO(itsbilal): Investigate if it's equally or more efficient to just call
	// newPebbleIterator with p.batch as the handle, instead of caching an
	// iterator in pebbleBatch. This would clean up some of the oddities around
	// pebbleBatchIterator.Close() (which doesn't close the underlying pebble
	// Iterator), vs pebbleIterator.Close(), and the way memory is managed for
	// the two iterators.
	if p.iter.batch != nil {
		panic("iterator already in use")
	} else if p.iter.iter != nil {
		p.iter.iter.Close()
	}

	p.iter.init(p.parent, p.batch, opts)
	p.iter.batch = p
	return &p.iter
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

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.batch.Delete(p.buf, nil)
}

// SingleClear implements the Batch interface.
func (p *pebbleBatch) SingleClear(key MVCCKey) error {
	if p.distinctOpen {
		panic("distinct batch open")
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
func (p *pebbleBatch) ClearIterRange(iter Iterator, start, end MVCCKey) error {
	if p.distinctOpen {
		panic("distinct batch open")
	}

	iter.SetUpperBound(end.Key)
	iter.Seek(start)

	for ; ; iter.Next() {
		valid, err := iter.Valid()
		if err != nil {
			return err
		} else if !valid {
			break
		}

		p.buf = EncodeKeyToBuf(p.buf[:0], iter.Key())
		err = p.batch.Delete(p.buf, nil)
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

	p.buf = EncodeKeyToBuf(p.buf[:0], key)
	return p.batch.Merge(p.buf, value, nil)
}

// Put implements the Batch interface.
func (p *pebbleBatch) Put(key MVCCKey, value []byte) error {
	if p.distinctOpen {
		panic("distinct batch open")
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
	if (p.batch == nil) {
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
	// Distinct batches are regular batches with isDistinct set to true.
	// The parent batch is stored in parentBatch, and all writes on it are
	// disallowed while the distinct batch is open. Both the distinct batch and
	// the parent batch share the same underlying pebble.Batch instance.
	//
	// TODO(itsbilal): Investigate if we need to distinguish between distinct
	// and non-distinct batches.
	batch := &pebbleBatch{}
	batch.parent = p.parent
	batch.batch = p.batch
	batch.isDistinct = true
	p.distinctOpen = true
	batch.parentBatch = p

	return batch
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
	return p.batch.Repr()
}

// pebbleBatchIterator extends pebbleIterator and is meant to be embedded inside
// a pebbleBatch.
type pebbleBatchIterator struct {
	pebbleIterator
	batch *pebbleBatch
}

// Close implements the Iterator interface. There are two notable differences
// from pebbleIterator.Close: 1. don't close the underlying p.iter (this is done
// when the batch is closed), and 2. don't release the pebbleIterator back into
// pebbleIterPool, since this memory is managed by pebbleBatch instead.
func (p *pebbleBatchIterator) Close() {
	if p.batch == nil {
		panic("closing idle iterator")
	}
	p.batch = nil
}

// destory resets all fields in a pebbleBatchIterator, while holding onto
// some buffers to reduce allocations down the line. Assumes the underlying
// pebble.Iterator has been closed already.
func (p *pebbleBatchIterator) destroy() {
	*p = pebbleBatchIterator{
		pebbleIterator: pebbleIterator{
			lowerBoundBuf: p.lowerBoundBuf,
			upperBoundBuf: p.upperBoundBuf,
		},
		batch:          nil,
	}
}
