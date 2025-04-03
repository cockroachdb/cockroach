// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build crdb_test && !crdb_test_off

package pebbleiter

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// Iterator wraps the *pebble.Iterator in crdb_test builds with an assertionIter
// that detects when Close is called on the iterator twice.  Double closes are
// problematic because they can result in an iterator being added to a sync pool
// twice, allowing concurrent use of the same iterator struct.
type Iterator = *assertionIter

// MaybeWrap returns the provided Pebble iterator, wrapped with double close
// detection.
func MaybeWrap(iter *pebble.Iterator) Iterator {
	return &assertionIter{Iterator: iter, closedCh: make(chan struct{})}
}

// assertionIter wraps a *pebble.Iterator with assertion checking.
type assertionIter struct {
	*pebble.Iterator
	closed   bool
	closedCh chan struct{}
	// unsafeBufs hold buffers used for returning values with short lifetimes to
	// the caller. To assert that the client is respecting the lifetimes,
	// assertionIter mangles the buffers as soon as the associated lifetime
	// expires. This is the same technique applied by the unsafeMVCCIterator in
	// pkg/storage, but this time applied at the API boundary between
	// pkg/storage and Pebble.
	//
	// unsafeBufs holds two buffers per-key type and an index indicating which
	// are currently in use. This is used to randomly switch to a different
	// buffer, ensuring that the buffer(s) returned to the caller for the
	// previous iterator position are garbage (as opposed to just state
	// corresponding to the current iterator position).
	unsafeBufs struct {
		idx int
		key [2][]byte
		val [2][]byte
	}
	rangeKeyBufs struct {
		idx  int
		bufs [2]rangeKeyBuf
	}
}

type rangeKeyBuf struct {
	start []byte
	end   []byte
	keys  []pebble.RangeKeyData

	// bgCh is used in race builds to synchronize with a separate goroutine
	// performing background buffer mangling. If non-nil, a separate mangling
	// goroutine is active and periodically mangling this buffer. When the
	// buffer is next used, maybeSaveAndMangleRangeKeyBufs performs a
	// synchronous send to the channel to signal that the mangling goroutine
	// should exit.
	bgCh chan struct{}
}

func (b *rangeKeyBuf) mangle() {
	zero(b.start)
	zero(b.end)
	for k := range b.keys {
		zero(b.keys[k].Suffix)
		zero(b.keys[k].Value)
	}
}

func (i *assertionIter) Clone(cloneOpts pebble.CloneOptions) (Iterator, error) {
	iter, err := i.Iterator.Clone(cloneOpts)
	if err != nil {
		return nil, err
	}
	return MaybeWrap(iter), nil
}

func (i *assertionIter) CloneWithContext(
	ctx context.Context, cloneOpts pebble.CloneOptions,
) (Iterator, error) {
	iter, err := i.Iterator.CloneWithContext(ctx, cloneOpts)
	if err != nil {
		return nil, err
	}
	return MaybeWrap(iter), nil
}

func (i *assertionIter) Close() error {
	if i.closed {
		panic(errors.AssertionFailedf("pebble.Iterator already closed"))
	}
	i.closed = true
	close(i.closedCh)
	return i.Iterator.Close()
}

func (i *assertionIter) Key() []byte {
	if !i.Valid() {
		panic(errors.AssertionFailedf("Key() called on !Valid() pebble.Iterator"))
	}
	idx := i.unsafeBufs.idx
	i.unsafeBufs.key[idx] = append(i.unsafeBufs.key[idx][:0], i.Iterator.Key()...)
	return i.unsafeBufs.key[idx]
}

func (i *assertionIter) Value() []byte {
	panic(errors.AssertionFailedf("Value() is deprecated; use ValueAndErr()"))
}

func (i *assertionIter) ValueAndErr() ([]byte, error) {
	if !i.Valid() {
		panic(errors.AssertionFailedf("ValueAndErr() called on !Valid() pebble.Iterator"))
	}
	val, err := i.Iterator.ValueAndErr()
	idx := i.unsafeBufs.idx
	i.unsafeBufs.val[idx] = append(i.unsafeBufs.val[idx][:0], val...)
	// Preserve nil-ness to ensure we test exactly the behavior of Pebble.
	if val == nil {
		return val, err
	}
	if i.unsafeBufs.val[idx] == nil {
		i.unsafeBufs.val[idx] = []byte{}
	}
	return i.unsafeBufs.val[idx], err
}

func (i *assertionIter) LazyValue() pebble.LazyValue {
	if !i.Valid() {
		panic(errors.AssertionFailedf("LazyValue() called on !Valid() pebble.Iterator"))
	}
	return i.Iterator.LazyValue()
}

func (i *assertionIter) RangeBounds() ([]byte, []byte) {
	if !i.Valid() {
		panic(errors.AssertionFailedf("RangeBounds() called on !Valid() pebble.Iterator"))
	}
	if _, hasRange := i.Iterator.HasPointAndRange(); !hasRange {
		return nil, nil
	}
	// See maybeSaveAndMangleRangeKeyBufs for where these are saved.
	j := i.rangeKeyBufs.idx
	return i.rangeKeyBufs.bufs[j].start, i.rangeKeyBufs.bufs[j].end
}

func (i *assertionIter) RangeKeys() []pebble.RangeKeyData {
	if !i.Valid() {
		panic(errors.AssertionFailedf("RangeKeys() called on !Valid() pebble.Iterator"))
	}
	if _, hasRange := i.Iterator.HasPointAndRange(); !hasRange {
		panic(errors.AssertionFailedf("RangeKeys() called on pebble.Iterator without range keys"))
	}
	// See maybeSaveAndMangleRangeKeyBufs for where these are saved.
	return i.rangeKeyBufs.bufs[i.rangeKeyBufs.idx].keys
}

func (i *assertionIter) First() bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.First()
}

func (i *assertionIter) SeekGE(key []byte) bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.SeekGE(key)
}

func (i *assertionIter) SeekGEWithLimit(key []byte, limit []byte) pebble.IterValidityState {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.SeekGEWithLimit(key, limit)
}

func (i *assertionIter) SeekPrefixGE(key []byte) bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.SeekPrefixGE(key)
}

func (i *assertionIter) Next() bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.Next()
}

func (i *assertionIter) NextWithLimit(limit []byte) pebble.IterValidityState {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.NextWithLimit(limit)
}

func (i *assertionIter) NextPrefix() bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.NextPrefix()
}

func (i *assertionIter) Last() bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.Last()
}

func (i *assertionIter) SeekLT(key []byte) bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.SeekLT(key)
}

func (i *assertionIter) SeekLTWithLimit(key []byte, limit []byte) pebble.IterValidityState {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.SeekLTWithLimit(key, limit)
}

func (i *assertionIter) Prev() bool {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.Prev()
}

func (i *assertionIter) PrevWithLimit(limit []byte) pebble.IterValidityState {
	i.maybeMangleBufs()
	defer i.maybeSaveAndMangleRangeKeyBufs()
	return i.Iterator.PrevWithLimit(limit)
}

// maybeMangleBufs trashes the contents of buffers used to return unsafe values
// to the caller. This is used to ensure that the client respects the Pebble
// iterator interface and the lifetimes of buffers it returns.
func (i *assertionIter) maybeMangleBufs() {
	if rand.Intn(2) == 0 {
		idx := i.unsafeBufs.idx
		zero(i.unsafeBufs.key[idx])
		zero(i.unsafeBufs.val[idx])
		if rand.Intn(2) == 0 {
			// Switch to a new buffer for the next iterator position.
			i.unsafeBufs.idx = (i.unsafeBufs.idx + 1) % 2
		}
	}
}

// maybeSaveAndMangleRangeKeyBufs is invoked at the end of every iterator
// operation. It saves the range keys to buffers owned by `assertionIter` and
// with random probability mangles any buffers previously returned to the user.
func (i *assertionIter) maybeSaveAndMangleRangeKeyBufs() {
	// If RangeKeyChanged()=false, the pebble.Iterator contract guarantees that
	// any buffers previously returned through RangeBounds() and RangeKeys() are
	// still valid.
	//
	// NB: Only permitted to call RangeKeyChanged() if Valid().
	valid := i.Iterator.Valid()
	if valid && !i.Iterator.RangeKeyChanged() {
		return
	}
	// INVARIANT: !Valid() || RangeKeyChanged()

	// The previous range key buffers are no longer guaranteed to be stable.
	// Randomly zero them to ensure we catch bugs where they're reused.
	idx := i.rangeKeyBufs.idx
	mangleBuf := &i.rangeKeyBufs.bufs[idx]
	if rand.Intn(2) == 0 {
		mangleBuf.mangle()
	}
	// If the new iterator position has range keys, copy them to our buffers.
	if !valid {
		return
	}
	if _, hasRange := i.Iterator.HasPointAndRange(); !hasRange {
		return
	}
	switchBuffers := rand.Intn(2) == 0
	if switchBuffers {
		// Switch to a new buffer for the new range key state.
		i.rangeKeyBufs.idx = (idx + 1) % 2

		// In race builds, mangle the old buffer from another goroutine. This is
		// nice because the race detector can tell us where the improper read is
		// originating. Otherwise, we're relying on the improper read
		// manifesting a test assertion failure, which may be far from the
		// problematic access of an unsafe buffer.
		if util.RaceEnabled {
			ch := make(chan struct{})
			mangleBuf.bgCh = ch
			ticker := time.NewTicker(5 * time.Millisecond)
			go func() {
				defer ticker.Stop()
				mangleBuf.mangle()
				for {
					select {
					case <-i.closedCh:
						return
					case <-ch:
						return
					case <-ticker.C:
						mangleBuf.mangle()
					}
				}
			}()

			// If the buffer we're about to use is being mangled in the
			// background, synchronize with the goroutine doing the mangling by
			// sending to its channel. After the synchronous channel send, we're
			// guaranteed the goroutine will not mangle the buffer again and
			// we're free to use it.
			if prevMangleBuf := &i.rangeKeyBufs.bufs[i.rangeKeyBufs.idx]; prevMangleBuf.bgCh != nil {
				prevMangleBuf.bgCh <- struct{}{}
				prevMangleBuf.bgCh = nil
			}
		}
	}
	start, end := i.Iterator.RangeBounds()
	rangeKeys := i.Iterator.RangeKeys()

	buf := &i.rangeKeyBufs.bufs[i.rangeKeyBufs.idx]
	buf.start = append(buf.start[:0], start...)
	buf.end = append(buf.end[:0], end...)
	if len(rangeKeys) > cap(buf.keys) {
		buf.keys = make([]pebble.RangeKeyData, len(rangeKeys))
	} else {
		buf.keys = buf.keys[:len(rangeKeys)]
	}
	for k := range rangeKeys {
		// Preserve nil-ness returned by Pebble to ensure we're testing exactly
		// what Pebble will return in production.
		copyWithMatchingNilness(&buf.keys[k].Suffix, rangeKeys[k].Suffix)
		copyWithMatchingNilness(&buf.keys[k].Value, rangeKeys[k].Value)
	}
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0x00
	}
}

func copyWithMatchingNilness(dst *[]byte, src []byte) {
	if src == nil {
		*dst = nil
		return
	}
	if *dst == nil {
		*dst = make([]byte, 0, len(src))
	}
	*dst = append((*dst)[:0], src...)
}
