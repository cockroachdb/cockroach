// Copyright 2020 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/internal/keyspan"
)

// flushable defines the interface for immutable memtables.
type flushable interface {
	newIter(o *IterOptions) internalIterator
	newFlushIter(o *IterOptions, bytesFlushed *uint64) internalIterator
	newRangeDelIter(o *IterOptions) keyspan.FragmentIterator
	newRangeKeyIter(o *IterOptions) keyspan.FragmentIterator
	containsRangeKeys() bool
	// inuseBytes returns the number of inuse bytes by the flushable.
	inuseBytes() uint64
	// totalBytes returns the total number of bytes allocated by the flushable.
	totalBytes() uint64
	// readyForFlush returns true when the flushable is ready for flushing. See
	// memTable.readyForFlush for one implementation which needs to check whether
	// there are any outstanding write references.
	readyForFlush() bool
}

// flushableEntry wraps a flushable and adds additional metadata and
// functionality that is common to all flushables.
type flushableEntry struct {
	flushable
	// Channel which is closed when the flushable has been flushed.
	flushed chan struct{}
	// flushForced indicates whether a flush was forced on this memtable (either
	// manual, or due to ingestion). Protected by DB.mu.
	flushForced bool
	// delayedFlushForcedAt indicates whether a timer has been set to force a
	// flush on this memtable at some point in the future. Protected by DB.mu.
	// Holds the timestamp of when the flush will be issued.
	delayedFlushForcedAt time.Time
	// logNum corresponds to the WAL that contains the records present in the
	// receiver.
	logNum FileNum
	// logSize is the size in bytes of the associated WAL. Protected by DB.mu.
	logSize uint64
	// The current logSeqNum at the time the memtable was created. This is
	// guaranteed to be less than or equal to any seqnum stored in the memtable.
	logSeqNum uint64
	// readerRefs tracks the read references on the flushable. The two sources of
	// reader references are DB.mu.mem.queue and readState.memtables. The memory
	// reserved by the flushable in the cache is released when the reader refs
	// drop to zero. If the flushable is a memTable, when the reader refs drops
	// to zero, the writer refs will already be zero because the memtable will
	// have been flushed and that only occurs once the writer refs drops to zero.
	readerRefs int32
	// Closure to invoke to release memory accounting.
	releaseMemAccounting func()
}

func (e *flushableEntry) readerRef() {
	switch v := atomic.AddInt32(&e.readerRefs, 1); {
	case v <= 1:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	}
}

func (e *flushableEntry) readerUnref() {
	switch v := atomic.AddInt32(&e.readerRefs, -1); {
	case v < 0:
		panic(fmt.Sprintf("pebble: inconsistent reference count: %d", v))
	case v == 0:
		if e.releaseMemAccounting == nil {
			panic("pebble: memtable reservation already released")
		}
		e.releaseMemAccounting()
		e.releaseMemAccounting = nil
	}
}

type flushableList []*flushableEntry
