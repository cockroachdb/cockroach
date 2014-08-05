// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"sync"

	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
)

// A ReadQueue maintains an interval tree of keys or key ranges for
// pending writes. Reads to keys or key ranges are submitted to the
// ReadQueue in exchange for a WaitGroup synchronization primitive,
// which is signaled once any pending writes to overlapping keys have
// completed.
//
// Writes are added via ReadQueue.AddWrite(start, end Key), which
// returns a key for eventual removal.
//
// Readers are added to the queue via ReadQueue.AddRead(start, end
// Key). The returned WaitGroup is waited on by the caller for
// confirmation that all overlapping, pending writes have completed
// and the read can proceed.
//
// Once writes complete, ReadQueue.RemoveWrite(key interface{}) is
// invoked to remove the pending write and decrement the counts on any
// pending reader WaitGroups, possibly signaling waiting readers who
// were gated by the write's affected key(s).
//
// ReadQueue is not thread safe.
type ReadQueue struct {
	cache *util.IntervalCache
}

type write struct {
	pending []*sync.WaitGroup // Pending readers
}

// NewReadQueue returns a new read queue.
func NewReadQueue() *ReadQueue {
	rq := &ReadQueue{
		cache: util.NewIntervalCache(util.CacheConfig{Policy: util.CacheNone}),
	}
	rq.cache.OnEvicted = rq.onEvicted
	return rq
}

// onEvicted is called when any entry is removed from the interval
// tree. This happens on calls to RemoveWrite() and to Clear().
func (rq *ReadQueue) onEvicted(key, value interface{}) {
	w := value.(*write)
	for _, wg := range w.pending {
		wg.Done()
	}
}

// AddWrite adds a pending write which affects the specified key range.
// If end is nil, it is set to start, meaning the write affects a single
// key. The returned interface is the key for the write and must be
// re-supplied on subsequent invocation of RemoveWrite().
//
// AddWrite is invoked as a mutating command is added to the queue of
// Raft proposals. As multiple commands may be in the proposal state,
// writes may overlap.
func (rq *ReadQueue) AddWrite(start, end engine.Key) interface{} {
	if end == nil {
		end = engine.NextKey(start)
	}
	key := rq.cache.NewKey(rangeKey(start), rangeKey(end))
	rq.cache.Add(key, &write{})
	return key
}

// AddRead adds a read to the queue for the specified key range. If
// end is nil, end is set to start, meaning the read affects a single
// key. The supplied WaitGroup is incremented according to the number
// of pending writes with key(s) overlapping the key (start==end) or
// key range [start, end). The caller should call wg.Wait() to wait
// for confirmation that all pending writes have completed or failed.
func (rq *ReadQueue) AddRead(start, end engine.Key, wg *sync.WaitGroup) {
	if end == nil {
		end = engine.NextKey(start)
	}
	for _, w := range rq.cache.GetOverlaps(rangeKey(start), rangeKey(end)) {
		w := w.(*write)
		w.pending = append(w.pending, wg)
		wg.Add(1)
	}
}

// RemoveWrite is invoked to signal that the write associated with the
// specified key has completed and should be removed. Any readers
// currently gated by the write will be signaled if this is the only
// write upon which they are still pending.
//
// RemoveWrite is invoked after a mutating command has been committed
// to the Raft log and applied to the underlying state machine.
func (rq *ReadQueue) RemoveWrite(key interface{}) {
	rq.cache.Del(key)
}

// Clear removes all pending writes, signaling any waiting readers.
func (rq *ReadQueue) Clear() {
	rq.cache.Clear()
}
