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
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"sync"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/cache"
)

// A CommandQueue maintains an interval tree of keys or key ranges for
// executing commands. New commands affecting keys or key ranges must
// wait on already-executing commands which overlap their key range.
//
// Before executing, a command invokes GetWait() to initialize a
// WaitGroup with the number of overlapping commands which are already
// running. The wait group is waited on by the caller for confirmation
// that all overlapping, pending commands have completed and the
// pending command can proceed.
//
// After waiting, a command is added to the queue's already-executing
// set via Add(). Add accepts a parameter indicating whether the
// command is read-only. Read-only commands don't need to wait on
// other read-only commands, so the wait group returned via GetWait()
// doesn't include read-only on read-only overlapping commands as an
// optimization.
//
// Once commands complete, Remove() is invoked to remove the executing
// command and decrement the counts on any pending WaitGroups,
// possibly signaling waiting commands who were gated by the executing
// command's affected key(s).
//
// CommandQueue is not thread safe.
type CommandQueue struct {
	cache *cache.IntervalCache
}

type cmd struct {
	readOnly bool
	pending  []*sync.WaitGroup // Pending commands gated on cmd
}

// NewCommandQueue returns a new command queue.
func NewCommandQueue() *CommandQueue {
	cq := &CommandQueue{
		cache: cache.NewIntervalCache(cache.Config{Policy: cache.CacheNone}),
	}
	cq.cache.OnEvicted = cq.onEvicted
	return cq
}

// onEvicted is called when any entry is removed from the interval
// tree. This happens on calls to Remove() and to Clear().
func (cq *CommandQueue) onEvicted(key, value interface{}) {
	c := value.(*cmd)
	for _, wg := range c.pending {
		wg.Done()
	}
}

// GetWait initializes the supplied wait group with the number of executing
// commands which overlap the specified key ranges. If an end key is empty, it
// only affects the start key. The caller should call wg.Wait() to wait for
// confirmation that all gating commands have completed or failed, and then
// call Add() to add the keys to the command queue. readOnly is true if the
// requester is a read-only command; false for read-write.
func (cq *CommandQueue) GetWait(readOnly bool, wg *sync.WaitGroup, spans ...roachpb.Span) {
	for _, span := range spans {
		// This gives us a memory-efficient end key if end is empty.
		start, end := span.Key, span.EndKey
		if len(end) == 0 {
			end = start.Next()
			start = end[:len(start)]
		}
		for _, c := range cq.cache.GetOverlaps(start, end) {
			c := c.Value.(*cmd)
			// Only add to the wait group if one of the commands isn't read-only.
			if !readOnly || !c.readOnly {
				c.pending = append(c.pending, wg)
				wg.Add(1)
			}
		}
	}
}

// Add adds commands to the queue which affect the specified key ranges. Ranges
// without an end key affect only the start key. The returned interface is the
// key for the command queue and must be re-supplied on subsequent invocation
// of Remove().
//
// Add should be invoked after waiting on already-executing, overlapping
// commands via the WaitGroup initialized through GetWait().
func (cq *CommandQueue) Add(readOnly bool, spans ...roachpb.Span) []interface{} {
	r := make([]interface{}, 0, len(spans))
	for _, span := range spans {
		start, end := span.Key, span.EndKey
		if len(end) == 0 {
			end = start.Next()
		}
		key := cq.cache.NewKey(start, end)
		cq.cache.Add(key, &cmd{readOnly: readOnly})
		r = append(r, key)
	}
	return r
}

// Remove is invoked to signal that the command associated with the
// specified key has completed and should be removed. Any pending
// commands waiting on this command will be signaled if this is the
// only command upon which they are still waiting.
//
// Remove is invoked after a mutating command has been committed to
// the Raft log and applied to the underlying state machine. Similarly,
// Remove is invoked after a read-only command has been executed
// against the underlying state machine.
func (cq *CommandQueue) Remove(keys []interface{}) {
	for _, k := range keys {
		cq.cache.Del(k)
	}
}

// Clear removes all executing commands, signaling any waiting commands.
func (cq *CommandQueue) Clear() {
	cq.cache.Clear()
}
