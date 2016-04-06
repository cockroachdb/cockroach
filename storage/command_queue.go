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
	"container/heap"
	"sync"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/cache"
	"github.com/cockroachdb/cockroach/util/interval"
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
	cache     *cache.IntervalCache
	idAlloc   int64
	wRg, rwRg interval.RangeGroup // avoids allocating in GetWait.
	oHeap     overlapHeap         // avoids allocating in GetWait.
}

type cmd struct {
	ID       int64
	readOnly bool
	pending  []*sync.WaitGroup // pending commands gated on cmd.
}

// NewCommandQueue returns a new command queue.
func NewCommandQueue() *CommandQueue {
	cq := &CommandQueue{
		cache: cache.NewIntervalCache(cache.Config{Policy: cache.CacheNone}),
		wRg:   interval.NewRangeTree(),
		rwRg:  interval.NewRangeTree(),
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
		newCmdRange := interval.Range{
			Start: interval.Comparable(start),
			End:   interval.Comparable(end),
		}
		overlaps := cq.cache.GetOverlaps(newCmdRange.Start, newCmdRange.End)
		if readOnly {
			// If both commands are read-only, there are no dependencies between them,
			// so these can be filtered out of the overlapping commands.
			overlaps = filterReadWrite(overlaps)
		}

		// Sort overlapping commands by command ID and iterate from latest to earliest,
		// adding the commands' ranges to the RangeGroup to determine gating keyspace
		// command dependencies. Because all commands are given WaitGroup dependencies
		// to the most recent commands that they are dependent on, and because of the
		// causality provided by the strictly increasing command ID allocation, this
		// approach will construct a DAG-like dependency graph between WaitGroups with
		// overlapping keys. This comes as an alternative to creating explicit WaitGroups
		// dependencies to all gating commands for each new command, which could result
		// in an exponential dependency explosion.
		//
		// For example, consider the following 5 write commands, each with key ranges
		// represented on the x axis and WaitGroup dependencies represented by vertical
		// lines:
		//
		// cmd 1:   --------------
		//           |      |
		// cmd 2:    |  -------------
		//           |    |    |
		// cmd 3:    -------   |
		//                |    |
		// cmd 4:         -------
		//                   |
		// cmd 5:         -------
		//
		// Instead of having each command establish explicit dependencies on all previous
		// overlapping commands, each command only needs to establish explicit dependencies
		// on the set of overlapping commands closest to the new command that together span
		// the new commands overlapped range. Following this strategy, the other dependencies
		// will be implicitly enforced, which reduces memory utilization and synchronization
		// costs.
		//
		// The exception are existing reads: since reads don't wait for each other, an incoming
		// write must wait for reads even when they are covered by a "later" read (since that
		// "later" read won't wait for the earlier read to complete). However, if that read is
		// covered by a "later" write, we don't need to wait because writes can't be reordered.
		//
		// Two example of how this logic works are shown below. Notice in the first example how
		// the overlapping reads do not establish dependencies on each other, and can therefore
		// be reordered. Also notice in the second example that once read command 4 overlaps
		// a "later" write, it no longer needs to be a dependency for the new write command 5.
		// However, because read command 3 does not overlap a "later" write, it is still a
		// dependency for the new write, but can be safely reordered before or after command 4.
		//
		// cmd 1 [R]:                -----               ----------
		//                             |                        |
		// cmd 2 [W]:              ========                 ========
		//                          |   |                    |   |
		// cmd 3 [R]:             --+------                --+------
		//                          | |                      | |
		// cmd 4 [R]:          -------+-----        -----------+-----
		//                       |    |              |         |
		// cmd 5 [W]:   =====    |    |          =======       |
		//                |      |    |            |           |
		// cmd 5 [W]:   ====================     ====================
		//
		cq.oHeap.Init(overlaps)
		for enclosed := false; cq.oHeap.Len() > 0 && !enclosed; {
			o := cq.oHeap.PopOverlap()
			keyRange, cmd := o.Key.Range, o.Value.(*cmd)
			if cmd.readOnly {
				// If the current overlap is a read (meaning we're a write because other reads will
				// be filtered out if we're a read as well), we only need to wait if the write RangeGroup
				// doesn't already overlap the read. Otherwise, we know that this current read is a dependent
				// itself to a command already accounted for in out write RangeGroup. Either way, we need to add
				// this current command to the combined RangeGroup.
				cq.rwRg.Add(keyRange)
				if !cq.wRg.Overlaps(keyRange) {
					cmd.pending = append(cmd.pending, wg)
					wg.Add(1)
				}
			} else {
				// If the current overlap is a write, pick which RangeGroup will be used to determine necessary
				// dependencies based on if we are a read or write.
				overlapRg := cq.wRg
				if !readOnly {
					// We only use the combined read-write RangeGroup when we are a new write command, because
					// otherwise all read commands would have been filtered out so we can avoid using a second
					// RangeGroup. Here, the previous reads rely on a distinction between a write command RangeGroup
					// and an all command RangeGroup. This is so that they can avoid establishing a dependency
					// if they are already dependent on previous writes, but can remain independent from other
					// reads.
					overlapRg = cq.rwRg
				}

				// We only need to establish a dependency when this write command key range is not overlapping
				// any other reads or writes in its future. If it is overlapping, we know there was already a
				// dependency established with a dependent of the current overlap, meaning we already established
				// an implicit transitive dependency to the current overlap.
				if !overlapRg.Overlaps(keyRange) {
					cmd.pending = append(cmd.pending, wg)
					wg.Add(1)
				}

				// The current command is a write, so add it to the write RangeGroup and observe if the group grows.
				if cq.wRg.Add(keyRange) {
					// We can stop dependency creation early in the case that the write RangeGroup fully encloses
					// our new range, which means that no new dependencies are needed. This looks only at the
					// write RangeGroup because even if the combined range group encloses us, there can always be
					// more reads that are necessary dependencies if they themselves don't overlap any writes. We
					// only need to perform this check when the write RangeGroup grows.
					//
					// We check the write RangeGroup's length before checking if it encloses the new command's
					// range because we know (based on the fact that these are all overlapping commands) that the
					// RangeGroup can enclose us only if its length is 1 (meaning all ranges inserted have coalesced).
					// This guarantees that this enclosure check will always be run in constant time.
					if cq.wRg.Len() == 1 && cq.wRg.Encloses(newCmdRange) {
						enclosed = true
					}
				}

				// Make sure the current command's range gets added to the combined RangeGroup if we are using it.
				if overlapRg == cq.rwRg {
					cq.rwRg.Add(keyRange)
				}
			}
		}

		// Clear heap to avoid leaking anything it is currently storing.
		cq.oHeap.Clear()

		// Clear the RangeGroups so that they can be used again. This is an alternative
		// to using local variables that must be allocated in every iteration.
		cq.wRg.Clear()
		cq.rwRg.Clear()
	}
}

// filterReadWrite filters out the read-only commands from the provided slice.
func filterReadWrite(cmds []cache.Overlap) []cache.Overlap {
	rwIdx := len(cmds)
	for i := 0; i < rwIdx; {
		c := cmds[i].Value.(*cmd)
		if !c.readOnly {
			i++
		} else {
			cmds[i], cmds[rwIdx-1] = cmds[rwIdx-1], cmds[i]
			rwIdx--
		}
	}
	return cmds[:rwIdx]
}

// overlapHeap is a max-heap of cache.Overlaps, sorting the elements
// in decreasing Value.(*cmd).ID order.
type overlapHeap []cache.Overlap

func (o overlapHeap) Len() int { return len(o) }
func (o overlapHeap) Less(i, j int) bool {
	return o[i].Value.(*cmd).ID > o[j].Value.(*cmd).ID
}
func (o overlapHeap) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

func (o *overlapHeap) Push(x interface{}) {
	panic("unimplemented")
}

func (o *overlapHeap) Pop() interface{} {
	n := len(*o) - 1
	// Returning a pointer to avoid an allocation when storing the cache.Overlap in an interface{}.
	// A *cache.Overlap stored in an interface{} won't allocate, but the value pointed to may
	// change if the heap is later modified, so the pointer should be dereferenced immediately.
	x := &(*o)[n]
	*o = (*o)[:n]
	return x
}

func (o *overlapHeap) Init(overlaps []cache.Overlap) {
	*o = overlaps
	heap.Init(o)
}

func (o *overlapHeap) Clear() {
	*o = nil
}

func (o *overlapHeap) PopOverlap() cache.Overlap {
	x := heap.Pop(o)
	return *x.(*cache.Overlap)
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
			start = end[:len(start)]
		}
		alloc := struct {
			key   cache.IntervalKey
			value cmd
			entry cache.Entry
		}{
			key: cq.cache.MakeKey(start, end),
			value: cmd{
				ID:       cq.nextID(),
				readOnly: readOnly,
			},
		}
		alloc.entry.Key = &alloc.key
		alloc.entry.Value = &alloc.value
		cq.cache.AddEntry(&alloc.entry)
		r = append(r, &alloc.key)
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

func (cq *CommandQueue) nextID() int64 {
	cq.idAlloc++
	return cq.idAlloc
}
