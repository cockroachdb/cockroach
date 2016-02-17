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
	"sort"
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
	cache   *cache.IntervalCache
	idAlloc int64
	rg      interval.RangeGroup // avoids allocating in GetWait.
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
		rg:    interval.NewRangeTree(),
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
		overlaps := cq.cache.GetOverlaps(start, end)
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
		// For example, consider the following 5 commands, each with key ranges represented
		// on the x axis and WaitGroup dependencies represented by vertical lines:
		//
		// cmd 1:  --------------
		//          |      |
		// cmd 2:   |  -------------
		//          |    |    |
		// cmd 3:   -------   |
		//               |    |
		// cmd 4:        -------
		//                  |
		// cmd 5:        -------
		//
		// Instead of having each command establish explicit dependencies on all previous
		// overlapping commands, each command only needs to establish explicit dependencies
		// on the set of overlapping commands closest to the new command that together span
		// the new commands overlapped range. Following this strategy, the other dependencies
		// will be implicitly enforced, which reduces memory utilization and synchronization
		// costs.
		sort.Sort(overlapSorter(overlaps))
		for i := len(overlaps) - 1; i >= 0; i-- {
			c := overlaps[i]
			if cq.rg.Add(c.Key.Range) {
				c := c.Value.(*cmd)
				c.pending = append(c.pending, wg)
				wg.Add(1)
			}
		}

		// Clear the RangeGroup so that it can be used again. This is an alternative
		// to using a local variable that must be allocated in every iteration.
		cq.rg.Clear()
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

// overlapSorter attaches the methods of sort.Interface to []cache.Overlap, sorting
// in increasing ID order.
type overlapSorter []cache.Overlap

func (o overlapSorter) Len() int { return len(o) }
func (o overlapSorter) Less(i, j int) bool {
	return o[i].Value.(*cmd).ID < o[j].Value.(*cmd).ID
}
func (o overlapSorter) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

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
