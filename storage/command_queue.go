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
	"fmt"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/interval"
)

// A CommandQueue maintains an interval tree of keys or key ranges for
// executing commands. New commands affecting keys or key ranges must
// wait on already-executing commands which overlap their key range.
//
// Before executing, a command invokes GetWait() to acquire a slice of
// channels belonging to overlapping commands which are already
// running. Each channel is waited on by the caller for confirmation
// that all overlapping, pending commands have completed and the
// pending command can proceed.
//
// After waiting, a command is added to the queue's already-executing
// set via add(). add accepts a parameter indicating whether the
// command is read-only. Read-only commands don't need to wait on other
// read-only commands, so the channels returned via GetWait() don't
// include read-only on read-only overlapping commands as an
// optimization.
//
// Once commands complete, remove() is invoked to remove the executing
// command and close its channel, possibly signaling waiting commands
// who were gated by the executing command's affected key(s).
//
// CommandQueue is not thread safe.
type CommandQueue struct {
	tree      interval.Tree
	idAlloc   int64
	wRg, rwRg interval.RangeGroup // avoids allocating in GetWait
	oHeap     overlapHeap         // avoids allocating in GetWait
	overlaps  []*cmd              // avoids allocating in getOverlaps
}

type cmd struct {
	id       int64
	key      interval.Range
	readOnly bool
	expanded bool          // have the children been added
	pending  chan struct{} // closed when complete
	children []cmd
}

// ID implements interval.Interface.
func (c *cmd) ID() uintptr {
	return uintptr(c.id)
}

// Range implements interval.Interface.
func (c *cmd) Range() interval.Range {
	return c.key
}

// NewCommandQueue returns a new command queue.
func NewCommandQueue() *CommandQueue {
	cq := &CommandQueue{
		tree: interval.Tree{Overlapper: interval.Range.OverlapExclusive},
		wRg:  interval.NewRangeTree(),
		rwRg: interval.NewRangeTree(),
	}
	return cq
}

// prepareSpans ensures the spans all have an end key. Note that this function
// mutates its arguments.
func prepareSpans(spans ...roachpb.Span) {
	for i, span := range spans {
		// This gives us a memory-efficient end key if end is empty.
		if len(span.EndKey) == 0 {
			span.EndKey = span.Key.Next()
			span.Key = span.EndKey[:len(span.Key)]
			spans[i] = span
		}
	}
}

// GetWait returns a slice of the pending channels of executing commands which
// overlap the specified key ranges. If an end key is empty, it only affects
// the start key. The caller should call wg.Wait() to wait for confirmation
// that all gating commands have completed or failed, and then call add() to
// add the keys to the command queue. readOnly is true if the requester is a
// read-only command; false for read-write.
func (cq *CommandQueue) getWait(readOnly bool, spans ...roachpb.Span) (chans []<-chan struct{}) {
	prepareSpans(spans...)

	for i := 0; i < len(spans); i++ {
		span := spans[i]
		start, end := span.Key, span.EndKey
		if end == nil {
			panic(fmt.Sprintf("%d: unexpected nil EndKey: %s", i, span))
		}
		newCmdRange := interval.Range{
			Start: interval.Comparable(start),
			End:   interval.Comparable(end),
		}
		overlaps := cq.getOverlaps(newCmdRange.Start, newCmdRange.End)
		if readOnly {
			// If both commands are read-only, there are no dependencies between them,
			// so these can be filtered out of the overlapping commands.
			overlaps = filterReadWrite(overlaps)
		}

		// Check to see if any of the overlapping entries are "covering"
		// entries. If we encounter a covering entry, we remove it from the
		// interval tree and add all of its children.
		restart := false
		for _, cmd := range overlaps {
			if !cmd.expanded && len(cmd.children) != 0 {
				restart = true
				cmd.expanded = true
				if err := cq.tree.Delete(cmd, false /* !fast */); err != nil {
					panic(err)
				}
				for i := range cmd.children {
					child := &cmd.children[i]
					if err := cq.tree.Insert(child, false /* !fast */); err != nil {
						panic(err)
					}
				}
			}
		}
		if restart {
			i--
			continue
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
			cmd := cq.oHeap.PopOverlap()
			keyRange := cmd.key
			if cmd.readOnly {
				// If the current overlap is a read (meaning we're a write because other reads will
				// be filtered out if we're a read as well), we only need to wait if the write RangeGroup
				// doesn't already overlap the read. Otherwise, we know that this current read is a dependent
				// itself to a command already accounted for in out write RangeGroup. Either way, we need to add
				// this current command to the combined RangeGroup.
				cq.rwRg.Add(keyRange)
				if !cq.wRg.Overlaps(keyRange) {
					if cmd.pending == nil {
						cmd.pending = make(chan struct{})
					}
					chans = append(chans, cmd.pending)
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
					if cmd.pending == nil {
						cmd.pending = make(chan struct{})
					}
					chans = append(chans, cmd.pending)
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
	return chans
}

// getOverlaps returns a slice of values which overlap the specified
// interval. The slice is only valid until the next call to GetOverlaps.
func (cq *CommandQueue) getOverlaps(start, end []byte) []*cmd {
	rng := interval.Range{
		Start: interval.Comparable(start),
		End:   interval.Comparable(end),
	}
	cq.tree.DoMatching(cq.doOverlaps, rng)
	overlaps := cq.overlaps
	cq.overlaps = cq.overlaps[:0]
	return overlaps
}

func (cq *CommandQueue) doOverlaps(i interval.Interface) bool {
	c := i.(*cmd)
	cq.overlaps = append(cq.overlaps, c)
	return false
}

// filterReadWrite filters out the read-only commands from the provided slice.
func filterReadWrite(cmds []*cmd) []*cmd {
	rwIdx := len(cmds)
	for i := 0; i < rwIdx; {
		c := cmds[i]
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
// in decreasing Value.(*cmd).id order.
type overlapHeap []*cmd

func (o overlapHeap) Len() int { return len(o) }
func (o overlapHeap) Less(i, j int) bool {
	return o[i].id > o[j].id
}
func (o overlapHeap) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

func (o *overlapHeap) Push(x interface{}) {
	panic("unimplemented")
}

func (o *overlapHeap) Pop() interface{} {
	n := len(*o) - 1
	x := (*o)[n]
	*o = (*o)[:n]
	return x
}

func (o *overlapHeap) Init(overlaps []*cmd) {
	*o = overlaps
	heap.Init(o)
}

func (o *overlapHeap) Clear() {
	*o = nil
}

func (o *overlapHeap) PopOverlap() *cmd {
	x := heap.Pop(o)
	return x.(*cmd)
}

// add adds commands to the queue which affect the specified key ranges. Ranges
// without an end key affect only the start key. The returned interface is the
// key for the command queue and must be re-supplied on subsequent invocation
// of remove().
//
// add should be invoked after waiting on already-executing, overlapping
// commands via the WaitGroup initialized through getWait().
func (cq *CommandQueue) add(readOnly bool, spans ...roachpb.Span) *cmd {
	prepareSpans(spans...)

	// Compute the min and max key that covers all of the spans.
	minKey, maxKey := spans[0].Key, spans[0].EndKey
	for i := 1; i < len(spans); i++ {
		start, end := spans[i].Key, spans[i].EndKey
		if minKey.Compare(start) > 0 {
			minKey = start
		}
		if maxKey.Compare(end) < 0 {
			maxKey = end
		}
	}

	numCmds := 1
	if len(spans) > 1 {
		numCmds += len(spans)
	}
	cmds := make([]cmd, numCmds)

	// Create the covering entry. Note that this may have an "illegal" key range
	// spanning from range-local to range-global, but that's acceptable here as
	// long as we're careful in the future.
	cmd := &cmds[0]
	cmd.id = cq.nextID()
	cmd.key = interval.Range{
		Start: interval.Comparable(minKey),
		End:   interval.Comparable(maxKey),
	}
	cmd.readOnly = readOnly
	cmd.expanded = false

	if len(spans) > 1 {
		// Populate the covering entry's children.
		cmd.children = cmds[1:]
		for i, span := range spans {
			child := &cmd.children[i]
			child.id = cq.nextID()
			child.key = interval.Range{
				Start: interval.Comparable(span.Key),
				End:   interval.Comparable(span.EndKey),
			}
			child.readOnly = readOnly
			child.expanded = true
		}
	}

	if err := cq.tree.Insert(cmd, false /* !fast */); err != nil {
		panic(err)
	}
	return cmd
}

// remove is invoked to signal that the command associated with the
// specified key has completed and should be removed. Any pending
// commands waiting on this command will be signaled if this is the
// only command upon which they are still waiting.
func (cq *CommandQueue) remove(cmd *cmd) {
	if cmd == nil {
		return
	}

	if !cmd.expanded {
		n := cq.tree.Len()
		if err := cq.tree.Delete(cmd, false /* !fast */); err != nil {
			panic(err)
		}
		if d := n - cq.tree.Len(); d != 1 {
			panic(fmt.Sprintf("%d: expected 1 deletion, found %d", cmd.id, d))
		}
		if ch := cmd.pending; ch != nil {
			close(ch)
		}
	} else {
		for i := range cmd.children {
			child := &cmd.children[i]
			n := cq.tree.Len()
			if err := cq.tree.Delete(child, false /* !fast */); err != nil {
				panic(err)
			}
			if d := n - cq.tree.Len(); d != 1 {
				panic(fmt.Sprintf("%d: expected 1 deletion, found %d", child.id, d))
			}
			if ch := child.pending; ch != nil {
				close(ch)
			}
		}
	}
}

func (cq *CommandQueue) nextID() int64 {
	cq.idAlloc++
	return cq.idAlloc
}
