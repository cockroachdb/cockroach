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
	"bytes"
	"container/heap"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// A CommandQueue maintains an interval tree of keys or key ranges for
// executing commands. New commands affecting keys or key ranges must
// wait on already-executing commands which overlap their key range.
//
// Before executing, a command invokes getWait() to acquire a slice of
// channels belonging to overlapping commands which are already
// running. Each channel is waited on by the caller for confirmation
// that all overlapping, pending commands have completed and the
// pending command can proceed.
//
// After waiting, a command is added to the queue's already-executing
// set via add(). add accepts a parameter indicating whether the
// command is read-only. Read-only commands don't need to wait on other
// read-only commands, so the channels returned via getWait() don't
// include read-only on read-only overlapping commands as an
// optimization.
//
// Once commands complete, remove() is invoked to remove the executing
// command and close its channel, possibly signaling waiting commands
// who were gated by the executing command's affected key(s).
//
// CommandQueue is not thread safe.
type CommandQueue struct {
	reads     interval.Tree
	writes    interval.Tree
	idAlloc   int64
	wRg, rwRg interval.RangeGroup // avoids allocating in getWait
	oHeap     overlapHeap         // avoids allocating in getWait
	overlaps  []*cmd              // avoids allocating in getOverlaps

	coveringOptimization bool // if true, use covering span optimization

	// Used to temporarily store metrics local to a single CommandQueue. These
	// will periodically be processed by the Store.
	localMetrics struct {
		readCommands    int64
		writeCommands   int64
		maxOverlapsSeen int64 // will be reset to 0 during metrics processing.
	}
}

type cmd struct {
	id        int64
	key       interval.Range
	readOnly  bool
	timestamp hlc.Timestamp
	expanded  bool          // have the children been added
	pending   chan struct{} // closed when complete
	children  []cmd
}

// ID implements interval.Interface.
func (c *cmd) ID() uintptr {
	return uintptr(c.id)
}

// Range implements interval.Interface.
func (c *cmd) Range() interval.Range {
	return c.key
}

// cmdCount returns the number of spans in c, taking into account the
// "covering" optimization (see CommandQueue.add). If a cmd was added to the
// CommandQueue with only a single span, it will have 0 children, behaving like
// the optimization does not exist. If a cmd was added to the CommandQueue with
// multiple spans, each span will be retained as a child command in a covering cmd,
// even if the covering cmd is expanded. As a result, len(c.children) will never be 1.
func (c *cmd) cmdCount() int {
	if len(c.children) == 0 {
		return 1
	}
	return len(c.children)
}

func (c *cmd) String() string {
	if c == nil {
		return "<nil>"
	}
	var buf bytes.Buffer
	var readOnly string
	if c.readOnly {
		readOnly = " readonly"
	}
	fmt.Fprintf(&buf, "%d%s [%s", c.id, readOnly, roachpb.Key(c.key.Start))
	if !roachpb.Key(c.key.End).Equal(roachpb.Key(c.key.Start).Next()) {
		fmt.Fprintf(&buf, ",%s", roachpb.Key(c.key.End))
	}
	fmt.Fprintf(&buf, ")")

	if !c.expanded {
		for i := range c.children {
			fmt.Fprintf(&buf, "\n    %d: %s", i, &c.children[i])
		}
	}
	return buf.String()
}

// NewCommandQueue returns a new command queue. The boolean specifies whether
// to enable the covering span optimization. With this optimization, whenever
// a command consisting of multiple spans is added, a covering span is computed
// and only that covering span inserted. The individual spans are inserted
// (i.e. the covering span expanded) only when required by a later overlapping
// command, the hope being that that occurs infrequently, and that in the
// common case savings are made due to the reduced number of spans active in
// the tree.
// As such, the optimization makes sense for workloads in which commands
// typically contain many spans, but are spatially disjoint.
func NewCommandQueue(coveringOptimization bool) *CommandQueue {
	cq := &CommandQueue{
		reads:                interval.Tree{Overlapper: interval.Range.OverlapExclusive},
		writes:               interval.Tree{Overlapper: interval.Range.OverlapExclusive},
		wRg:                  interval.NewRangeTree(),
		rwRg:                 interval.NewRangeTree(),
		coveringOptimization: coveringOptimization,
	}
	return cq
}

// String dumps the contents of the command queue for testing.
func (cq *CommandQueue) String() string {
	var buf bytes.Buffer
	var keysPrinted int
	const keysToPrint = 10
	f := func(i interval.Interface) bool {
		fmt.Fprintf(&buf, "  %s\n", i)
		keysPrinted++
		return keysPrinted >= keysToPrint
	}

	cq.reads.Do(f)
	if keysPrinted >= keysToPrint {
		fmt.Fprintf(&buf, "  ...remaining %d reads omitted\n", cq.reads.Len()-keysPrinted)
	}
	keysPrinted = 0

	cq.writes.Do(f)
	if keysPrinted >= keysToPrint {
		fmt.Fprintf(&buf, "  ...remaining %d writes omitted", cq.writes.Len()-keysPrinted)
	}
	keysPrinted = 0

	return buf.String()
}

// prepareSpans ensures the spans all have an end key. Note that this function
// mutates its arguments.
func prepareSpans(spans []roachpb.Span) {
	for i, span := range spans {
		// This gives us a memory-efficient end key if end is empty.
		if len(span.EndKey) == 0 {
			span.EndKey = span.Key.Next()
			span.Key = span.EndKey[:len(span.Key)]
			spans[i] = span
		}
	}
}

// expand replaces the command with its children, returning true if work was
// done in the process. The boolean parameter must be true if the covering span
// was previously inserted into the tree.
func (cq *CommandQueue) expand(c *cmd, isInserted bool) bool {
	if c.expanded || len(c.children) == 0 {
		return false
	}
	c.expanded = true

	tree := cq.tree(c)
	if isInserted {
		if err := tree.Delete(c, false /* !fast */); err != nil {
			panic(err)
		}
	}
	for i := range c.children {
		child := &c.children[i]
		if err := tree.Insert(child, false /* !fast */); err != nil {
			panic(err)
		}
	}
	return true
}

// getWait returns a slice of the pending channels of executing
// commands which overlap the specified key ranges. The caller should
// call wg.Wait() to fetch the required wait channels. The caller
// should then invoke add() to add the keys to the command queue and
// then wait for confirmation that all gating commands have completed
// or failed. readOnly is true if the requester is a read-only
// command; false for read-write. The provided timestamp, if non-zero,
// is used to allow reads to proceed if they are at earlier timestamps
// than pending writes, and writes to proceed if they are at later
// timestamps than pending reads.
func (cq *CommandQueue) getWait(
	readOnly bool, timestamp hlc.Timestamp, spans []roachpb.Span,
) (chans []<-chan struct{}) {
	prepareSpans(spans)

	for i := 0; i < len(spans); i++ {
		span := spans[i]
		if span.EndKey == nil {
			panic(fmt.Sprintf("%d: unexpected nil EndKey: %s", i, span))
		}
		newCmdRange := span.AsRange()
		overlaps := cq.getOverlaps(readOnly, timestamp, newCmdRange)

		// Check to see if any of the overlapping entries are "covering"
		// entries. If we encounter a covering entry, we remove it from the
		// interval tree and add all of its children.
		restart := false
		for _, c := range overlaps {
			// Operand order matters: call cq.expand() for its side effects
			// even if `restart` is already true.
			restart = cq.expand(c, true /* isInserted */) || restart
		}
		if restart {
			i--
			continue
		}
		if overlapCount := int64(len(overlaps)); overlapCount > cq.localMetrics.maxOverlapsSeen {
			cq.localMetrics.maxOverlapsSeen = overlapCount
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
		// the new command's overlapped range. Following this strategy, the other dependencies
		// will be implicitly enforced, which reduces memory utilization and synchronization
		// costs.
		//
		// The exception are existing reads: since reads don't wait for each other, an incoming
		// write must wait for reads even when they are covered by a "later" read (since that
		// "later" read won't wait for the earlier read to complete). However, if that read is
		// covered by a "later" write, we don't need to wait because writes can't be reordered.
		//
		// Two examples of how this logic works are shown below. Notice in the first example how
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
		// Things get more interesting with timestamps:
		// -------------------------------------------
		// - For a read-only command, overlaps will include only writes which have occurred
		//   with earlier timestamps. Because writes all must depend on each other, things
		//   work as expected.
		//
		// - Write commands overlap both reads and writes. The writes that a write command
		//   overlaps will depend reliably on each other if they in turn overlap. However, reads
		//   that a write command overlaps may not in turn be depended on by overlapping writes,
		//   if the reads have earlier timestamps. This means that writes don't necessarily
		//   subsume overlapping reads.
		//
		//   We solve this problem by always including read commands with timestamps less than
		//   the latest write timestamp seen so far, which guarantees that we will wait on any
		//   reads which might not be dependend on by writes with higher IDs. Similarly, we
		//   include write commands with timestamps greater than or equal to the earliest
		//   read timestamp seen so far.
		//
		// TODO(spencer): this mechanism is a blunt instrument and will lead to reads rarely
		//   being consolidated because of range group overlaps.
		maxWriteTS, minReadTS := hlc.Timestamp{}, hlc.MaxTimestamp
		cq.oHeap.Init(overlaps)
		for cq.oHeap.Len() > 0 {
			cmd := cq.oHeap.PopOverlap()
			keyRange := cmd.key
			cmdHasTimestamp := cmd.timestamp != hlc.Timestamp{}
			mustWait := false

			if cmd.readOnly {
				if cmdHasTimestamp {
					if cmd.timestamp.Less(minReadTS) {
						minReadTS = cmd.timestamp
					}
					if cmd.timestamp.Less(maxWriteTS) {
						mustWait = true
					}
				}
				// If the current overlap is a read (meaning we're a write because other reads will
				// be filtered out if we're a read as well), we only need to wait if the write RangeGroup
				// doesn't already overlap the read. Otherwise, we know that this current read is a dependent
				// itself to a command already accounted for in our write RangeGroup. Either way, we need to add
				// this current command to the combined RangeGroup.
				cq.rwRg.Add(keyRange)
				if mustWait || !cq.wRg.Overlaps(keyRange) {
					if cmd.pending == nil {
						cmd.pending = make(chan struct{})
					}
					chans = append(chans, cmd.pending)
				}
			} else {
				if cmdHasTimestamp {
					if maxWriteTS.Less(cmd.timestamp) {
						maxWriteTS = cmd.timestamp
					}
					if minReadTS.Less(cmd.timestamp) {
						mustWait = true
					}
				}
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
				if mustWait || !overlapRg.Overlaps(keyRange) {
					if cmd.pending == nil {
						cmd.pending = make(chan struct{})
					}
					chans = append(chans, cmd.pending)
				}

				// The current command is a write, so add it to the write RangeGroup.
				cq.wRg.Add(keyRange)

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
func (cq *CommandQueue) getOverlaps(
	readOnly bool, timestamp hlc.Timestamp, rng interval.Range,
) []*cmd {
	if !readOnly {
		cq.reads.DoMatching(func(i interval.Interface) bool {
			c := i.(*cmd)
			// Writes only wait on equal or later reads (we always wait
			// if the pending read didn't have a timestamp specified).
			if (c.timestamp == hlc.Timestamp{}) || !c.timestamp.Less(timestamp) {
				cq.overlaps = append(cq.overlaps, c)
			}
			return false
		}, rng)
	}
	// Both reads and writes must wait on other writes, depending on timestamps.
	cq.writes.DoMatching(func(i interval.Interface) bool {
		c := i.(*cmd)
		// Writes always wait on other writes. Reads must wait on writes
		// which occur at the same or an earlier timestamp. Note that
		// timestamps for write commands may be pushed forward by the
		// timestamp cache. This is fine because it doesn't matter how far
		// forward the timestamp is pushed if it's already ahead of this read.
		if !readOnly || (timestamp == hlc.Timestamp{}) || !timestamp.Less(c.timestamp) {
			cq.overlaps = append(cq.overlaps, c)
		}
		return false
	}, rng)
	overlaps := cq.overlaps
	cq.overlaps = cq.overlaps[:0]
	return overlaps
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
// Either all supplied spans must be range-global or range-local. Failure to
// obey with this restriction results in a fatal error.
//
// Returns a nil `cmd` when no spans are given.
//
// add should be invoked after waiting on already-executing, overlapping
// commands via the WaitGroup initialized through getWait().
func (cq *CommandQueue) add(readOnly bool, timestamp hlc.Timestamp, spans []roachpb.Span) *cmd {
	if len(spans) == 0 {
		return nil
	}
	prepareSpans(spans)

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
	coveringSpan := roachpb.Span{
		Key:    minKey,
		EndKey: maxKey,
	}

	if keys.IsLocal(minKey) != keys.IsLocal(maxKey) {
		log.Fatalf(
			context.TODO(),
			"mixed range-global and range-local keys: %s and %s",
			minKey, maxKey,
		)
	}

	numCmds := 1
	if len(spans) > 1 {
		numCmds += len(spans)
	}
	cmds := make([]cmd, numCmds)

	// Create the covering entry.
	cmd := &cmds[0]
	cmd.id = cq.nextID()
	cmd.key = coveringSpan.AsRange()
	cmd.readOnly = readOnly
	cmd.timestamp = timestamp
	cmd.expanded = false

	if len(spans) > 1 {
		// Populate the covering entry's children.
		cmd.children = cmds[1:]
		for i, span := range spans {
			child := &cmd.children[i]
			child.id = cq.nextID()
			child.key = span.AsRange()
			child.readOnly = readOnly
			child.timestamp = timestamp
			child.expanded = true
		}
	}

	if cmd.readOnly {
		cq.localMetrics.readCommands += int64(cmd.cmdCount())
	} else {
		cq.localMetrics.writeCommands += int64(cmd.cmdCount())
	}

	if cq.coveringOptimization || len(spans) == 1 {
		tree := cq.tree(cmd)
		if err := tree.Insert(cmd, false /* !fast */); err != nil {
			panic(err)
		}
	} else {
		cq.expand(cmd, false /* !isInserted */)
	}
	return cmd
}

// remove is invoked to signal that the command associated with the
// specified key has completed and should be removed. Any pending
// commands waiting on this command will be signaled if this is the
// only command upon which they are still waiting.
//
// Removing a `nil` cmd is a no-op.
func (cq *CommandQueue) remove(cmd *cmd) {
	if cmd == nil {
		return
	}

	if cmd.readOnly {
		cq.localMetrics.readCommands -= int64(cmd.cmdCount())
	} else {
		cq.localMetrics.writeCommands -= int64(cmd.cmdCount())
	}

	tree := cq.tree(cmd)
	if !cmd.expanded {
		n := tree.Len()
		if err := tree.Delete(cmd, false /* !fast */); err != nil {
			panic(err)
		}
		if d := n - tree.Len(); d != 1 {
			panic(fmt.Sprintf("%d: expected 1 deletion, found %d", cmd.id, d))
		}
		if ch := cmd.pending; ch != nil {
			close(ch)
		}
	} else {
		for i := range cmd.children {
			child := &cmd.children[i]
			n := tree.Len()
			if err := tree.Delete(child, false /* !fast */); err != nil {
				panic(err)
			}
			if d := n - tree.Len(); d != 1 {
				panic(fmt.Sprintf("%d: expected 1 deletion, found %d", child.id, d))
			}
			if ch := child.pending; ch != nil {
				close(ch)
			}
		}
	}
}

func (cq *CommandQueue) tree(c *cmd) *interval.Tree {
	if c.readOnly {
		return &cq.reads
	}
	return &cq.writes
}

func (cq *CommandQueue) nextID() int64 {
	cq.idAlloc++
	return cq.idAlloc
}

func (cq *CommandQueue) treeSize() int {
	return cq.reads.Len() + cq.writes.Len()
}
