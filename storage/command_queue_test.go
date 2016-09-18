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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func getPrereqs(cq *CommandQueue, from, to roachpb.Key, readOnly bool) []*cmd {
	return cq.getPrereqs(readOnly, roachpb.Span{Key: from, EndKey: to})
}

func add(cq *CommandQueue, from, to roachpb.Key, readOnly bool, prereqs []*cmd) *cmd {
	return cq.add(readOnly, prereqs, roachpb.Span{Key: from, EndKey: to})
}

func getPrereqsAndAdd(cq *CommandQueue, from, to roachpb.Key, readOnly bool) ([]*cmd, *cmd) {
	prereqs := getPrereqs(cq, from, to, readOnly)
	return prereqs, add(cq, from, to, readOnly, prereqs)
}

func waitCmdDone(prereqs []*cmd) {
	for _, prereq := range prereqs {
		<-prereq.pending
	}
}

// testCmdDone waits for the prereqs' pending channels to be closed for at
// most the specified wait duration. Returns true if the command finished in
// the allotted time, false otherwise.
func testCmdDone(prereqs []*cmd, wait time.Duration) bool {
	t := time.After(wait)
	for _, prereq := range prereqs {
		select {
		case <-t:
			return false
		case <-prereq.pending:
		}
	}
	return true
}

func TestCommandQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()

	// Try a command with no overlapping already-running commands.
	waitCmdDone(getPrereqs(cq, roachpb.Key("a"), nil, false))
	waitCmdDone(getPrereqs(cq, roachpb.Key("a"), roachpb.Key("b"), false))

	// Add a command and verify dependency on it.
	wk := add(cq, roachpb.Key("a"), nil, false, nil)
	prereqs := getPrereqs(cq, roachpb.Key("a"), nil, false)
	if testCmdDone(prereqs, 1*time.Millisecond) {
		t.Fatal("command should not finish with command outstanding")
	}
	cq.remove(wk)
	if !testCmdDone(prereqs, 5*time.Millisecond) {
		t.Fatal("command should finish with no commands outstanding")
	}
}

// TestCommandQueueWriteWaitForNonAdjacentRead tests that the command queue
// lets a writer wait for a read which is separated from it through another
// read. Since reads don't wait for reads, there was a bug in which the writer
// would wind up waiting only for one of the two readers under it.
func TestCommandQueueWriteWaitForNonAdjacentRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()
	key := roachpb.Key("a")
	// Add a read-only command.
	wk1 := add(cq, key, nil, true, nil)
	// Add another one on top.
	wk2 := add(cq, key, nil, true, nil)

	// A write should have to wait for **both** reads, not only the second
	// one.
	prereqs := getPrereqs(cq, key, nil, false /* !readOnly */)

	assert := func(blocked bool) {
		d := time.Millisecond
		if !blocked {
			d *= 5
		}
		f, l, _ := caller.Lookup(1)
		if testCmdDone(prereqs, d) {
			if blocked {
				t.Fatalf("%s:%d: command should not finish with command outstanding", f, l)
			}
		} else if !blocked {
			t.Fatalf("%s:%d: command should not have been blocked", f, l)
		}
	}

	// Certainly blocks now.
	assert(true)

	// The second read returns, but the first one remains.
	cq.remove(wk2)

	// Should still block. This being broken is why this test exists.
	assert(true)

	// First read returns.
	cq.remove(wk1)

	// Now it goes through.
	assert(false)
}

func TestCommandQueueNoWaitOnReadOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()
	// Add a read-only command.
	prereqs1, wk := getPrereqsAndAdd(cq, roachpb.Key("a"), nil, true)
	// Verify no wait on another read-only command.
	waitCmdDone(prereqs1)
	// Verify wait with a read-write command.
	prereqs2 := getPrereqs(cq, roachpb.Key("a"), nil, false)
	if testCmdDone(prereqs2, 1*time.Millisecond) {
		t.Fatal("command should not finish with command outstanding")
	}
	cq.remove(wk)
	if !testCmdDone(prereqs2, 5*time.Millisecond) {
		t.Fatal("command should finish with no commands outstanding")
	}
}

func TestCommandQueueMultipleExecutingCommands(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()

	// Add multiple commands and add a command which overlaps them all.
	wk1 := add(cq, roachpb.Key("a"), nil, false, nil)
	wk2 := add(cq, roachpb.Key("b"), roachpb.Key("c"), false, nil)
	wk3 := add(cq, roachpb.Key("0"), roachpb.Key("d"), false, nil)
	prereqs := getPrereqs(cq, roachpb.Key("a"), roachpb.Key("cc"), false)
	cq.remove(wk1)
	if testCmdDone(prereqs, 1*time.Millisecond) {
		t.Fatal("command should not finish with two commands outstanding")
	}
	cq.remove(wk2)
	if testCmdDone(prereqs, 1*time.Millisecond) {
		t.Fatal("command should not finish with one command outstanding")
	}
	cq.remove(wk3)
	if !testCmdDone(prereqs, 5*time.Millisecond) {
		t.Fatal("command should finish with no commands outstanding")
	}
}

func TestCommandQueueMultiplePendingCommands(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()

	// Add a command which will overlap all commands.
	wk0 := add(cq, roachpb.Key("a"), roachpb.Key("d"), false, nil)
	prereqs1, wk1 := getPrereqsAndAdd(cq, roachpb.Key("a"), roachpb.Key("b").Next(), false)
	prereqs2 := getPrereqs(cq, roachpb.Key("b"), nil, false)
	prereqs3 := getPrereqs(cq, roachpb.Key("c"), nil, false)

	for i, prereqs := range [][]*cmd{prereqs1, prereqs2, prereqs3} {
		if testCmdDone(prereqs, 1*time.Millisecond) {
			t.Fatalf("command %d should not finish with command 0 outstanding", i+1)
		}
	}

	cq.remove(wk0)
	if !testCmdDone(prereqs1, 5*time.Millisecond) {
		t.Fatal("command 1 should finish")
	}
	if !testCmdDone(prereqs3, 5*time.Millisecond) {
		t.Fatal("command 3 should finish")
	}
	if testCmdDone(prereqs2, 5*time.Millisecond) {
		t.Fatal("command 2 should remain outstanding")
	}
	cq.remove(wk1)
	if !testCmdDone(prereqs2, 5*time.Millisecond) {
		t.Fatal("command 2 should finish with no commands outstanding")
	}
}

func TestCommandQueueRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()

	// Add multiple commands and commands which access each.
	wk1 := add(cq, roachpb.Key("a"), nil, false, nil)
	wk2 := add(cq, roachpb.Key("b"), nil, false, nil)
	prereqs1 := getPrereqs(cq, roachpb.Key("a"), nil, false)
	prereqs2 := getPrereqs(cq, roachpb.Key("b"), nil, false)

	// Remove the commands from the queue and verify both commands are signaled.
	cq.remove(wk1)
	cq.remove(wk2)

	for i, prereqs := range [][]*cmd{prereqs1, prereqs2} {
		if !testCmdDone(prereqs, 5*time.Millisecond) {
			t.Fatalf("command %d should finish with clearing queue", i+1)
		}
	}
}

// TestCommandQueueExclusiveEnd verifies that an end key is treated as
// an exclusive end when GetPrereqs calculates overlapping commands. Test
// it by calling GetPrereqs with a command whose start key is equal to
// the end key of a previous command.
func TestCommandQueueExclusiveEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()
	add(cq, roachpb.Key("a"), roachpb.Key("b"), false, nil)

	// Verify no wait on the second writer command on "b" since
	// it does not overlap with the first command on ["a", "b").
	waitCmdDone(getPrereqs(cq, roachpb.Key("b"), nil, false))
}

// TestCommandQueueSelfOverlap makes sure that GetPrereqs adds all of the
// key ranges simultaneously. If that weren't the case, all but the first
// span would wind up waiting on overlapping previous spans, resulting
// in deadlock.
func TestCommandQueueSelfOverlap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()
	a := roachpb.Key("a")
	k := add(cq, a, roachpb.Key("b"), false, nil)
	prereqs := cq.getPrereqs(false, []roachpb.Span{{Key: a}, {Key: a}, {Key: a}}...)
	cq.remove(k)
	waitCmdDone(prereqs)
}

func TestCommandQueueCovering(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()

	a := roachpb.Span{Key: roachpb.Key("a")}
	b := roachpb.Span{Key: roachpb.Key("b")}
	c := roachpb.Span{Key: roachpb.Key("c")}

	{
		// Test adding a covering entry and then not expanding it.
		wk := cq.add(false, nil, a, b)
		waitCmdDone(cq.getPrereqs(false, c))
		cq.remove(wk)
	}

	{
		// Test adding a covering entry and expanding it.
		wk := cq.add(false, nil, a, b)
		prereqs := cq.getPrereqs(false, a)
		cq.remove(wk)
		waitCmdDone(prereqs)
	}
}

// Reconstruct a set of commands that tickled a bug in interval.Tree. See
// https://github.com/cockroachdb/cockroach/issues/6495 for details.
func TestCommandQueueIssue6495(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()
	cq.idAlloc = 1997

	mkSpan := func(start, end string) roachpb.Span {
		return roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)}
	}
	spans1998 := []roachpb.Span{
		mkSpan("\xbb\x89\x8b\x8a\x89", "\xbb\x89\x8b\x8a\x89\x00"),
	}
	spans1999 := []roachpb.Span{
		mkSpan("\xbb\x89\x88", "\xbb\x89\x89"),
		mkSpan("\xbb\x89\x8b", "\xbb\x89\x8c"),
	}
	spans2002 := []roachpb.Span{
		mkSpan("\xbb\x89\x8b", "\xbb\x89\x8c"),
	}
	spans2003 := []roachpb.Span{
		mkSpan("\xbb\x89\x8a\x8a\x89", "\xbb\x89\x8a\x8a\x89\x00"),
		mkSpan("\xbb\x89\x8b\x8a\x89", "\xbb\x89\x8b\x8a\x89\x00"),
		mkSpan("\xbb\x89\x8a\x8a\x89", "\xbb\x89\x8a\x8a\x89\x00"),
	}

	cq.getPrereqs(false, spans1998...)
	cmd1998 := cq.add(false, nil, spans1998...)

	cq.getPrereqs(true, spans1999...)
	cmd1999 := cq.add(true, nil, spans1999...)

	cq.getPrereqs(true, spans2002...)
	cq.add(true, nil, spans2002...)

	cq.getPrereqs(false, spans2003...)
	cq.add(false, nil, spans2003...)

	cq.remove(cmd1998)
	cq.remove(cmd1999)
}
