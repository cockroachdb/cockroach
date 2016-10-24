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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func getWait(cq *CommandQueue, from, to roachpb.Key, readOnly bool) []<-chan struct{} {
	return cq.getWait(readOnly, roachpb.Span{Key: from, EndKey: to})
}

func add(cq *CommandQueue, from, to roachpb.Key, readOnly bool) *cmd {
	return cq.add(readOnly, roachpb.Span{Key: from, EndKey: to})
}

func getWaitAndAdd(cq *CommandQueue, from, to roachpb.Key, readOnly bool) ([]<-chan struct{}, *cmd) {
	return getWait(cq, from, to, readOnly), add(cq, from, to, readOnly)
}

func waitCmdDone(chans []<-chan struct{}) {
	for _, ch := range chans {
		<-ch
	}
}

// testCmdDone waits for the cmdDone channel to be closed for at most
// the specified wait duration. Returns true if the command finished in
// the allotted time, false otherwise.
func testCmdDone(chans []<-chan struct{}, wait time.Duration) bool {
	t := time.After(wait)
	for _, ch := range chans {
		select {
		case <-t:
			return false
		case <-ch:
		}
	}
	return true
}

// checkCmdDoesNotFinish makes sure that the command waiting on the provided channels
// does not finish, returning false if this assertion fails.
func checkCmdDoesNotFinish(t *testing.T, chans []<-chan struct{}) bool {
	return !testCmdDone(chans, 3*time.Millisecond)
}

// checkCmdFinishes makes sure that the command waiting on the provided channels
// finishes, returning false if this assertion fails.
func checkCmdFinishes(t *testing.T, chans []<-chan struct{}) bool {
	return testCmdDone(chans, 15*time.Millisecond)
}

func TestCommandQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)

	// Try a command with no overlapping already-running commands.
	waitCmdDone(getWait(cq, roachpb.Key("a"), nil, false))
	waitCmdDone(getWait(cq, roachpb.Key("a"), roachpb.Key("b"), false))

	// Add a command and verify dependency on it.
	wk := add(cq, roachpb.Key("a"), nil, false)
	chans := getWait(cq, roachpb.Key("a"), nil, false)
	if !checkCmdDoesNotFinish(t, chans) {
		t.Fatal("command should not finish with command outstanding")
	}
	cq.remove(wk)
	if !checkCmdFinishes(t, chans) {
		t.Fatal("command should finish with no commands outstanding")
	}
}

// TestCommandQueueWriteWaitForNonAdjacentRead tests that the command queue
// lets a writer wait for a read which is separated from it through another
// read. Since reads don't wait for reads, there was a bug in which the writer
// would wind up waiting only for one of the two readers under it.
func TestCommandQueueWriteWaitForNonAdjacentRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)
	key := roachpb.Key("a")
	// Add a read-only command.
	wk1 := add(cq, key, nil, true)
	// Add another one on top.
	wk2 := add(cq, key, nil, true)

	// A write should have to wait for **both** reads, not only the second
	// one.
	chans := getWait(cq, key, nil, false /* !readOnly */)

	// Certainly blocks now.
	if !checkCmdDoesNotFinish(t, chans) {
		t.Fatal("command should not finish with command outstanding")
	}

	// The second read returns, but the first one remains.
	cq.remove(wk2)

	// Should still block. This being broken is why this test exists.
	if !checkCmdDoesNotFinish(t, chans) {
		t.Fatal("command should not finish with command outstanding")
	}

	// First read returns.
	cq.remove(wk1)

	// Now it goes through.
	if !checkCmdFinishes(t, chans) {
		t.Fatal("command should finish with no commands outstanding")
	}
}

func TestCommandQueueNoWaitOnReadOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)
	// Add a read-only command.
	chans1, wk := getWaitAndAdd(cq, roachpb.Key("a"), nil, true)
	// Verify no wait on another read-only command.
	waitCmdDone(chans1)
	// Verify wait with a read-write command.
	chans2 := getWait(cq, roachpb.Key("a"), nil, false)
	if !checkCmdDoesNotFinish(t, chans2) {
		t.Fatal("command should not finish with command outstanding")
	}
	cq.remove(wk)
	if !checkCmdFinishes(t, chans2) {
		t.Fatal("command should finish with no commands outstanding")
	}
}

func TestCommandQueueMultipleExecutingCommands(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)

	// Add multiple commands and add a command which overlaps them all.
	wk1 := add(cq, roachpb.Key("a"), nil, false)
	wk2 := add(cq, roachpb.Key("b"), roachpb.Key("c"), false)
	wk3 := add(cq, roachpb.Key("0"), roachpb.Key("d"), false)
	chans := getWait(cq, roachpb.Key("a"), roachpb.Key("cc"), false)
	cq.remove(wk1)
	if !checkCmdDoesNotFinish(t, chans) {
		t.Fatal("command should not finish with two commands outstanding")
	}
	cq.remove(wk2)
	if !checkCmdDoesNotFinish(t, chans) {
		t.Fatal("command should not finish with one command outstanding")
	}
	cq.remove(wk3)
	if !checkCmdFinishes(t, chans) {
		t.Fatal("command should finish with no commands outstanding")
	}
}

func TestCommandQueueMultiplePendingCommands(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)

	// Add a command which will overlap all commands.
	wk0 := add(cq, roachpb.Key("a"), roachpb.Key("d"), false)
	chans1, wk1 := getWaitAndAdd(cq, roachpb.Key("a"), roachpb.Key("b").Next(), false)
	chans2 := getWait(cq, roachpb.Key("b"), nil, false)
	chans3 := getWait(cq, roachpb.Key("c"), nil, false)

	for i, chans := range [][]<-chan struct{}{chans1, chans2, chans3} {
		if !checkCmdDoesNotFinish(t, chans) {
			t.Fatalf("command %d should not finish with command 0 outstanding", i+1)
		}
	}

	cq.remove(wk0)
	if !checkCmdFinishes(t, chans1) {
		t.Fatal("command 1 should finish")
	}
	if !checkCmdFinishes(t, chans3) {
		t.Fatal("command 3 should finish")
	}
	if !checkCmdDoesNotFinish(t, chans2) {
		t.Fatal("command 2 should remain outstanding")
	}
	cq.remove(wk1)
	if !checkCmdFinishes(t, chans2) {
		t.Fatal("command 2 should finish with no commands outstanding")
	}
}

func TestCommandQueueRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)

	// Add multiple commands and commands which access each.
	wk1 := add(cq, roachpb.Key("a"), nil, false)
	wk2 := add(cq, roachpb.Key("b"), nil, false)
	chans1 := getWait(cq, roachpb.Key("a"), nil, false)
	chans2 := getWait(cq, roachpb.Key("b"), nil, false)

	// Remove the commands from the queue and verify both commands are signaled.
	cq.remove(wk1)
	cq.remove(wk2)

	for i, chans := range [][]<-chan struct{}{chans1, chans2} {
		if !checkCmdFinishes(t, chans) {
			t.Fatalf("command %d should finish with clearing queue", i+1)
		}
	}
}

// TestCommandQueueExclusiveEnd verifies that an end key is treated as
// an exclusive end when GetWait calculates overlapping commands. Test
// it by calling GetWait with a command whose start key is equal to
// the end key of a previous command.
func TestCommandQueueExclusiveEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)
	add(cq, roachpb.Key("a"), roachpb.Key("b"), false)

	// Verify no wait on the second writer command on "b" since
	// it does not overlap with the first command on ["a", "b").
	waitCmdDone(getWait(cq, roachpb.Key("b"), nil, false))
}

// TestCommandQueueSelfOverlap makes sure that GetWait adds all of the
// key ranges simultaneously. If that weren't the case, all but the first
// span would wind up waiting on overlapping previous spans, resulting
// in deadlock.
func TestCommandQueueSelfOverlap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)
	a := roachpb.Key("a")
	k := add(cq, a, roachpb.Key("b"), false)
	chans := cq.getWait(false, []roachpb.Span{{Key: a}, {Key: a}, {Key: a}}...)
	cq.remove(k)
	waitCmdDone(chans)
}

func TestCommandQueueCoveringOptimization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)

	a := roachpb.Span{Key: roachpb.Key("a")}
	b := roachpb.Span{Key: roachpb.Key("b")}
	c := roachpb.Span{Key: roachpb.Key("c")}

	{
		// Test adding a covering entry and then not expanding it.
		wk := cq.add(false, a, b)
		if act := cq.tree.Len(); act != 1 {
			t.Fatal("expected one covering span, but got two spans")
		}
		waitCmdDone(cq.getWait(false, c))
		cq.remove(wk)
	}

	{
		// Test adding a covering entry and expanding it.
		wk := cq.add(false, a, b)
		chans := cq.getWait(false, a)
		cq.remove(wk)
		waitCmdDone(chans)
	}
}

func TestCommandQueueWithoutCoveringOptimization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(false /* !coveringOptimization */)

	a := roachpb.Span{Key: roachpb.Key("a")}
	b := roachpb.Span{Key: roachpb.Key("b")}
	c := roachpb.Span{Key: roachpb.Key("c")}

	{
		cmd := cq.add(false, a, b)
		if !cmd.expanded {
			t.Errorf("expected non-expanded command, not %+v", cmd)
		}
		if len(cmd.children) != 2 {
			t.Errorf("expected one child in command %+v", cmd)
		}
		if act, exp := cq.tree.Len(), 2; act != exp {
			t.Errorf("expected %d spans in tree, got %d", exp, act)
		}
		cq.remove(cmd)
	}

	{
		cmd := cq.add(false, c)
		if cmd.expanded {
			t.Errorf("expected unexpanded command, not %+v", cmd)
		}
		if len(cmd.children) != 0 {
			t.Errorf("expected no children in command %+v", cmd)
		}
		if act, exp := cq.tree.Len(), 1; act != exp {
			t.Errorf("expected %d spans in tree, got %d", exp, act)
		}
		cq.remove(cmd)
	}
}

// Reconstruct a set of commands that tickled a bug in interval.Tree. See
// https://github.com/cockroachdb/cockroach/issues/6495 for details.
func TestCommandQueueIssue6495(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)
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

	cq.getWait(false, spans1998...)
	cmd1998 := cq.add(false, spans1998...)

	cq.getWait(true, spans1999...)
	cmd1999 := cq.add(true, spans1999...)

	cq.getWait(true, spans2002...)
	cq.add(true, spans2002...)

	cq.getWait(false, spans2003...)
	cq.add(false, spans2003...)

	cq.remove(cmd1998)
	cq.remove(cmd1999)
}
