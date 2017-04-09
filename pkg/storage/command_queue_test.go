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
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func getWait(
	cq *CommandQueue, from, to roachpb.Key, readOnly bool, ts hlc.Timestamp,
) []<-chan struct{} {
	return cq.getWait(readOnly, ts, []roachpb.Span{{Key: from, EndKey: to}})
}

func add(cq *CommandQueue, from, to roachpb.Key, readOnly bool, ts hlc.Timestamp) *cmd {
	return cq.add(readOnly, ts, []roachpb.Span{{Key: from, EndKey: to}})
}

func getWaitAndAdd(
	cq *CommandQueue, from, to roachpb.Key, readOnly bool, ts hlc.Timestamp,
) ([]<-chan struct{}, *cmd) {
	return getWait(cq, from, to, readOnly, ts), add(cq, from, to, readOnly, ts)
}

func waitCmdDone(chans []<-chan struct{}) {
	for _, ch := range chans {
		<-ch
	}
}

var zeroTS = hlc.Timestamp{}

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
	waitCmdDone(getWait(cq, roachpb.Key("a"), nil, false, zeroTS))
	waitCmdDone(getWait(cq, roachpb.Key("a"), roachpb.Key("b"), false, zeroTS))

	// Add a command and verify dependency on it.
	wk := add(cq, roachpb.Key("a"), nil, false, zeroTS)
	chans := getWait(cq, roachpb.Key("a"), nil, false, zeroTS)
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
	wk1 := add(cq, key, nil, true, zeroTS)
	// Add another one on top.
	wk2 := add(cq, key, nil, true, zeroTS)

	// A write should have to wait for **both** reads, not only the second
	// one.
	chans := getWait(cq, key, nil, false /* !readOnly */, zeroTS)

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
	chans1, wk := getWaitAndAdd(cq, roachpb.Key("a"), nil, true, zeroTS)
	// Verify no wait on another read-only command.
	waitCmdDone(chans1)
	// Verify wait with a read-write command.
	chans2 := getWait(cq, roachpb.Key("a"), nil, false, zeroTS)
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
	wk1 := add(cq, roachpb.Key("a"), nil, false, zeroTS)
	wk2 := add(cq, roachpb.Key("b"), roachpb.Key("c"), false, zeroTS)
	wk3 := add(cq, roachpb.Key("0"), roachpb.Key("d"), false, zeroTS)
	chans := getWait(cq, roachpb.Key("a"), roachpb.Key("cc"), false, zeroTS)
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
	wk0 := add(cq, roachpb.Key("a"), roachpb.Key("d"), false, zeroTS)
	chans1, wk1 := getWaitAndAdd(cq, roachpb.Key("a"), roachpb.Key("b").Next(), false, zeroTS)
	chans2 := getWait(cq, roachpb.Key("b"), nil, false, zeroTS)
	chans3 := getWait(cq, roachpb.Key("c"), nil, false, zeroTS)

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
	wk1 := add(cq, roachpb.Key("a"), nil, false, zeroTS)
	wk2 := add(cq, roachpb.Key("b"), nil, false, zeroTS)
	chans1 := getWait(cq, roachpb.Key("a"), nil, false, zeroTS)
	chans2 := getWait(cq, roachpb.Key("b"), nil, false, zeroTS)

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
	add(cq, roachpb.Key("a"), roachpb.Key("b"), false, zeroTS)

	// Verify no wait on the second writer command on "b" since
	// it does not overlap with the first command on ["a", "b").
	waitCmdDone(getWait(cq, roachpb.Key("b"), nil, false, zeroTS))
}

// TestCommandQueueSelfOverlap makes sure that GetWait adds all of the
// key ranges simultaneously. If that weren't the case, all but the first
// span would wind up waiting on overlapping previous spans, resulting
// in deadlock.
func TestCommandQueueSelfOverlap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)
	a := roachpb.Key("a")
	k := add(cq, a, roachpb.Key("b"), false, zeroTS)
	chans := cq.getWait(false, zeroTS, []roachpb.Span{{Key: a}, {Key: a}, {Key: a}})
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
		wk := cq.add(false, zeroTS, []roachpb.Span{a, b})
		if n := cq.treeSize(); n != 1 {
			t.Fatalf("expected a single covering span, but got %d", n)
		}
		waitCmdDone(cq.getWait(false, zeroTS, []roachpb.Span{c}))
		cq.remove(wk)
	}

	{
		// Test adding a covering entry and expanding it.
		wk := cq.add(false, zeroTS, []roachpb.Span{a, b})
		chans := cq.getWait(false, zeroTS, []roachpb.Span{a})
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
		cmd := cq.add(false, zeroTS, []roachpb.Span{a, b})
		if !cmd.expanded {
			t.Errorf("expected non-expanded command, not %+v", cmd)
		}
		if exp, act := 2, len(cmd.children); exp != act {
			t.Errorf("expected %d children in command, got %d: %+v", exp, act, cmd)
		}
		if exp, act := 2, cq.treeSize(); act != exp {
			t.Errorf("expected %d spans in tree, got %d", exp, act)
		}
		cq.remove(cmd)
	}

	{
		cmd := cq.add(false, zeroTS, []roachpb.Span{c})
		if cmd.expanded {
			t.Errorf("expected unexpanded command, not %+v", cmd)
		}
		if len(cmd.children) != 0 {
			t.Errorf("expected no children in command %+v", cmd)
		}
		if act, exp := cq.treeSize(), 1; act != exp {
			t.Errorf("expected %d spans in tree, got %d", exp, act)
		}
		cq.remove(cmd)
	}
}

func mkSpan(start, end string) roachpb.Span {
	if len(end) == 0 {
		return roachpb.Span{Key: roachpb.Key(start)}
	}
	return roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)}
}

// Reconstruct a set of commands that tickled a bug in interval.Tree. See
// https://github.com/cockroachdb/cockroach/issues/6495 for details.
func TestCommandQueueIssue6495(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)
	cq.idAlloc = 1997

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

	cq.getWait(false, zeroTS, spans1998)
	cmd1998 := cq.add(false, zeroTS, spans1998)

	cq.getWait(true, zeroTS, spans1999)
	cmd1999 := cq.add(true, zeroTS, spans1999)

	cq.getWait(true, zeroTS, spans2002)
	cq.add(true, zeroTS, spans2002)

	cq.getWait(false, zeroTS, spans2003)
	cq.add(false, zeroTS, spans2003)

	cq.remove(cmd1998)
	cq.remove(cmd1999)
}

// TestCommandQueueTimestamps creates a command queue with a mix of
// read and write spans and verifies that writes don't wait on
// earlier reads and reads don't wait on later writes. The spans
// are layered as follows (earlier spans have earlier timestamps):
//
// Span  TS  RO  a  b  c  d  e  f  g
//    1   1   T  ------   -
//    2   2   F     -
//    3   3   F        -  ------
//    4   4   T              ------
func TestCommandQueueTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)

	spans1 := []roachpb.Span{
		mkSpan("a", "c"),
		mkSpan("d", ""),
	}
	spans2 := []roachpb.Span{
		mkSpan("b", ""),
	}
	spans3 := []roachpb.Span{
		mkSpan("c", ""),
		mkSpan("d", "f"),
	}
	spans4 := []roachpb.Span{
		mkSpan("e", "g"),
	}

	cmd1 := cq.add(true, makeTS(1, 0), spans1)

	if w := cq.getWait(true, makeTS(2, 0), spans2); w != nil {
		t.Errorf("expected nil wait slice; got %+v", w)
	}
	cmd2 := cq.add(false, makeTS(2, 0), spans2)

	if w := cq.getWait(true, makeTS(3, 0), spans3); w != nil {
		t.Errorf("expected nil wait slice; got %+v", w)
	}
	cmd3 := cq.add(false, makeTS(3, 0), spans3)

	// spans4 should wait on spans3.children[1].pending.
	w := cq.getWait(true, makeTS(4, 0), spans4)
	expW := []<-chan struct{}{cmd3.children[1].pending}
	if !reflect.DeepEqual(w, expW) {
		t.Errorf("expected wait channels %+v; got %+v", expW, w)
	}
	cmd4 := cq.add(true, makeTS(4, 0), spans4)

	// Verify that an earlier writer for whole span waits on all commands.
	w = cq.getWait(false, makeTS(0, 1), []roachpb.Span{mkSpan("a", "g")})
	allW := []<-chan struct{}{
		cmd4.pending,
		// Skip cmd3.children[1].pending here because it's a dependency.
		cmd3.children[0].pending,
		cmd2.pending,
		cmd1.children[1].pending,
		cmd1.children[0].pending,
	}
	expW = allW
	if !reflect.DeepEqual(expW, w) {
		t.Errorf("expected wait channels %+v; got %+v", expW, w)
	}

	// Verify that a later writer for whole span. At the same
	// timestamp, we wait on the latest read.
	expW = []<-chan struct{}{cmd4.pending, cmd3.children[0].pending, cmd2.pending}
	if w := cq.getWait(false, makeTS(4, 0), []roachpb.Span{mkSpan("a", "g")}); !reflect.DeepEqual(expW, w) {
		t.Errorf("expected wait channels %+v; got %+v", expW, w)
	}
	// At +1 logical tick, we skip the latest read and instead
	// read the overlapped write just beneath the latest read.
	expW = []<-chan struct{}{cmd3.children[1].pending, cmd3.children[0].pending, cmd2.pending}
	if w := cq.getWait(false, makeTS(4, 1), []roachpb.Span{mkSpan("a", "g")}); !reflect.DeepEqual(expW, w) {
		t.Errorf("expected wait channels %+v; got %+v", expW, w)
	}

	// Verify an earlier reader for whole span doesn't wait.
	if w := cq.getWait(true, makeTS(0, 1), []roachpb.Span{mkSpan("a", "g")}); w != nil {
		t.Errorf("expected nil wait slice; got %+v", w)
	}

	// Verify a later reader for whole span waits on both writers.
	expW = []<-chan struct{}{cmd3.children[1].pending, cmd3.children[0].pending, cmd2.pending}
	if w := cq.getWait(true, makeTS(4, 0), []roachpb.Span{mkSpan("a", "g")}); !reflect.DeepEqual(expW, w) {
		t.Errorf("expected wait channels %+v; got %+v", expW, w)
	}

	// Verify that if no timestamp is specified, we always wait (on writers and readers!).
	expW = allW
	if w := cq.getWait(false, hlc.Timestamp{}, []roachpb.Span{mkSpan("a", "g")}); !reflect.DeepEqual(expW, w) {
		t.Errorf("expected wait channels %+v; got %+v", expW, w)
	}
	expW = []<-chan struct{}{cmd3.children[1].pending, cmd3.children[0].pending, cmd2.pending}
	if w := cq.getWait(true, hlc.Timestamp{}, []roachpb.Span{mkSpan("a", "g")}); !reflect.DeepEqual(expW, w) {
		t.Errorf("expected wait channels %+v; got %+v", expW, w)
	}
}

// TestCommandQueueEnclosed verifies that the command queue doesn't
// fail to return read-only dependencies that a candidate read/write
// span depends on, despite there being an overlapping span which
// completely encloses the candidate. See #14434.
//
//      Span  TS  RO  a  b  depends
//         1   2   T  -     n/a
//         2   3   F  ---   n/a
// Candidate   1   F  -     spans 1, 2
func TestCommandQueueEnclosed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)

	spans1 := []roachpb.Span{
		mkSpan("a", ""),
	}
	spans2 := []roachpb.Span{
		mkSpan("a", "b"),
	}
	spansCandidate := []roachpb.Span{
		mkSpan("a", ""),
	}

	cmd1 := cq.add(true, makeTS(2, 0), spans1)
	cmd2 := cq.add(false, makeTS(3, 0), spans2)

	w := cq.getWait(false, makeTS(1, 0), spansCandidate)
	expW := []<-chan struct{}{cmd2.pending, cmd1.pending}
	if !reflect.DeepEqual(w, expW) {
		t.Errorf("expected wait channels %+v; got %+v", expW, w)
	}
}

// TestCommandQueueTimestampsEmpty verifies command queue wait
// behavior when added commands have zero timestamps and when
// the waiter has a zero timestamp.
func TestCommandQueueTimestampsEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)

	spansR := []roachpb.Span{
		mkSpan("a", "c"),
	}
	spansW := []roachpb.Span{
		mkSpan("d", "f"),
	}
	spansRTS := []roachpb.Span{
		mkSpan("g", ""),
	}
	spansWTS := []roachpb.Span{
		mkSpan("h", ""),
	}

	cmd1 := cq.add(true, hlc.Timestamp{}, spansR)
	cmd2 := cq.add(false, hlc.Timestamp{}, spansW)
	cmd3 := cq.add(true, makeTS(1, 0), spansRTS)
	cmd4 := cq.add(false, makeTS(1, 0), spansWTS)

	// A writer will depend on both zero-timestamp spans.
	w := cq.getWait(false, makeTS(1, 0), []roachpb.Span{mkSpan("a", "f")})
	expW := []<-chan struct{}{cmd2.pending, cmd1.pending}
	if !reflect.DeepEqual(w, expW) {
		t.Errorf("expected wait channels %+v; got %+v", expW, w)
	}

	// A reader will depend on the write span.
	w = cq.getWait(true, makeTS(1, 0), []roachpb.Span{mkSpan("a", "f")})
	expW = []<-chan struct{}{cmd2.pending}
	if !reflect.DeepEqual(w, expW) {
		t.Errorf("expected wait channels %+v; got %+v", expW, w)
	}

	// A zero-timestamp writer will depend on both ts=1 spans.
	w = cq.getWait(false, hlc.Timestamp{}, []roachpb.Span{mkSpan("g", "i")})
	expW = []<-chan struct{}{cmd4.pending, cmd3.pending}
	if !reflect.DeepEqual(w, expW) {
		t.Errorf("expected wait channels %+v; got %+v", expW, w)
	}

	// A zero-timestamp reader will depend on the write span.
	w = cq.getWait(true, hlc.Timestamp{}, []roachpb.Span{mkSpan("g", "i")})
	expW = []<-chan struct{}{cmd4.pending}
	if !reflect.DeepEqual(w, expW) {
		t.Errorf("expected wait channels %+v; got %+v", expW, w)
	}
}

func BenchmarkCommandQueueGetWait(b *testing.B) {
	// Test read-only getWait performance for various number of command queue
	// entries. See #13627 where a previous implementation of
	// CommandQueue.getOverlaps had O(n) performance in this setup. Since reads
	// do not wait on other reads, expected performance is O(1).
	for _, size := range []int{1, 4, 16, 64, 128, 256} {
		b.Run(fmt.Sprintf("%d", size), func(b *testing.B) {
			cq := NewCommandQueue(true)
			spans := []roachpb.Span{{
				Key:    roachpb.Key("aaaaaaaaaa"),
				EndKey: roachpb.Key("aaaaaaaaab"),
			}}
			for i := 0; i < size; i++ {
				cq.add(true, zeroTS, spans)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = cq.getWait(true, zeroTS, spans)
			}
		})
	}
}
