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

package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

var zeroTS = hlc.Timestamp{}

func getPrereqs(cq *CommandQueue, from, to roachpb.Key, readOnly bool) []*cmd {
	return cq.getPrereqs(readOnly, zeroTS, []roachpb.Span{{Key: from, EndKey: to}})
}

func add(cq *CommandQueue, from, to roachpb.Key, readOnly bool, prereqs []*cmd) *cmd {
	return cq.add(readOnly, zeroTS, prereqs, []roachpb.Span{{Key: from, EndKey: to}})
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

// checkCmdDoesNotFinish makes sure that the command waiting on the provided channels
// does not finish, returning false if this assertion fails.
func checkCmdDoesNotFinish(prereqs []*cmd) bool {
	return !testCmdDone(prereqs, 3*time.Millisecond)
}

// checkCmdFinishes makes sure that the command waiting on the provided channels
// finishes, returning false if this assertion fails.
func checkCmdFinishes(prereqs []*cmd) bool {
	return testCmdDone(prereqs, 15*time.Millisecond)
}

func TestCommandQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)

	// Try a command with no overlapping already-running commands.
	waitCmdDone(getPrereqs(cq, roachpb.Key("a"), nil, false))
	waitCmdDone(getPrereqs(cq, roachpb.Key("a"), roachpb.Key("b"), false))

	// Add a command and verify dependency on it.
	cmd1 := add(cq, roachpb.Key("a"), nil, false, nil)
	prereqs := getPrereqs(cq, roachpb.Key("a"), nil, false)
	if !checkCmdDoesNotFinish(prereqs) {
		t.Fatal("command should not finish with command outstanding")
	}
	cq.remove(cmd1)
	if !checkCmdFinishes(prereqs) {
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
	cmd1 := add(cq, key, nil, true, nil)
	// Add another one on top.
	cmd2 := add(cq, key, nil, true, nil)

	// A write should have to wait for **both** reads, not only the second
	// one.
	prereqs := getPrereqs(cq, key, nil, false /* !readOnly */)

	// Certainly blocks now.
	if !checkCmdDoesNotFinish(prereqs) {
		t.Fatal("command should not finish with command outstanding")
	}

	// The second read returns, but the first one remains.
	cq.remove(cmd2)

	// Should still block. This being broken is why this test exists.
	if !checkCmdDoesNotFinish(prereqs) {
		t.Fatal("command should not finish with command outstanding")
	}

	// First read returns.
	cq.remove(cmd1)

	// Now it goes through.
	if !checkCmdFinishes(prereqs) {
		t.Fatal("command should finish with no commands outstanding")
	}
}

func TestCommandQueueNoWaitOnReadOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)
	// Add a read-only command.
	prereqs1, cmd1 := getPrereqsAndAdd(cq, roachpb.Key("a"), nil, true)
	// Verify no wait on another read-only command.
	waitCmdDone(prereqs1)
	// Verify wait with a read-write command.
	prereqs2 := getPrereqs(cq, roachpb.Key("a"), nil, false)
	if !checkCmdDoesNotFinish(prereqs2) {
		t.Fatal("command should not finish with command outstanding")
	}
	cq.remove(cmd1)
	if !checkCmdFinishes(prereqs2) {
		t.Fatal("command should finish with no commands outstanding")
	}
}

func TestCommandQueueMultipleExecutingCommands(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)

	// Add multiple commands and add a command which overlaps them all.
	cmd1 := add(cq, roachpb.Key("a"), nil, false, nil)
	cmd2 := add(cq, roachpb.Key("b"), roachpb.Key("c"), false, nil)
	cmd3 := add(cq, roachpb.Key("0"), roachpb.Key("d"), false, nil)
	prereqs := getPrereqs(cq, roachpb.Key("a"), roachpb.Key("cc"), false)
	cq.remove(cmd1)
	if !checkCmdDoesNotFinish(prereqs) {
		t.Fatal("command should not finish with two commands outstanding")
	}
	cq.remove(cmd2)
	if !checkCmdDoesNotFinish(prereqs) {
		t.Fatal("command should not finish with one command outstanding")
	}
	cq.remove(cmd3)
	if !checkCmdFinishes(prereqs) {
		t.Fatal("command should finish with no commands outstanding")
	}
}

func TestCommandQueueMultiplePendingCommands(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)

	// Add a command which will overlap all commands.
	wk0 := add(cq, roachpb.Key("a"), roachpb.Key("d"), false, nil)
	prereqs1, cmd1 := getPrereqsAndAdd(cq, roachpb.Key("a"), roachpb.Key("b").Next(), false)
	prereqs2 := getPrereqs(cq, roachpb.Key("b"), nil, false)
	prereqs3 := getPrereqs(cq, roachpb.Key("c"), nil, false)

	for i, prereqs := range [][]*cmd{prereqs1, prereqs2, prereqs3} {
		if !checkCmdDoesNotFinish(prereqs) {
			t.Fatalf("command %d should not finish with command 0 outstanding", i+1)
		}
	}

	cq.remove(wk0)
	if !checkCmdFinishes(prereqs1) {
		t.Fatal("command 1 should finish")
	}
	if !checkCmdFinishes(prereqs3) {
		t.Fatal("command 3 should finish")
	}
	if !checkCmdDoesNotFinish(prereqs2) {
		t.Fatal("command 2 should remain outstanding")
	}
	cq.remove(cmd1)
	if !checkCmdFinishes(prereqs2) {
		t.Fatal("command 2 should finish with no commands outstanding")
	}
}

func TestCommandQueueRemove(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)

	// Add multiple commands and commands which access each.
	cmd1 := add(cq, roachpb.Key("a"), nil, false, nil)
	cmd2 := add(cq, roachpb.Key("b"), nil, false, nil)
	prereqs1 := getPrereqs(cq, roachpb.Key("a"), nil, false)
	prereqs2 := getPrereqs(cq, roachpb.Key("b"), nil, false)

	// Remove the commands from the queue and verify both commands are signaled.
	cq.remove(cmd1)
	cq.remove(cmd2)

	for i, prereqs := range [][]*cmd{prereqs1, prereqs2} {
		if !checkCmdFinishes(prereqs) {
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
	cq := NewCommandQueue(true)
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
	cq := NewCommandQueue(true)
	a := roachpb.Key("a")
	cmd := add(cq, a, roachpb.Key("b"), false, nil)
	prereqs := cq.getPrereqs(false, zeroTS, []roachpb.Span{{Key: a}, {Key: a}, {Key: a}})
	cq.remove(cmd)
	waitCmdDone(prereqs)
}

func TestCommandQueueCoveringOptimization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(true)

	a := roachpb.Key("a")
	b := roachpb.Key("b")
	c := roachpb.Key("c")

	{
		// Test adding a covering entry and then not expanding it.
		cmd := add(cq, a, b, false, nil)
		if n := cq.treeSize(); n != 1 {
			t.Fatalf("expected a single covering span, but got %d", n)
		}
		waitCmdDone(getPrereqs(cq, c, nil, false))
		cq.remove(cmd)
	}

	{
		// Test adding a covering entry and expanding it.
		cmd := add(cq, a, b, false, nil)
		prereqs := getPrereqs(cq, a, nil, false)
		cq.remove(cmd)
		waitCmdDone(prereqs)
	}
}

func TestCommandQueueWithoutCoveringOptimization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue(false /* !coveringOptimization */)

	a := roachpb.Span{Key: roachpb.Key("a")}
	b := roachpb.Span{Key: roachpb.Key("b")}
	c := roachpb.Span{Key: roachpb.Key("c")}

	{
		cmd := cq.add(false, zeroTS, nil, []roachpb.Span{a, b})
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
		cmd := cq.add(false, zeroTS, nil, []roachpb.Span{c})
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

func randBytes(n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return b
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

	cq.getPrereqs(false, zeroTS, spans1998)
	cmd1998 := cq.add(false, zeroTS, nil, spans1998)

	cq.getPrereqs(true, zeroTS, spans1999)
	cmd1999 := cq.add(true, zeroTS, nil, spans1999)

	cq.getPrereqs(true, zeroTS, spans2002)
	cq.add(true, zeroTS, nil, spans2002)

	cq.getPrereqs(false, zeroTS, spans2003)
	cq.add(false, zeroTS, nil, spans2003)

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

	cmd1 := cq.add(true, makeTS(1, 0), nil, spans1)

	pre2 := cq.getPrereqs(true, makeTS(2, 0), spans2)
	if pre2 != nil {
		t.Errorf("expected nil prereq slice; got %+v", pre2)
	}
	cmd2 := cq.add(false, makeTS(2, 0), pre2, spans2)

	pre3 := cq.getPrereqs(true, makeTS(3, 0), spans3)
	if pre3 != nil {
		t.Errorf("expected nil prereq slice; got %+v", pre3)
	}
	cmd3 := cq.add(false, makeTS(3, 0), pre3, spans3)

	// spans4 should wait on spans3.children[1].
	pre4 := cq.getPrereqs(true, makeTS(4, 0), spans4)
	expPre := []*cmd{&cmd3.children[1]}
	if !reflect.DeepEqual(expPre, pre4) {
		t.Errorf("expected prereq commands %+v; got %+v", expPre, pre4)
	}
	cmd4 := cq.add(true, makeTS(4, 0), pre4, spans4)

	// Verify that an earlier writer for whole span waits on all commands.
	pre5 := cq.getPrereqs(false, makeTS(0, 1), []roachpb.Span{mkSpan("a", "g")})
	allCmds := []*cmd{
		cmd4,
		// Skip cmd3.children[1] here because it's a dependency.
		&cmd3.children[0],
		cmd2,
		&cmd1.children[1],
		&cmd1.children[0],
	}
	expPre = allCmds
	if !reflect.DeepEqual(expPre, pre5) {
		t.Errorf("expected prereq commands %+v; got %+v", expPre, pre5)
	}

	// Verify that a later writer for whole span. At the same
	// timestamp, we wait on the latest read.
	expPre = []*cmd{cmd4, &cmd3.children[0], cmd2}
	if pre := cq.getPrereqs(false, makeTS(4, 0), []roachpb.Span{mkSpan("a", "g")}); !reflect.DeepEqual(expPre, pre) {
		t.Errorf("expected prereq commands %+v; got %+v", expPre, pre)
	}
	// At +1 logical tick, we skip the latest read and instead
	// read the overlapped write just beneath the latest read.
	expPre = []*cmd{&cmd3.children[1], &cmd3.children[0], cmd2}
	if pre := cq.getPrereqs(false, makeTS(4, 1), []roachpb.Span{mkSpan("a", "g")}); !reflect.DeepEqual(expPre, pre) {
		t.Errorf("expected prereq commands %+v; got %+v", expPre, pre)
	}

	// Verify an earlier reader for whole span doesn't wait.
	if pre := cq.getPrereqs(true, makeTS(0, 1), []roachpb.Span{mkSpan("a", "g")}); pre != nil {
		t.Errorf("expected nil prereq command; got %+v", pre)
	}

	// Verify a later reader for whole span waits on both writers.
	expPre = []*cmd{&cmd3.children[1], &cmd3.children[0], cmd2}
	if pre := cq.getPrereqs(true, makeTS(4, 0), []roachpb.Span{mkSpan("a", "g")}); !reflect.DeepEqual(expPre, pre) {
		t.Errorf("expected prereq commands %+v; got %+v", expPre, pre)
	}

	// Verify that if no timestamp is specified, we always wait (on writers and readers!).
	expPre = allCmds
	if pre := cq.getPrereqs(false, hlc.Timestamp{}, []roachpb.Span{mkSpan("a", "g")}); !reflect.DeepEqual(expPre, pre) {
		t.Errorf("expected prereq commands %+v; got %+v", expPre, pre)
	}
	expPre = []*cmd{&cmd3.children[1], &cmd3.children[0], cmd2}
	if pre := cq.getPrereqs(true, hlc.Timestamp{}, []roachpb.Span{mkSpan("a", "g")}); !reflect.DeepEqual(expPre, pre) {
		t.Errorf("expected prereq commands %+v; got %+v", expPre, pre)
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

	cmd1 := cq.add(true, makeTS(2, 0), nil, spans1)
	cmd2 := cq.add(false, makeTS(3, 0), nil, spans2)

	pre := cq.getPrereqs(false, makeTS(1, 0), spansCandidate)
	expPre := []*cmd{cmd2, cmd1}
	if !reflect.DeepEqual(expPre, pre) {
		t.Errorf("expected prereq commands %+v; got %+v", expPre, pre)
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

	cmd1 := cq.add(true, zeroTS, nil, spansR)
	cmd2 := cq.add(false, zeroTS, nil, spansW)
	cmd3 := cq.add(true, makeTS(1, 0), nil, spansRTS)
	cmd4 := cq.add(false, makeTS(1, 0), nil, spansWTS)

	// A writer will depend on both zero-timestamp spans.
	pre := cq.getPrereqs(false, makeTS(1, 0), []roachpb.Span{mkSpan("a", "f")})
	expPre := []*cmd{cmd2, cmd1}
	if !reflect.DeepEqual(expPre, pre) {
		t.Errorf("expected prereq commands %+v; got %+v", expPre, pre)
	}

	// A reader will depend on the write span.
	pre = cq.getPrereqs(true, makeTS(1, 0), []roachpb.Span{mkSpan("a", "f")})
	expPre = []*cmd{cmd2}
	if !reflect.DeepEqual(expPre, pre) {
		t.Errorf("expected prereq commands %+v; got %+v", expPre, pre)
	}

	// A zero-timestamp writer will depend on both ts=1 spans.
	pre = cq.getPrereqs(false, hlc.Timestamp{}, []roachpb.Span{mkSpan("g", "i")})
	expPre = []*cmd{cmd4, cmd3}
	if !reflect.DeepEqual(expPre, pre) {
		t.Errorf("expected prereq commands %+v; got %+v", expPre, pre)
	}

	// A zero-timestamp reader will depend on the write span.
	pre = cq.getPrereqs(true, hlc.Timestamp{}, []roachpb.Span{mkSpan("g", "i")})
	expPre = []*cmd{cmd4}
	if !reflect.DeepEqual(expPre, pre) {
		t.Errorf("expected prereq commands %+v; got %+v", expPre, pre)
	}
}

func BenchmarkCommandQueueGetPrereqsAllReadOnly(b *testing.B) {
	// Test read-only getPrereqs performance for various number of command queue
	// entries. See #13627 where a previous implementation of
	// CommandQueue.getOverlaps had O(n) performance in this setup. Since reads
	// do not wait on other reads, expected performance is O(1).
	for _, size := range []int{1, 4, 16, 64, 128, 256} {
		b.Run(fmt.Sprintf("size=%d", size), func(b *testing.B) {
			cq := NewCommandQueue(true)
			spans := []roachpb.Span{{
				Key:    roachpb.Key("aaaaaaaaaa"),
				EndKey: roachpb.Key("aaaaaaaaab"),
			}}
			for i := 0; i < size; i++ {
				cq.add(true, zeroTS, nil, spans)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = cq.getPrereqs(true, zeroTS, spans)
			}
		})
	}
}

func BenchmarkCommandQueueReadWriteMix(b *testing.B) {
	// Test performance with a mixture of reads and writes with a high number
	// of reads per write.
	// See #15544.
	for _, readsPerWrite := range []int{1, 4, 16, 64, 128, 256} {
		b.Run(fmt.Sprintf("readsPerWrite=%d", readsPerWrite), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				totalCmds := 1 << 10
				liveCmdQueue := make(chan *cmd, 16)
				cq := NewCommandQueue(true /* coveringOptimization */)
				for j := 0; j < totalCmds; j++ {
					a, b := randBytes(100), randBytes(100)
					// Overwrite first byte so that we do not mix local and global ranges
					a[0], b[0] = 'a', 'a'
					if bytes.Compare(a, b) > 0 {
						a, b = b, a
					}
					spans := []roachpb.Span{{
						Key:    roachpb.Key(a),
						EndKey: roachpb.Key(b),
					}}
					var cmd *cmd
					readOnly := j%(readsPerWrite+1) != 0
					prereqs := cq.getPrereqs(readOnly, zeroTS, spans)
					cmd = cq.add(readOnly, zeroTS, prereqs, spans)
					if len(liveCmdQueue) == cap(liveCmdQueue) {
						cq.remove(<-liveCmdQueue)
					}
					liveCmdQueue <- cmd
				}
			}
		})
	}
}
