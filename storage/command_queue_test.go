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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/caller"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func getWait(cq *CommandQueue, from, to roachpb.Key, readOnly bool, wg *sync.WaitGroup) {
	cq.GetWait(readOnly, wg, roachpb.Span{Key: from, EndKey: to})
}

func add(cq *CommandQueue, from, to roachpb.Key, readOnly bool) interface{} {
	return cq.Add(readOnly, roachpb.Span{Key: from, EndKey: to})[0]
}

func getWaitAndAdd(cq *CommandQueue, from, to roachpb.Key, readOnly bool, wg *sync.WaitGroup) interface{} {
	getWait(cq, from, to, readOnly, wg)
	return add(cq, from, to, readOnly)
}

// waitForCmd launches a goroutine to wait on the supplied
// WaitGroup. A channel is returned which signals the completion of
// the wait.
func waitForCmd(wg *sync.WaitGroup) <-chan struct{} {
	cmdDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(cmdDone)
	}()
	return cmdDone
}

// testCmdDone waits for the cmdDone channel to be closed for at most
// the specified wait duration. Returns true if the command finished in
// the allotted time, false otherwise.
func testCmdDone(cmdDone <-chan struct{}, wait time.Duration) bool {
	select {
	case <-cmdDone:
		return true
	case <-time.After(wait):
		return false
	}
}

func TestCommandQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()
	wg := sync.WaitGroup{}

	// Try a command with no overlapping already-running commands.
	getWait(cq, roachpb.Key("a"), nil, false, &wg)
	wg.Wait()
	getWait(cq, roachpb.Key("a"), roachpb.Key("b"), false, &wg)
	wg.Wait()

	// Add a command and verify wait group is returned.
	wk := add(cq, roachpb.Key("a"), nil, false)
	getWait(cq, roachpb.Key("a"), nil, false, &wg)
	cmdDone := waitForCmd(&wg)
	if testCmdDone(cmdDone, 1*time.Millisecond) {
		t.Fatal("command should not finish with command outstanding")
	}
	cq.Remove([]interface{}{wk})
	if !testCmdDone(cmdDone, 5*time.Millisecond) {
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
	wk1 := add(cq, key, nil, true)
	// Add another one on top.
	wk2 := add(cq, key, nil, true)

	// A write should have to wait for **both** reads, not only the second
	// one.
	wg := sync.WaitGroup{}
	getWait(cq, key, nil, false /* !readOnly */, &wg)

	assert := func(blocked bool) {
		cmdDone := waitForCmd(&wg)
		d := time.Millisecond
		if !blocked {
			d *= 5
		}
		f, l, _ := caller.Lookup(1)
		if testCmdDone(cmdDone, d) {
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
	cq.Remove([]interface{}{wk2})

	// Should still block. This being broken is why this test exists.
	assert(true)

	// First read returns.
	cq.Remove([]interface{}{wk1})

	// Now it goes through.
	assert(false)
}

func TestCommandQueueNoWaitOnReadOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()
	wg := sync.WaitGroup{}
	// Add a read-only command.
	wk := add(cq, roachpb.Key("a"), nil, true)
	// Verify no wait on another read-only command.
	getWait(cq, roachpb.Key("a"), nil, true, &wg)
	wg.Wait()
	// Verify wait with a read-write command.
	getWait(cq, roachpb.Key("a"), nil, false, &wg)
	cmdDone := waitForCmd(&wg)
	if testCmdDone(cmdDone, 1*time.Millisecond) {
		t.Fatal("command should not finish with command outstanding")
	}
	cq.Remove([]interface{}{wk})
	if !testCmdDone(cmdDone, 5*time.Millisecond) {
		t.Fatal("command should finish with no commands outstanding")
	}
}

func TestCommandQueueMultipleExecutingCommands(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()
	wg := sync.WaitGroup{}

	// Add multiple commands and add a command which overlaps them all.
	wk1 := add(cq, roachpb.Key("a"), nil, false)
	wk2 := add(cq, roachpb.Key("b"), roachpb.Key("c"), false)
	wk3 := add(cq, roachpb.Key("0"), roachpb.Key("d"), false)
	getWait(cq, roachpb.Key("a"), roachpb.Key("cc"), false, &wg)
	cmdDone := waitForCmd(&wg)
	cq.Remove([]interface{}{wk1})
	if testCmdDone(cmdDone, 1*time.Millisecond) {
		t.Fatal("command should not finish with two commands outstanding")
	}
	cq.Remove([]interface{}{wk2})
	if testCmdDone(cmdDone, 1*time.Millisecond) {
		t.Fatal("command should not finish with one command outstanding")
	}
	cq.Remove([]interface{}{wk3})
	if !testCmdDone(cmdDone, 5*time.Millisecond) {
		t.Fatal("command should finish with no commands outstanding")
	}
}

func TestCommandQueueMultiplePendingCommands(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()
	wg1 := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}
	wg3 := sync.WaitGroup{}

	// Add a command which will overlap all commands.
	wk0 := add(cq, roachpb.Key("a"), roachpb.Key("d"), false)
	wk1 := getWaitAndAdd(cq, roachpb.Key("a"), roachpb.Key("b").Next(), false, &wg1)
	getWait(cq, roachpb.Key("b"), nil, false, &wg2)
	getWait(cq, roachpb.Key("c"), nil, false, &wg3)
	cmdDone1 := waitForCmd(&wg1)
	cmdDone2 := waitForCmd(&wg2)
	cmdDone3 := waitForCmd(&wg3)

	if testCmdDone(cmdDone1, 1*time.Millisecond) ||
		testCmdDone(cmdDone2, 1*time.Millisecond) ||
		testCmdDone(cmdDone3, 1*time.Millisecond) {
		t.Fatal("no commands should finish with command outstanding")
	}
	cq.Remove([]interface{}{wk0})
	if !testCmdDone(cmdDone1, 5*time.Millisecond) ||
		!testCmdDone(cmdDone3, 5*time.Millisecond) {
		t.Fatal("command 1 and 3 should finish")
	}
	if testCmdDone(cmdDone2, 5*time.Millisecond) {
		t.Fatal("command 2 should remain outstanding")
	}
	cq.Remove([]interface{}{wk1})
	if !testCmdDone(cmdDone2, 5*time.Millisecond) {
		t.Fatal("command 2 should finish with no commands outstanding")
	}
}

func TestCommandQueueClear(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()
	wg1 := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}

	// Add multiple commands and commands which access each.
	add(cq, roachpb.Key("a"), nil, false)
	add(cq, roachpb.Key("b"), nil, false)
	getWait(cq, roachpb.Key("a"), nil, false, &wg1)
	getWait(cq, roachpb.Key("b"), nil, false, &wg2)
	cmdDone1 := waitForCmd(&wg1)
	cmdDone2 := waitForCmd(&wg2)

	// Clear the queue and verify both commands are signaled.
	cq.Clear()

	if !testCmdDone(cmdDone1, 100*time.Millisecond) ||
		!testCmdDone(cmdDone2, 100*time.Millisecond) {
		t.Fatal("commands should finish when clearing queue")
	}
}

// TestCommandQueueExclusiveEnd verifies that an end key is treated as
// an exclusive end when GetWait calculates overlapping commands. Test
// it by calling GetWait with a command whose start key is equal to
// the end key of a previous command.
func TestCommandQueueExclusiveEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()
	add(cq, roachpb.Key("a"), roachpb.Key("b"), false)

	wg := sync.WaitGroup{}
	getWait(cq, roachpb.Key("b"), nil, false, &wg)
	// Verify no wait on the second writer command on "b" since
	// it does not overlap with the first command on ["a", "b").
	wg.Wait()
}

// TestCommandQueueSelfOverlap makes sure that GetWait adds all of the
// key ranges simultaneously. If that weren't the case, all but the first
// span would wind up waiting on overlapping previous spans, resulting
// in deadlock.
func TestCommandQueueSelfOverlap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	cq := NewCommandQueue()
	a := roachpb.Key("a")
	k := add(cq, a, roachpb.Key("b"), false)
	var wg sync.WaitGroup
	cq.GetWait(false, &wg, []roachpb.Span{{Key: a}, {Key: a}, {Key: a}}...)
	cq.Remove([]interface{}{k})
	wg.Wait()
}
