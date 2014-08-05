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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/storage/engine"
)

// waitForReader launches a goroutine to wait on the supplied
// WaitGroup. A channel is returned which signals the completion of
// the wait.
func waitForReader(wg *sync.WaitGroup) <-chan struct{} {
	readDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(readDone)
	}()
	return readDone
}

// testReadDone waits for the readDone channel to be closed for at
// most the specified wait duration. Returns true if the read finished
// in the allotted time, false otherwise.
func testReadDone(readDone <-chan struct{}, wait time.Duration) bool {
	select {
	case <-readDone:
		return true
	case <-time.After(wait):
		return false
	}
}

func TestReadQueue(t *testing.T) {
	rq := NewReadQueue()
	wg := sync.WaitGroup{}

	// Try a reader with no writes.
	rq.AddRead(engine.Key("a"), nil, &wg)
	wg.Wait()
	rq.AddRead(engine.Key("a"), engine.Key("b"), &wg)
	wg.Wait()

	// Add a writer and verify wait group is returned for reader.
	wk := rq.AddWrite(engine.Key("a"), nil)
	rq.AddRead(engine.Key("a"), nil, &wg)
	readDone := waitForReader(&wg)
	if testReadDone(readDone, 1*time.Millisecond) {
		t.Fatal("read should not finish with write outstanding")
	}
	rq.RemoveWrite(wk)
	if !testReadDone(readDone, 5*time.Millisecond) {
		t.Fatal("read should finish with no writes outstanding")
	}
}

func TestReadQueueMultipleWrites(t *testing.T) {
	rq := NewReadQueue()
	wg := sync.WaitGroup{}

	// Add multiple writes and add a read which overlaps them all.
	wk1 := rq.AddWrite(engine.Key("a"), nil)
	wk2 := rq.AddWrite(engine.Key("b"), engine.Key("c"))
	wk3 := rq.AddWrite(engine.Key("0"), engine.Key("d"))
	rq.AddRead(engine.Key("a"), engine.Key("cc"), &wg)
	readDone := waitForReader(&wg)
	rq.RemoveWrite(wk1)
	if testReadDone(readDone, 1*time.Millisecond) {
		t.Fatal("read should not finish with two writes outstanding")
	}
	rq.RemoveWrite(wk2)
	if testReadDone(readDone, 1*time.Millisecond) {
		t.Fatal("read should not finish with one write outstanding")
	}
	rq.RemoveWrite(wk3)
	if !testReadDone(readDone, 5*time.Millisecond) {
		t.Fatal("read should finish with no writes outstanding")
	}
}

func TestReadQueueMultipleReads(t *testing.T) {
	rq := NewReadQueue()
	wg1 := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}
	wg3 := sync.WaitGroup{}

	// Add a write which will overlap all reads.
	wk := rq.AddWrite(engine.Key("a"), engine.Key("d"))
	rq.AddRead(engine.Key("a"), nil, &wg1)
	rq.AddRead(engine.Key("b"), nil, &wg2)
	rq.AddRead(engine.Key("c"), nil, &wg3)
	rd1 := waitForReader(&wg1)
	rd2 := waitForReader(&wg2)
	rd3 := waitForReader(&wg3)

	if testReadDone(rd1, 1*time.Millisecond) ||
		testReadDone(rd2, 1*time.Millisecond) ||
		testReadDone(rd3, 1*time.Millisecond) {
		t.Fatal("no reads should finish with write outstanding")
	}
	rq.RemoveWrite(wk)
	if !testReadDone(rd1, 5*time.Millisecond) ||
		!testReadDone(rd2, 5*time.Millisecond) ||
		!testReadDone(rd3, 5*time.Millisecond) {
		t.Fatal("reads should finish with no writes outstanding")
	}
}

func TestReadQueueClear(t *testing.T) {
	rq := NewReadQueue()
	wg1 := sync.WaitGroup{}
	wg2 := sync.WaitGroup{}

	// Add multiple writes and reads which access each.
	rq.AddWrite(engine.Key("a"), nil)
	rq.AddWrite(engine.Key("b"), nil)
	rq.AddRead(engine.Key("a"), nil, &wg1)
	rq.AddRead(engine.Key("b"), nil, &wg2)
	rd1 := waitForReader(&wg1)
	rd2 := waitForReader(&wg2)

	// Clear the read queue and verify both readers are signaled.
	rq.Clear()

	if !testReadDone(rd1, 1*time.Millisecond) ||
		!testReadDone(rd2, 1*time.Millisecond) {
		t.Fatal("reads should finish when clearing read queue")
	}
}
