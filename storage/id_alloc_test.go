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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestIDAllocator creates an ID allocator which allocates from
// the Raft ID generator system key in blocks of 10 with a minimum
// ID value of 2 and then starts up 10 goroutines each allocating
// 10 IDs. All goroutines deposit the allocated IDs into a final
// channel, which is queried at the end to ensure that all IDs
// from 2 to 101 are present.
func TestIDAllocator(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _ := createTestStore(t)
	defer store.Stop()
	allocd := make(chan int, 100)
	idAlloc, err := NewIDAllocator(engine.KeyRaftIDGenerator, store.db, 2, 10)
	if err != nil {
		t.Errorf("failed to create IDAllocator: %v", err)
	}

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				allocd <- int(idAlloc.Allocate())
			}
		}()
	}

	// Verify all IDs accounted for.
	ids := make([]int, 100)
	for i := 0; i < 100; i++ {
		ids[i] = <-allocd
	}
	sort.Ints(ids)
	for i := 0; i < 100; i++ {
		if ids[i] != i+2 {
			t.Errorf("expected \"%d\"th ID to be %d; got %d", i, i+2, ids[i])
		}
	}

	// Verify no leftover IDs.
	select {
	case id := <-allocd:
		t.Errorf("there appear to be leftover IDs, starting with %d", id)
	default:
		// Expected; noop.
	}
}

// TestIDAllocatorNegativeValue creates an ID allocator against an
// increment key which is preset to a negative value. We verify that
// the id allocator makes a double-alloc to make up the difference
// and push the id allocation into positive integers.
func TestIDAllocatorNegativeValue(t *testing.T) {
	defer leaktest.AfterTest(t)
	store, _ := createTestStore(t)
	defer store.Stop()

	// Increment our key to a negative value.
	newValue, err := engine.MVCCIncrement(store.Engine(), nil, engine.KeyRaftIDGenerator, store.clock.Now(), nil, -1024)
	if err != nil {
		t.Fatal(err)
	}
	if newValue != -1024 {
		t.Errorf("expected new value to be -1024; got %d", newValue)
	}
	idAlloc, err := NewIDAllocator(engine.KeyRaftIDGenerator, store.db, 2, 10)
	if err != nil {
		t.Errorf("failed to create IDAllocator: %v", err)
	}
	value := idAlloc.Allocate()
	if value != 2 {
		t.Errorf("expected id allocation to have value 2; got %d", value)
	}
}

// TestNewIDAllocatorInvalidArgs checks validation logic of NewIDAllocator.
func TestNewIDAllocatorInvalidArgs(t *testing.T) {
	args := [][]int64{
		{0, 10}, // minID <= 0
		{2, 0},  // blockSize < 1
	}
	for i := range args {
		if _, err := NewIDAllocator(nil, nil, args[i][0], args[i][1]); err == nil {
			t.Errorf("expect to have error return, but got nil")
		}
	}
}

// TestAllocateErrorAndRecovery has several steps:
// 1) Allocate a set of ID firstly and check.
// 2) Then make IDAllocator invalid, should be able to return existing one.
// 3) After channel becomes empty, allocation will be blocked.
// 4) Make IDAllocator valid again, the blocked allocations return correct ID.
// 5) Check if the following allocations return correctly.
func TestAllocateErrorAndRecovery(t *testing.T) {
	store, _ := createTestStore(t)
	allocd := make(chan int, 10)

	// Firstly create a valid IDAllocator to get some ID.
	idAlloc, err := NewIDAllocator(engine.KeyRaftIDGenerator, store.db, 2, 10)
	if err != nil {
		t.Errorf("failed to create IDAllocator: %v", err)
	}

	if id := idAlloc.Allocate(); id != 2 {
		t.Errorf("expected ID is 2, but got: %d", id)
	}

	// Make Allocator invalid.
	idAlloc.idKey = nil

	// Should be able to get the allocated IDs, and there will be one
	// background allocateBlock to get ID continuously.
	for i := 0; i < 8; i++ {
		if id := int(idAlloc.Allocate()); id != i+3 {
			t.Errorf("expected ID is %d, but got: %d", i+3, id)
		}
	}

	// Then the paralleled allocations should be blocked until Allocator
	// is recovered.
	for i := 0; i < 10; i++ {
		go func() {
			allocd <- int(idAlloc.Allocate())
		}()
	}
	// Make sure no allocation returns.
	time.Sleep(10 * time.Millisecond)
	if len(allocd) != 0 {
		t.Errorf("Allocate() should be blocked until allocateBlock return ID")
	}

	// Make the IDAllocator valid again.
	idAlloc.idKey = engine.KeyRaftIDGenerator
	// Check if the blocked allocations return expected ID.
	ids := make([]int, 10)
	for i := 0; i < 10; i++ {
		ids[i] = <-allocd
	}
	sort.Ints(ids)
	for i := 0; i < 10; i++ {
		if ids[i] != i+11 {
			t.Errorf("expected \"%d\"th ID to be %d; got %d", i, i+11, ids[i])
		}
	}

	// Check if the following allocations return expected ID.
	for i := 0; i < 10; i++ {
		if id := int(idAlloc.Allocate()); id != i+21 {
			t.Errorf("expected ID is %d, but got: %d", i+21, id)
		}
	}
}
