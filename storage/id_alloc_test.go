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
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
)

// TestIDAllocator creates an ID allocator which allocates from
// the Range ID generator system key in blocks of 10 with a minimum
// ID value of 2 and then starts up 10 goroutines each allocating
// 10 IDs. All goroutines deposit the allocated IDs into a final
// channel, which is queried at the end to ensure that all IDs
// from 2 to 101 are present.
func TestIDAllocator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	allocd := make(chan int, 100)
	idAlloc, err := newIDAllocator(keys.RangeIDGenerator, store.ctx.DB, 2, 10, stopper)
	if err != nil {
		t.Errorf("failed to create idAllocator: %v", err)
	}

	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				id, err := idAlloc.Allocate()
				if err != nil {
					t.Fatal(err)
				}
				allocd <- int(id)
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
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	// Increment our key to a negative value.
	newValue, err := engine.MVCCIncrement(context.Background(), store.Engine(), nil, keys.RangeIDGenerator, store.ctx.Clock.Now(), nil, -1024)
	if err != nil {
		t.Fatal(err)
	}
	if newValue != -1024 {
		t.Errorf("expected new value to be -1024; got %d", newValue)
	}
	idAlloc, err := newIDAllocator(keys.RangeIDGenerator, store.ctx.DB, 2, 10, stopper)
	if err != nil {
		t.Errorf("failed to create IDAllocator: %v", err)
	}
	value, err := idAlloc.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	if value != 2 {
		t.Errorf("expected id allocation to have value 2; got %d", value)
	}
}

// TestNewIDAllocatorInvalidArgs checks validation logic of newIDAllocator.
func TestNewIDAllocatorInvalidArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	args := [][]uint32{
		{0, 10}, // minID <= 0
		{2, 0},  // blockSize < 1
	}
	for i := range args {
		if _, err := newIDAllocator(nil, nil, args[i][0], args[i][1], nil); err == nil {
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
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	allocd := make(chan int, 10)

	// Firstly create a valid IDAllocator to get some ID.
	idAlloc, err := newIDAllocator(keys.RangeIDGenerator, store.ctx.DB, 2, 10, stopper)
	if err != nil {
		t.Errorf("failed to create IDAllocator: %v", err)
	}

	firstID, err := idAlloc.Allocate()
	if err != nil {
		t.Fatal(err)
	}
	if firstID != 2 {
		t.Errorf("expected ID is 2, but got: %d", firstID)
	}

	// Make Allocator invalid.
	idAlloc.idKey.Store(roachpb.KeyMin)

	// Should be able to get the allocated IDs, and there will be one
	// background allocateBlock to get ID continuously.
	for i := 0; i < 8; i++ {
		id, err := idAlloc.Allocate()
		if err != nil {
			t.Fatal(err)
		}
		if int(id) != i+3 {
			t.Errorf("expected ID is %d, but got: %d", i+3, id)
		}
	}

	const routines = 10

	var wg sync.WaitGroup
	wg.Add(routines)

	// Then the paralleled allocations should be blocked until Allocator
	// is recovered.
	for i := 0; i < routines; i++ {
		go func() {
			select {
			case <-idAlloc.ids:
				t.Errorf("Allocate() should be blocked until idKey is valid")
			case <-time.After(10 * time.Millisecond):
			}
			wg.Done()

			id, err := idAlloc.Allocate()
			if err != nil {
				t.Fatal(err)
			}
			allocd <- int(id)
		}()
	}

	// Wait until all the allocations are blocked.
	wg.Wait()

	// Make the IDAllocator valid again.
	idAlloc.idKey.Store(keys.RangeIDGenerator)
	// Check if the blocked allocations return expected ID.
	ids := make([]int, routines)
	for i := 0; i < routines; i++ {
		ids[i] = <-allocd
	}
	sort.Ints(ids)
	for i := 0; i < routines; i++ {
		if ids[i] != i+11 {
			t.Errorf("expected \"%d\"th ID to be %d; got %d", i, i+11, ids[i])
		}
	}

	// Check if the following allocations return expected ID.
	for i := 0; i < routines; i++ {
		id, err := idAlloc.Allocate()
		if err != nil {
			t.Fatal(err)
		}
		if int(id) != i+21 {
			t.Errorf("expected ID is %d, but got: %d", i+21, id)
		}
	}
}

func TestAllocateWithStopper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	idAlloc, err := newIDAllocator(keys.RangeIDGenerator, store.ctx.DB, 2, 10, stopper)
	if err != nil {
		log.Fatal(err)
	}

	stopper.Stop()

	if _, err := idAlloc.Allocate(); err == nil {
		t.Errorf("unexpected success")
	} else if !strings.Contains(err.Error(), "system is draining") {
		t.Errorf("unexpected error: %s", err)
	}
}
