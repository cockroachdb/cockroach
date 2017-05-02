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
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestIDAllocator creates an ID allocator which allocates from
// the Range ID generator system key in blocks of 10 with a minimum
// ID value of 2 and then starts up 10 goroutines each allocating
// 10 IDs. All goroutines deposit the allocated IDs into a final
// channel, which is queried at the end to ensure that all IDs
// from 2 to 101 are present.
func TestIDAllocator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store, _ := createTestStore(t, stopper)
	const maxI, maxJ = 10, 10
	allocd := make(chan uint32, maxI*maxJ)
	errChan := make(chan error, maxI*maxJ)
	idAlloc, err := newIDAllocator(
		log.AmbientContext{}, keys.RangeIDGenerator, store.cfg.DB, 2, 10, stopper,
	)
	if err != nil {
		t.Errorf("failed to create idAllocator: %v", err)
	}

	for i := 0; i < maxI; i++ {
		go func() {
			for j := 0; j < maxJ; j++ {
				id, err := idAlloc.Allocate()
				errChan <- err
				allocd <- id
			}
		}()
	}

	// Verify all IDs accounted for.
	ids := make([]int, maxI*maxJ)
	for i := range ids {
		ids[i] = int(<-allocd)
		if err := <-errChan; err != nil {
			t.Fatal(err)
		}
	}
	sort.Ints(ids)
	for i := range ids {
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store, _ := createTestStore(t, stopper)

	// Increment our key to a negative value.
	newValue, err := engine.MVCCIncrement(context.Background(), store.Engine(), nil, keys.RangeIDGenerator, store.cfg.Clock.Now(), nil, -1024)
	if err != nil {
		t.Fatal(err)
	}
	if newValue != -1024 {
		t.Errorf("expected new value to be -1024; got %d", newValue)
	}
	idAlloc, err := newIDAllocator(
		log.AmbientContext{}, keys.RangeIDGenerator, store.cfg.DB, 2, 10, stopper,
	)
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
		if _, err := newIDAllocator(
			log.AmbientContext{}, nil, nil, args[i][0], args[i][1], nil,
		); err == nil {
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
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store, _ := createTestStore(t, stopper)
	const routines = 10
	allocd := make(chan uint32, routines)

	// Firstly create a valid IDAllocator to get some ID.
	idAlloc, err := newIDAllocator(
		log.AmbientContext{}, keys.RangeIDGenerator, store.cfg.DB, 2, 10, stopper,
	)
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

	errChan := make(chan error, routines)

	// Then the paralleled allocations should be blocked until Allocator
	// is recovered.
	for i := 0; i < routines; i++ {
		go func() {
			select {
			case <-idAlloc.ids:
				errChan <- errors.Errorf("Allocate() should be blocked until idKey is valid")
			case <-time.After(10 * time.Millisecond):
				errChan <- nil
			}

			id, err := idAlloc.Allocate()
			errChan <- err
			allocd <- id
		}()
	}

	// Wait until all the allocations are blocked.
	for i := 0; i < routines; i++ {
		if err := <-errChan; err != nil {
			t.Fatal(err)
		}
	}

	// Make the IDAllocator valid again.
	idAlloc.idKey.Store(keys.RangeIDGenerator)
	// Check if the blocked allocations return expected ID.
	ids := make([]int, routines)
	for i := range ids {
		ids[i] = int(<-allocd)
		if err := <-errChan; err != nil {
			t.Fatal(err)
		}
	}
	sort.Ints(ids)
	for i := range ids {
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

	// Wrap things in a function so we can defer the stopper, but have it stop
	// before the end of the test.
	idAlloc := func() *idAllocator {
		stopper := stop.NewStopper()
		defer stopper.Stop(context.TODO())
		store, _ := createTestStore(t, stopper)
		idAlloc, err := newIDAllocator(
			log.AmbientContext{}, keys.RangeIDGenerator, store.cfg.DB, 2, 10, stopper,
		)
		if err != nil {
			t.Fatal(err)
		}
		return idAlloc
	}()

	if _, err := idAlloc.Allocate(); !testutils.IsError(err, "system is draining") {
		t.Errorf("unexpected error: %v", err)
	}
}
