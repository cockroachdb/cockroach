// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package idalloc_test

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/idalloc"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	// Import to set ZoneConfigHook.
	_ "github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/localtestcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// newTestAllocator creates and returns a new idalloc.Allocator, backed by a
// LocalTestCluster. The test cluster is returned as well, and should be stopped
// by calling Stop.
func newTestAllocator(t testing.TB) (*localtestcluster.LocalTestCluster, *idalloc.Allocator) {
	s := &localtestcluster.LocalTestCluster{
		DisableLivenessHeartbeat: true,
		DontCreateSystemRanges:   true,
	}
	s.Start(t, testutils.NewNodeTestBaseContext(), kvcoord.InitFactoryForLocalTestCluster)
	idAlloc, err := idalloc.NewAllocator(
		s.Cfg.AmbientCtx,
		keys.RangeIDGenerator,
		s.DB,
		10, /* blockSize */
		s.Stopper,
	)
	if err != nil {
		s.Stop()
		t.Errorf("failed to create idAllocator: %+v", err)
	}
	return s, idAlloc
}

// TestIDAllocator creates an ID allocator which allocates from
// the Range ID generator system key in blocks of 10 with a minimum
// ID value of 2 and then starts up 10 goroutines each allocating
// 10 IDs. All goroutines deposit the allocated IDs into a final
// channel, which is queried at the end to ensure that all IDs
// from 2 to 101 are present.
func TestIDAllocator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, idAlloc := newTestAllocator(t)
	defer s.Stop()

	const maxI, maxJ = 10, 10
	allocd := make(chan uint32, maxI*maxJ)
	errChan := make(chan error, maxI*maxJ)

	for i := 0; i < maxI; i++ {
		go func() {
			for j := 0; j < maxJ; j++ {
				id, err := idAlloc.Allocate(context.Background())
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

func TestNewAllocatorInvalidBlockSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	expErr := "blockSize must be a positive integer"
	if _, err := idalloc.NewAllocator(
		log.AmbientContext{Tracer: tracing.NewTracer()},
		nil /* idKey */, nil, /* db */
		0 /* blockSize */, nil, /* stopper */
	); !testutils.IsError(err, expErr) {
		t.Errorf("expected err: %s, got: %+v", expErr, err)
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
	s, idAlloc := newTestAllocator(t)
	defer s.Stop()
	ctx := context.Background()

	const routines = 10
	allocd := make(chan uint32, routines)

	firstID, err := idAlloc.Allocate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if firstID != 2 {
		t.Fatalf("expected ID is 2, but got: %d", firstID)
	}

	// Make Allocator invalid.
	idAlloc.SetIDKey(roachpb.KeyMin)

	// Should be able to get the allocated IDs, and there will be one
	// background Allocate to get ID continuously.
	for i := 0; i < 9; i++ {
		id, err := idAlloc.Allocate(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if int(id) != i+3 {
			t.Fatalf("expected ID is %d, but got: %d", i+3, id)
		}
	}

	errChan := make(chan error, routines)

	// Then the paralleled allocations should be blocked until Allocator
	// is recovered.
	for i := 0; i < routines; i++ {
		go func() {
			select {
			case <-idAlloc.IDs():
				errChan <- errors.Errorf("Allocate() should be blocked until idKey is valid")
			case <-time.After(10 * time.Millisecond):
				errChan <- nil
			}

			id, err := idAlloc.Allocate(context.Background())
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

	// Attempt a few allocations with a context timeout while allocations are
	// blocked. All attempts should hit a context deadline exceeded error.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	for i := 0; i < routines; i++ {
		id, err := idAlloc.Allocate(ctx)
		if id != 0 || err != context.DeadlineExceeded {
			t.Fatalf("expected context cancellation, found id=%d, err=%v", id, err)
		}
	}

	// Make the IDAllocator valid again.
	idAlloc.SetIDKey(keys.RangeIDGenerator)
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
		if ids[i] != i+12 {
			t.Errorf("expected \"%d\"th ID to be %d; got %d", i, i+12, ids[i])
		}
	}

	// Check if the following allocations return expected ID.
	for i := 0; i < routines; i++ {
		id, err := idAlloc.Allocate(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if int(id) != i+22 {
			t.Errorf("expected ID is %d, but got: %d", i+22, id)
		}
	}
}

func TestAllocateWithStopper(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, idAlloc := newTestAllocator(t)
	s.Stop() // not deferred.

	if _, err := idAlloc.Allocate(context.Background()); !testutils.IsError(err, "system is draining") {
		t.Errorf("unexpected error: %+v", err)
	}
}
