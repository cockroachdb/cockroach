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
	"fmt"
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
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
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
	idAlloc, err := idalloc.NewAllocator(idalloc.Options{
		AmbientCtx:  s.Cfg.AmbientCtx,
		Key:         keys.RangeIDGenerator,
		Incrementer: idalloc.DBIncrementer(s.DB),
		BlockSize:   10, /* blockSize */
		Stopper:     s.Stopper(),
	})
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
	allocd := make(chan int64, maxI*maxJ)
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
	if _, err := idalloc.NewAllocator(idalloc.Options{
		BlockSize: 0,
	}); !testutils.IsError(err, expErr) {
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

	ctx := context.Background()

	var mu struct {
		syncutil.Mutex
		err     error
		counter int64
	}
	inc := func(_ context.Context, _ roachpb.Key, inc int64) (new int64, _ error) {
		mu.Lock()
		defer mu.Unlock()
		if mu.err != nil {
			return 0, mu.err
		}
		mu.counter += inc
		return mu.counter, nil
	}

	s := stop.NewStopper()
	defer s.Stop(ctx)

	idAlloc, err := idalloc.NewAllocator(idalloc.Options{
		Key:         roachpb.Key("foo"),
		Incrementer: inc,
		BlockSize:   10,
		Stopper:     s,
	})
	require.NoError(t, err)

	const routines = 10
	allocd := make(chan int64, routines)

	firstID, err := idAlloc.Allocate(ctx)
	require.NoError(t, err)

	require.EqualValues(t, 1, firstID)

	// Make Allocator invalid.
	mu.Lock()
	mu.err = errors.New("boom")
	mu.Unlock()

	// Should be able to get the allocated IDs, and there will be one
	// background Allocate to get ID continuously.
	for i := 0; i < 9; i++ {
		id, err := idAlloc.Allocate(ctx)
		require.NoError(t, err)
		require.EqualValues(t, i+2, id)
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
		require.NoError(t, <-errChan)
	}

	// Attempt a few allocations with a context timeout while allocations are
	// blocked. All attempts should hit a context deadline exceeded error.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	for i := 0; i < routines; i++ {
		id, err := idAlloc.Allocate(ctx)
		if id != 0 || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expected context cancellation, found id=%d, err=%v", id, err)
		}
	}

	// Make the IDAllocator valid again.
	mu.Lock()
	mu.err = nil
	mu.Unlock()

	// Check if the blocked allocations return expected ID.
	ids := make([]int, routines)
	for i := range ids {
		ids[i] = int(<-allocd)
		require.NoError(t, <-errChan)
	}
	sort.Ints(ids)
	for i := range ids {
		require.EqualValues(t, i+11, ids[i], "i=%d", i)
	}

	// Check if the following allocations return expected ID.
	for i := 0; i < routines; i++ {
		id, err := idAlloc.Allocate(context.Background())
		require.NoError(t, err)
		require.EqualValues(t, i+21, id, "i=%d", i)
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

// TestLostWriteAssertion makes sure that the Allocator performs a best-effort
// detection of counter regressions.
func TestLostWriteAssertion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s := stop.NewStopper()
	defer s.Stop(ctx)

	var mu struct {
		syncutil.Mutex
		counter int64
		fatal   string
	}

	inc := func(_ context.Context, _ roachpb.Key, inc int64) (new int64, _ error) {
		mu.Lock()
		defer mu.Unlock()
		mu.counter += inc
		return mu.counter, nil
	}

	opts := idalloc.Options{
		Key:         roachpb.Key("foo"),
		Incrementer: inc,
		BlockSize:   10,
		Stopper:     s,
		Fatalf: func(_ context.Context, format string, args ...interface{}) {
			mu.Lock()
			defer mu.Unlock()
			mu.fatal = fmt.Sprintf(format, args...)
		},
	}
	a, err := idalloc.NewAllocator(opts)
	require.NoError(t, err)

	// Trigger first allocation.
	{
		n, err := a.Allocate(ctx)
		require.NoError(t, err)
		require.EqualValues(t, 1, n)
	}

	// Mess with the counter.
	mu.Lock()
	mu.counter--
	mu.Unlock()

	for i := 0; ; i++ {
		n, err := a.Allocate(ctx)
		if err != nil {
			mu.Lock()
			msg := mu.fatal
			mu.Unlock()
			require.Contains(t, msg, "counter corrupt")
			break
		}
		require.EqualValues(t, 2+i, n)
		if i > 10*int(opts.BlockSize) {
			t.Fatal("unexpected infinite loop")
		}
	}
}
