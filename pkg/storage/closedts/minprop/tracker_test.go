// Copyright 2018 The Cockroach Authors.
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

package minprop

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestTrackerClosure(t *testing.T) {
	ctx := context.Background()
	tracker := NewTracker()
	_, done := tracker.Track(ctx)

	done(ctx, 1, 100, 200)
	done(ctx, 1, 0, 0)
}

func ExampleTracker_Close() {
	ctx := context.Background()
	tracker := NewTracker()
	const ep1 ctpb.Epoch = 1
	_, slow := tracker.Track(ctx)
	_, _, _ = tracker.Close(hlc.Timestamp{WallTime: 1E9}, ep1)
	_, fast := tracker.Track(ctx)

	fmt.Println("Slow proposal finishes at LAI 2")
	slow(ctx, 1, 99, 2)
	closed, m, ok := tracker.Close(hlc.Timestamp{WallTime: 2E9}, ep1)
	fmt.Println("Closed:", closed, m, ok)

	fmt.Println("Fast proposal finishes at LAI 1")
	fast(ctx, 1, 99, 1)
	fmt.Println(tracker)
	closed, m, ok = tracker.Close(hlc.Timestamp{WallTime: 3E9}, ep1)
	fmt.Println("Closed:", closed, m, ok)
	fmt.Println("Note how the MLAI has 'regressed' from 2 to 1. The consumer")
	fmt.Println("needs to track the maximum over all deltas received.")

	// Output:
	// Slow proposal finishes at LAI 2
	// Closed: 1.000000000,0 map[99:2] true
	// Fast proposal finishes at LAI 1
	//
	//   closed=1.000000000,0
	//       |            next=2.000000000,0
	//       |          left | right
	//       |             0 # 0
	//       |             1 @        (r99)
	//       v               v
	// ---------------------------------------------------------> time
	//
	// Closed: 2.000000000,0 map[99:1] true
	// Note how the MLAI has 'regressed' from 2 to 1. The consumer
	// needs to track the maximum over all deltas received.
}

func TestTrackerDoubleRelease(t *testing.T) {
	var exited bool
	log.SetExitFunc(true /* hideStack */, func(int) { exited = true })
	defer log.ResetExitFunc()

	ctx := context.Background()
	tracker := NewTracker()

	_, release := tracker.Track(ctx)
	release(ctx, 1, 0, 0)
	release(ctx, 1, 4, 10)

	if !exited {
		t.Fatal("expected fatal error")
	}
}

func TestTrackerReleaseZero(t *testing.T) {
	ctx := context.Background()
	tracker := NewTracker()
	const ep1 ctpb.Epoch = 1
	trackedTs1, release1 := tracker.Track(ctx)
	trackedTs2, release2 := tracker.Track(ctx)
	release2(ctx, ep1, 2, 0)
	leftTs, _, _ := tracker.Close(trackedTs2, ep1)
	leftTs.Logical += 2
	release1(ctx, ep1, 1, 0)
	closedTs, mlais, ok := tracker.Close(leftTs, ep1)
	if !ok {
		t.Fatalf("expected closed to succeed")
	} else if closedTs != trackedTs1 {
		t.Fatalf("expected to have closed %v, got %v %v", trackedTs1, closedTs, mlais)
	} else if mlai1, found := mlais[1]; !found {
		t.Fatalf("expected to find mlai for range 1")
	} else if mlai1 != 0 {
		t.Fatalf("expected to find zero mlai for range 1, got %v", mlai1)
	} else if mlai2, found := mlais[2]; !found {
		t.Fatalf("expected to find mlai for range 2")
	} else if mlai2 != 0 {
		t.Fatalf("expected to find zero mlai for range 2, got %v", mlai2)
	}
}

type modelClient struct {
	lai map[roachpb.RangeID]*int64 // read-only map, values accessed atomically
	mu  struct {
		syncutil.Mutex
		closed   []hlc.Timestamp                // closed timestamps
		released []map[roachpb.RangeID]ctpb.LAI // known released LAIs, rotated on Close
		m        map[roachpb.RangeID]ctpb.LAI   // max over all maps returned from Close()
	}
}

// Operate a Tracker concurrently and verify that closed timestamps don't regress
// and that the emitted MLAIs are not obviously inconsistent with commands we know
// finished.
func TestTrackerConcurrentUse(t *testing.T) {
	ctx := context.Background()
	tracker := NewTracker()

	const (
		numCmds    = 1000 // operations to carry out in total
		closeEvery = 20   // turn every i'th operation into a Close
		numRanges  = 5
	)

	var mc modelClient
	mc.mu.m = map[roachpb.RangeID]ctpb.LAI{}
	mc.mu.closed = make([]hlc.Timestamp, 1)
	mc.mu.released = []map[roachpb.RangeID]ctpb.LAI{{}, {}, {}}

	mc.lai = map[roachpb.RangeID]*int64{}
	for i := roachpb.RangeID(1); i <= numRanges; i++ {
		mc.lai[i] = new(int64)
	}

	get := func(i int) (roachpb.RangeID, ctpb.LAI) {
		rangeID := roachpb.RangeID(1 + (i % numRanges))
		return rangeID, ctpb.LAI(atomic.AddInt64(mc.lai[rangeID], 1))
	}

	// It becomes a lot more complicated to collect the released indexes
	// correctly when multiple calls to Close are in-flight at any given time.
	// The intended use case is for Close to be called from a single goroutine,
	// so the test specializes to that situation.
	//
	// NB: The `mc.mu` sections are intentionally kept small to allow for more
	// interleaving between tracked commands and close operations.
	var closeMU syncutil.Mutex
	const ep1 ctpb.Epoch = 1
	close := func(newNext hlc.Timestamp) error {
		closeMU.Lock()
		defer closeMU.Unlock()
		mc.mu.Lock()
		// Note last closed timestamp.
		prevClosed := mc.mu.closed[len(mc.mu.closed)-1]

		mc.mu.Unlock()

		t.Log("before closing:", tracker)
		// Ignore epoch mismatches which may occur before any values have been
		// released from the tracker.
		closed, m, _ := tracker.Close(newNext, ep1)
		if closed.Less(prevClosed) {
			return errors.Errorf("closed timestamp regressed from %s to %s", prevClosed, closed)
		} else if prevClosed == closed && len(m) != 0 {
			return errors.Errorf("closed timestamp %s not incremented, but MLAIs %v emitted", prevClosed, m)
		}

		mc.mu.Lock()
		defer mc.mu.Unlock()

		if closed != prevClosed {
			// The released bucket is rotated after each call to Close (we can't
			// really do it before because we only want to rotate when a new
			// closed timestamp was established).
			//
			// Taking into account the call to Close we just performed, the
			// - current bucket contains: commands that could be on the left
			//   (expected) or the right: A command could start after our call to
			//   Close but make it into the pre-rotation bucket.
			// - previous bucket contains commands that could be on the left
			//   or emitted
			// - bucket before that contains commands that definitely must have
			//   been emitted.
			//
			// So we check the latter bucket. Trying to close the synchronization
			// gap would allow checking the middle bucket instead, but this would
			// weaken the test overall.
			released := mc.mu.released[len(mc.mu.released)-3]
			// Rotate released commands bucket.
			mc.mu.released = append(mc.mu.released, map[roachpb.RangeID]ctpb.LAI{})

			for rangeID, mlai := range m {
				// Intuitively you expect mc.mu.m[rangeID] < mlai, but this
				// doesn't always hold. A slow proposal could get assigned a
				// higher lease index on the left side than a "newer"
				// proposal on the right. The client really has to track the
				// maximum.
				//
				if mc.mu.m[rangeID] < mlai {
					mc.mu.m[rangeID] = mlai
				}

				if trackedMLAI, rMLAI := mc.mu.m[rangeID], released[rangeID]; rMLAI > trackedMLAI {
					return errors.Errorf(
						"incorrect MLAI %d for r%d does not reflect %d:\nemitted: %+v\n%s\nreleased: %s\naggregate: %s",
						trackedMLAI, rangeID, rMLAI, m, tracker, pretty.Sprint(mc.mu.released), pretty.Sprint(mc.mu.m),
					)
				}
			}
		}

		// Store latest closed timestamp.
		mc.mu.closed = append(mc.mu.closed, closed)
		return nil
	}

	newNext := func(i int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: int64(i) * 1E9}
	}

	run := func(i int) error {
		if i%closeEvery == 1 {
			return close(newNext(i))
		}

		mc.mu.Lock()
		prevClosed := mc.mu.closed[len(mc.mu.closed)-1]
		mc.mu.Unlock()

		ts, done := tracker.Track(ctx)
		if ts.Less(prevClosed) {
			return errors.Errorf("%d: proposal forwarded to %s, but closed %s", i, ts, prevClosed)
		}

		runtime.Gosched()

		var rangeID roachpb.RangeID
		var lai ctpb.LAI
		switch i % 3 {
		case 0:
			// Successful evaluation.
			rangeID, lai = get(i)
			done(ctx, ep1, rangeID, lai)
		case 1:
			// Successful evaluation followed by deferred zero call.
			rangeID, lai = get(i)
			done(ctx, ep1, rangeID, lai)
			done(ctx, ep1, 0, 0)
		case 2:
			// Failed evaluation. Burns a LAI.
			done(ctx, ep1, 0, 0)
		default:
			panic("the impossible happened")
		}

		mc.mu.Lock()
		if lai != 0 {
			mc.mu.released[len(mc.mu.released)-1][rangeID] = lai
		}
		mc.mu.Unlock()

		return nil
	}

	var g errgroup.Group
	for i := 0; i < numCmds; i++ {
		i := i
		g.Go(func() error {
			return run(i)
		})
	}

	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}

	// We'd like to at least assert something about the MLAIs below, namely that
	// the final view of the client state is equivalent to the MLAIs that were
	// actually used by the proposals. To get there, we need to close out twice:
	// once to flush the right side to the left, and another time to force it
	// to be output.
	for i := 0; i < 2; i++ {
		if err := close(newNext(numCmds + i)); err != nil {
			t.Fatal(err)
		}
	}

	t.Log(tracker)

	for rangeID, addr := range mc.lai {
		assignedMLAI := ctpb.LAI(atomic.LoadInt64(addr))
		mlai := mc.mu.m[rangeID]

		if assignedMLAI > mlai {
			t.Errorf("r%d: assigned %d, but only %d reflected in final MLAI map", rangeID, assignedMLAI, mlai)
		}
	}
}

// TestTrackerCloseEpochChanges tests the interactions between epoch values
// passed to Close and epoch values of proposals being tracked.
func TestTrackerPreviousEpoch(t *testing.T) {
	const (
		ep1 ctpb.Epoch = iota + 1
		ep2
		ep3
	)
	nilTs := hlc.Timestamp{}
	ts1 := hlc.Timestamp{WallTime: 1E9}
	ts2 := hlc.Timestamp{WallTime: 2E9}
	ts3 := hlc.Timestamp{WallTime: 3E9}

	ctx := context.Background()
	tracker := NewTracker()

	// Track and release a proposal on range 1 in ep1.
	_, r1e1lai1 := tracker.Track(ctx)
	r1e1lai1(ctx, ep1, 1, 1)
	// Track another proposal on range 2 which will be released after the first
	// call to close.
	_, r2e2lai1 := tracker.Track(ctx)
	// Attempt to close the current left side and with a higher epoch and ensure
	// that we fail.
	assertClosed(t, nilTs, nil, false)(tracker.Close(ts1, ep2))
	// Release the proposal on the left hand side in epoch 2 so that we would
	// see it on this next call to Close.
	r2e2lai1(ctx, ep2, 2, 1)
	// Track and release a proposal on range 2 in epoch 2 with LAI 2.
	// This proposal is on the right side.
	_, r1e1lai2 := tracker.Track(ctx)
	r1e1lai2(ctx, ep1, 2, 2)
	// Track and release a proposal on range 1 in epoch 3 with LAI 2.
	// This proposal is on the right side but is in a higher epoch.
	_, r2e3lai1 := tracker.Track(ctx)
	r2e3lai1(ctx, ep3, 1, 2)
	// Close the current left side and assert that the tracker value on the
	// range 1 epoch 1 value from the first interval is not present.
	assertClosed(t, ts1, mlais{2: 1}, true)(tracker.Close(ts2, ep2))
	// Close with the old epoch 2 which should return the same
	// closed timestamp as above.
	assertClosed(t, ts1, nil, true)(tracker.Close(ts3, ep2))
	// And again for good measure.
	assertClosed(t, ts1, nil, true)(tracker.Close(ts3, ep2))
	// Close with epoch 3 and expect to see the tracked proposal for range 1 in
	// epoch 3.
	assertClosed(t, ts2, mlais{1: 2}, true)(tracker.Close(ts3, ep3))
}

// TestTrackerMultipleEpochsReleased tests that when proposals submitted between
// calls to Close span multiple epochs, only data for the highest epoch are
// retained and reported.
func TestTrackerMultipleEpochsReleased(t *testing.T) {
	const (
		ep1 ctpb.Epoch = iota + 1
		ep2
	)
	ts0 := hlc.Timestamp{Logical: 1}
	ts1 := hlc.Timestamp{WallTime: 1E9}
	ts2 := hlc.Timestamp{WallTime: 2E9}
	ts3 := hlc.Timestamp{WallTime: 3E9}

	ctx := context.Background()
	tracker := NewTracker()

	// Track and release a proposal on range 1 in ep1.
	_, r1e1lai1 := tracker.Track(ctx)
	r1e1lai1(ctx, ep1, 1, 1)
	// Begin tracking a proposal on range 2 which won't be released until after
	// the next call to Close.
	_, r2e2lai1 := tracker.Track(ctx)
	// Close the current left side and assert that the tracker reports an empty
	// MLAI map in epoch 1 for the initial timestamp value.
	assertClosed(t, ts0, mlais{}, true)(tracker.Close(ts1, ep1))
	// Track and release another proposal on range 1 in epoch 1 with LAI 2.
	// This proposal is on the right side.
	_, r1e1lai2 := tracker.Track(ctx)
	r1e1lai2(ctx, ep1, 1, 2)
	// Release the proposal for range 2 in epoch 2 which should be on the left
	// side. This release call will invalidate the LAI for range 1 that was
	// recorded in epoch 1 both on the left and right side.
	r2e2lai1(ctx, ep2, 2, 1)
	// Close the current left side and assert that the tracker value on the
	// range 1 epoch 1 value from the first interval is not present.
	assertClosed(t, ts1, mlais{2: 1}, true)(tracker.Close(ts2, ep2))
	assertClosed(t, ts1, nil, true)(tracker.Close(ts2, ep2))
	assertClosed(t, ts2, mlais{}, true)(tracker.Close(ts3, ep2))
}

type mlais = map[roachpb.RangeID]ctpb.LAI

func assertClosed(
	t *testing.T, expTs hlc.Timestamp, expM mlais, expOk bool,
) func(hlc.Timestamp, mlais, bool) {
	return func(
		ts hlc.Timestamp,
		m mlais,
		ok bool,
	) {
		t.Helper()
		assert.Equal(t, expOk, ok)
		assert.Equal(t, expTs, ts)
		assert.EqualValues(t, expM, m)
	}
}
