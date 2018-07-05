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
	"sync"
	"testing"

	"runtime"

	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

func TestTrackerClosure(t *testing.T) {
	ctx := context.Background()
	tracker := NewTracker()
	_, done := tracker.Track(ctx)

	done(ctx, 100, 200)
	done(ctx, 0, 0)
}

type modelClient struct {
	syncutil.Mutex
	closed hlc.Timestamp
	m      map[roachpb.RangeID]int64
}

func ExampleTracker_Close() {
	ctx := context.Background()
	tracker := NewTracker()
	_, slow := tracker.Track(ctx)
	_, _ = tracker.Close(hlc.Timestamp{WallTime: 1E9})
	_, fast := tracker.Track(ctx)

	fmt.Println("Slow proposal finishes at LAI 2")
	slow(ctx, 99, 2)
	closed, m := tracker.Close(hlc.Timestamp{WallTime: 2E9})
	fmt.Println("Closed:", closed, m)

	fmt.Println("Fast proposal finishes at LAI 1")
	fast(ctx, 99, 1)
	fmt.Println(tracker)

	closed, m = tracker.Close(hlc.Timestamp{WallTime: 3E9})
	fmt.Println("Closed:", closed, m)
	fmt.Println("Note how the MLAI has 'regressed' from 2 to 1. The consumer")
	fmt.Println("needs to track the maximum over all deltas received.")

	// Output:
	// Slow proposal finishes at LAI 2
	// Closed: 1.000000000,0 map[99:2]
	// Fast proposal finishes at LAI 1
	//
	//     closed           next
	//       |          left | right
	//       |             0 # 0
	//       |             1 @        (r99)
	//       v               v
	// ---------------------------------------------------------> time
	//
	// closed = 1.000000000,0
	// next   = 2.000000000,0
	//
	// Closed: 2.000000000,0 map[99:1]
	// Note how the MLAI has 'regressed' from 2 to 1. The consumer
	// needs to track the maximum over all deltas received.
}

// Operate a Tracker concurrently and verify a number of invariants:
// - proposals are forced above the closed timestamp
// -
func TestTrackerConcurrentUse(t *testing.T) {
	ctx := context.Background()
	tracker := NewTracker()

	const numCmds = 1000
	const closeEvery = 20

	var wg sync.WaitGroup
	wg.Add(numCmds)

	var mc modelClient
	mc.m = map[roachpb.RangeID]int64{}
	errCh := make(chan error, numCmds)

	for i := 0; i < numCmds; i++ {
		go func(i int) {
			defer wg.Done()
			mc.Lock()
			closed := mc.closed
			mc.Unlock()

			if i%closeEvery == 1 {
				newNext := hlc.Timestamp{WallTime: int64(i) * 1E9}
				ct, m := tracker.Close(newNext)
				if ct.Less(closed) {
					errCh <- errors.Errorf("%d: closed timestamp regressed from %s to %s", i, closed, ct)
					return
				} else if ct == closed && len(m) != 0 {
					errCh <- errors.Errorf("%d: closed timestamp not incremented, but MLAIs %v emitted", i, m)
					return
				}

				mc.Lock()
				for rangeID, mlai := range m {
					// Intuitively you expect mc.m[rangeID] < mlai, but this
					// doesn't always hold. A slow proposal could get assigned a
					// higher lease index on the left side than a "newer"
					// proposal on the right The client really has to track the
					// maximum.
					//
					// See ExampleTracker_Close.
					if cur := mc.m[rangeID]; cur < mlai {
						mc.m[rangeID] = mlai
					}
				}
				mc.Unlock()
			} else {
				ts, done := tracker.Track(ctx)
				if ts.Less(closed) {
					errCh <- errors.Errorf("%d: proposal forwarded to %s, but closed %s", i, ts, closed)
					return
				}
				runtime.Gosched()
				switch i % 3 {
				case 0:
					// Successful evaluation.
					done(ctx, roachpb.RangeID(i%5), 1+int64(i)/5)
				case 1:
					// Successful evaluation followed by deferred zero call.
					done(ctx, roachpb.RangeID(i%5), 1+int64(i)/5)
					done(ctx, 0, 0)
				case 2:
					// Failed evaluation.
					done(ctx, 0, 0)
				default:
					panic("the impossible happened")
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	// t.Error(tracker)
}
