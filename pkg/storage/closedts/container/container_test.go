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

package container_test // intentionally test from external package

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/container"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	providertestutils "github.com/cockroachdb/cockroach/pkg/storage/closedts/provider/testutils"
	transporttestutils "github.com/cockroachdb/cockroach/pkg/storage/closedts/transport/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/kr/pretty"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

type LateBoundDialer struct {
	Wrapped *transporttestutils.ChanDialer
}

func (d *LateBoundDialer) Dial(ctx context.Context, nodeID roachpb.NodeID) (ctpb.Client, error) {
	return d.Wrapped.Dial(ctx, nodeID)
}

func (d *LateBoundDialer) Ready(nodeID roachpb.NodeID) bool {
	return d.Wrapped.Ready(nodeID)
}

type TestContainer struct {
	*container.Container
	NodeID    roachpb.NodeID
	Refreshed struct {
		syncutil.Mutex
		RangeIDs []roachpb.RangeID
	}
	Dialer    *LateBoundDialer
	TestClock *providertestutils.TestClock
}

func prepareContainer() *TestContainer {
	stopper := stop.NewStopper()

	tc := &TestContainer{}

	tc.TestClock = providertestutils.NewTestClock(stopper)

	var wg sync.WaitGroup
	wg.Add(1)
	refresh := func(requested ...roachpb.RangeID) {
		tc.Refreshed.Lock()
		tc.Refreshed.RangeIDs = append(tc.Refreshed.RangeIDs, requested...)
		tc.Refreshed.Unlock()
	}

	st := cluster.MakeTestingClusterSettings()

	// Set the target duration to a second and the close fraction so small
	// that the Provider will essentially close in a hot loop. In this test
	// we'll block in the clock to pace the Provider's closer loop.
	closedts.TargetDuration.Override(&st.SV, time.Second)
	closedts.CloseFraction.Override(&st.SV, 1E-9)

	// We perform a little dance with the Dialer. It needs to be hooked up to the
	// Server, but that's only created in NewContainer. The Dialer isn't used until
	// that point, so we just create it a little later.
	tc.Dialer = &LateBoundDialer{}

	cfg := container.Config{
		Settings: st,
		Stopper:  stopper,
		Clock:    tc.TestClock.LiveNow,
		Refresh:  refresh,
		Dialer:   tc.Dialer,
	}

	tc.Container = container.NewContainer(cfg)
	return tc
}

func setupTwoNodeTest() (_ *TestContainer, _ *TestContainer, shutdown func()) {
	c1 := prepareContainer()
	c2 := prepareContainer()

	c1.NodeID = roachpb.NodeID(1)
	c2.NodeID = roachpb.NodeID(2)

	c1.Start(c1.NodeID)
	c2.Start(c2.NodeID)

	// Link the containers.
	c1.Dialer.Wrapped = transporttestutils.NewChanDialer(c1.Stopper, c2.Server)
	c2.Dialer.Wrapped = transporttestutils.NewChanDialer(c2.Stopper, c1.Server)

	return c1, c2, func() {
		// Oh, the joy of multiple stoppers.
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			c1.Stopper.Stop(context.Background())
		}()
		go func() {
			defer wg.Done()
			c2.Stopper.Stop(context.Background())
		}()
	}
}

func TestTwoNodes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	c1, c2, shutdown := setupTwoNodeTest()
	defer shutdown()
	defer func() {
		t.Logf("n1 -> n2: %s", pretty.Sprint(c1.Dialer.Wrapped.Transcript(c2.NodeID)))
		t.Logf("n2 -> n1: %s", pretty.Sprint(c2.Dialer.Wrapped.Transcript(c1.NodeID)))
	}()

	// Initially, can't serve random things for either n1 or n2.
	require.False(t, c1.Container.Provider.CanServe(
		c1.NodeID, hlc.Timestamp{}, roachpb.RangeID(5), ctpb.Epoch(0), ctpb.LAI(0)),
	)
	require.False(t, c1.Container.Provider.CanServe(
		c2.NodeID, hlc.Timestamp{}, roachpb.RangeID(5), ctpb.Epoch(0), ctpb.LAI(0)),
	)

	// Track and release a command.
	ts, release := c1.Tracker.Track(ctx)
	release(ctx, roachpb.RangeID(17), ctpb.LAI(12))

	// The command is forced above ts=0.2. This is just an artifact of how the
	// Tracker is implemented - it closes out 0.1 first, so it begins by forcing
	// commands just above that.
	require.Equal(t, hlc.Timestamp{Logical: 2}, ts)

	// The clock gives a timestamp to the Provider, which should close out the
	// current timestamp and set up 2E9-1E9=1E9 as the next one it wants to close.
	// We do this twice (for the same timestamp) to make sure that the Provider
	// not only read the tick, but also processed it. Otherwise, it becomes hard
	// to write the remainder of the test because the commands we track below may
	// fall into either case, and may be forced above the old or new timestamp.
	for i := 0; i < 2; i++ {
		c1.TestClock.Tick(hlc.Timestamp{WallTime: 2E9}, ctpb.Epoch(1), nil)
	}

	// The Tracker still won't let us serve anything, even though it has closed out
	// 0.1 - this is because it has no information about any ranges at that timestamp.
	// (Note that the Tracker may not have processed the closing yet, so if there were
	// a bug here, this test would fail flakily - that's ok).
	require.False(t, c1.Container.Provider.CanServe(
		c1.NodeID, hlc.Timestamp{Logical: 1}, roachpb.RangeID(17), ctpb.Epoch(1), ctpb.LAI(12)),
	)

	// Two more commands come in.
	ts, release = c1.Tracker.Track(ctx)
	release(ctx, roachpb.RangeID(17), ctpb.LAI(16))
	require.Equal(t, hlc.Timestamp{WallTime: 1E9, Logical: 1}, ts)

	ts, release = c1.Tracker.Track(ctx)
	release(ctx, roachpb.RangeID(8), ctpb.LAI(88))
	require.Equal(t, hlc.Timestamp{WallTime: 1E9, Logical: 1}, ts)

	// Now another tick. Shortly after it, we should be able to serve below 1E9, and 2E9 should
	// be the next planned closed timestamp (though we can only verify the former).
	c1.TestClock.Tick(hlc.Timestamp{WallTime: 3E9}, ctpb.Epoch(1), nil)

	testutils.SucceedsSoon(t, func() error {
		if !c1.Container.Provider.CanServe(
			c1.NodeID, hlc.Timestamp{WallTime: 1E9}, roachpb.RangeID(17), ctpb.Epoch(1), ctpb.LAI(12),
		) {
			return errors.New("still can't serve")
		}
		return nil
	})

	// Shouldn't be able to serve the same thing if we haven't caught up yet.
	require.False(t, c1.Container.Provider.CanServe(
		c1.NodeID, hlc.Timestamp{WallTime: 1E9}, roachpb.RangeID(17), ctpb.Epoch(1), ctpb.LAI(11),
	))

	// Shouldn't be able to serve at a higher timestamp.
	require.False(t, c1.Container.Provider.CanServe(
		c1.NodeID, hlc.Timestamp{WallTime: 1E9, Logical: 1}, roachpb.RangeID(17), ctpb.Epoch(1), ctpb.LAI(12),
	))

	// Now things get a little more interesting. Tell node2 to get a stream of
	// information from node1. We do this via Request, which as a side effect lets
	// us ascertain that this request makes it to n1.
	c2.Clients.Request(roachpb.NodeID(1), roachpb.RangeID(18))
	testutils.SucceedsSoon(t, func() error {
		exp := []roachpb.RangeID{18}
		c1.Refreshed.Lock()
		defer c1.Refreshed.Unlock()
		if !reflect.DeepEqual(exp, c1.Refreshed.RangeIDs) {
			return errors.Errorf("still waiting for %v: currently %v", exp, c1.Refreshed.RangeIDs)
		}
		return nil
	})

	// And n2 should soon also be able to serve follower reads for a range lead by
	// n1 when it has caught up.
	testutils.SucceedsSoon(t, func() error {
		if !c2.Container.Provider.CanServe(
			c1.NodeID, hlc.Timestamp{WallTime: 1E9}, roachpb.RangeID(17), ctpb.Epoch(1), ctpb.LAI(12),
		) {
			return errors.New("n2 still can't serve")
		}
		return nil
	})

	// Remember the other proposals we tracked above on n1: (r17, 16) and (r8, 88). Feeding another
	// timestamp to n1, we should see them closed out at t=2E9, and both n1 and n2 should automatically
	// be able to serve them soon thereafter.
	c1.TestClock.Tick(hlc.Timestamp{WallTime: 4E9}, ctpb.Epoch(1), nil)

	checkEpoch1Reads := func(ts hlc.Timestamp) {
		for i, c := range []*TestContainer{c1, c2} {
			for _, tuple := range []struct {
				roachpb.RangeID
				ctpb.LAI
			}{
				{17, 16},
				{8, 88},
			} {
				testutils.SucceedsSoon(t, func() error {
					if !c.Container.Provider.CanServe(
						c1.NodeID, ts, tuple.RangeID, ctpb.Epoch(1), tuple.LAI,
					) {
						return errors.Errorf("n%d still can't serve (r%d,%d) @ %s", i+1, tuple.RangeID, tuple.LAI, ts)
					}
					return nil
				})
				// Still can't serve when not caught up.
				require.False(t, c.Container.Provider.CanServe(
					c1.NodeID, ts, tuple.RangeID, ctpb.Epoch(1), tuple.LAI-1,
				))
				// Can serve when more than caught up.
				require.True(t, c.Container.Provider.CanServe(
					c1.NodeID, ts, tuple.RangeID, ctpb.Epoch(1), tuple.LAI+1,
				))
				// Can't serve when in different epoch, no matter larger or smaller.
				require.False(t, c.Container.Provider.CanServe(
					c1.NodeID, ts, tuple.RangeID, ctpb.Epoch(0), tuple.LAI,
				))
				require.False(t, c.Container.Provider.CanServe(
					c1.NodeID, ts, tuple.RangeID, ctpb.Epoch(2), tuple.LAI,
				))
			}
		}
	}
	checkEpoch1Reads(hlc.Timestamp{WallTime: 2E9})

	// Uh-oh! n1 must've missed a heartbeat. The epoch goes up by one. This means
	// that soon (after the next tick) timestamps should be closed out under the
	// the epoch. 3E9 gets closed out under the first epoch in this tick. The
	// timestamp at which this happens is doctored to make sure the Storage holds
	// on to the past information, because we want to end-to-end test that this all
	// works out. Consequently we try Tick at the rotation interval plus the target
	// duration next (so that the next closed timestamp is the rotation interval).
	c1.TestClock.Tick(hlc.Timestamp{WallTime: int64(container.StorageBucketScale) + 5E9}, ctpb.Epoch(2), nil)

	// Previously valid reads should remain valid.
	checkEpoch1Reads(hlc.Timestamp{WallTime: 2E9})
	checkEpoch1Reads(hlc.Timestamp{WallTime: 3E9})

	// Commands get forced above next closed timestamp (from the tick above) minus target interval.
	ts, release = c1.Tracker.Track(ctx)
	release(ctx, roachpb.RangeID(123), ctpb.LAI(456))
	require.Equal(t, hlc.Timestamp{WallTime: int64(container.StorageBucketScale) + 4E9, Logical: 1}, ts)

	// With the next tick, epoch two fully goes into effect (as the first epoch two
	// timestamp gets closed out). We do this twice to make sure it's processed before
	// the test proceeds.
	c1.TestClock.Tick(hlc.Timestamp{WallTime: int64(container.StorageBucketScale) + 6E9}, ctpb.Epoch(2), nil)

	// Previously valid reads should remain valid. Note that this is because the
	// storage keeps historical data, and we've fine tuned the epoch flip so that
	// it happens after the epoch 1 information rotates into another bucket and
	// thus is preserved. If the epoch changed at a smaller timestamp, that
	// would've wiped out the first epoch's information.
	//
	// TODO(tschottdorf): we could make the storage smarter so that it forces a
	// rotation when the epoch changes, at the expense of pushing out historical
	// information earlier. Frequent epoch changes could lead to very little
	// historical information in the storage. Probably better not to risk that.
	checkEpoch1Reads(hlc.Timestamp{WallTime: 2E9})
	checkEpoch1Reads(hlc.Timestamp{WallTime: 3E9})

	// Another second, another tick. Now the proposal tracked during epoch 2 should
	// be readable from followers (as `scale+5E9` gets closed out).
	c1.TestClock.Tick(hlc.Timestamp{WallTime: int64(container.StorageBucketScale) + 7E9}, ctpb.Epoch(2), nil)
	for i, c := range []*TestContainer{c1, c2} {
		rangeID := roachpb.RangeID(123)
		lai := ctpb.LAI(456)
		epoch := ctpb.Epoch(2)
		ts := hlc.Timestamp{WallTime: int64(container.StorageBucketScale) + 5E9}

		testutils.SucceedsSoon(t, func() error {
			if !c.Container.Provider.CanServe(
				c1.NodeID, ts, rangeID, epoch, lai,
			) {
				return errors.Errorf("n%d still can't serve (r%d,%d) @ %s", i+1, rangeID, lai, ts)
			}
			return nil
		})

		// Still can't serve when not caught up.
		require.False(t, c.Container.Provider.CanServe(
			c1.NodeID, ts, rangeID, epoch, lai-1,
		))

		// Can serve when more than caught up.
		require.True(t, c.Container.Provider.CanServe(
			c1.NodeID, ts, rangeID, epoch, lai+1,
		))

		// Can't serve when in different epoch, no matter larger or smaller.
		require.False(t, c.Container.Provider.CanServe(
			c1.NodeID, ts, rangeID, epoch-1, lai,
		))
		require.False(t, c.Container.Provider.CanServe(
			c1.NodeID, ts, rangeID, epoch+1, lai,
		))
	}
}
