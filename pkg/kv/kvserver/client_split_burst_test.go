// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"math"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

type splitBurstTest struct {
	*testcluster.TestCluster
	baseKey                     roachpb.Key
	magicStickyBit              hlc.Timestamp
	numSplitsSeenOnSlowFollower *int32 // atomic
	initialRaftSnaps            int
}

func (sbt *splitBurstTest) SplitWithDelay(t *testing.T, location byte) {
	t.Helper()
	require.NoError(t, sbt.SplitWithDelayE(location))
}

func (sbt *splitBurstTest) SplitWithDelayE(location byte) error {
	k := append([]byte(nil), sbt.baseKey...)
	splitKey := append(k, location)
	_, _, err := sbt.SplitRangeWithExpiration(splitKey, sbt.magicStickyBit)
	return err
}

func (sbt *splitBurstTest) NumRaftSnaps(t *testing.T) int {
	var totalSnaps int
	for i := range sbt.Servers {
		var n int // num rows (sanity check against test rotting)
		var c int // num Raft snapshots
		if err := sbt.ServerConn(i).QueryRow(`
SELECT count(*), sum(value) FROM crdb_internal.node_metrics WHERE
	name = 'range.snapshots.applied-voter'
`).Scan(&n, &c); err != nil {
			t.Fatal(err)
		}
		require.EqualValues(t, 1, n)
		totalSnaps += c
	}
	return totalSnaps - sbt.initialRaftSnaps
}

func setupSplitBurstTest(t *testing.T, delay time.Duration) *splitBurstTest {
	var magicStickyBit = hlc.Timestamp{WallTime: math.MaxInt64 - 123, Logical: 987654321}

	numSplitsSeenOnSlowFollower := new(int32) // atomic
	var quiesceCh <-chan struct{}
	knobs := base.TestingKnobs{Store: &kvserver.StoreTestingKnobs{
		TestingApplyFilter: func(args kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
			if args.Split == nil || delay == 0 {
				return 0, nil
			}
			if args.Split.RightDesc.GetStickyBit() != magicStickyBit {
				return 0, nil
			}
			select {
			case <-time.After(delay):
			case <-quiesceCh:
			}
			atomic.AddInt32(numSplitsSeenOnSlowFollower, 1)
			return 0, nil
		},
	}}

	ctx := context.Background()

	// n1 and n3 are fast, n2 is slow (to apply the splits). We need
	// three nodes here; delaying the apply loop on n2 also delays
	// how quickly commands can reach quorum and would backpressure
	// the splits by accident.
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			1: {Knobs: knobs},
		},
		ReplicationMode: base.ReplicationManual,
	})
	defer t.Cleanup(func() {
		tc.Stopper().Stop(ctx)
	})
	quiesceCh = tc.Stopper().ShouldQuiesce()

	k := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, k, tc.Target(1), tc.Target(2))

	sbc := &splitBurstTest{
		TestCluster:                 tc,
		baseKey:                     k,
		magicStickyBit:              magicStickyBit,
		numSplitsSeenOnSlowFollower: numSplitsSeenOnSlowFollower,
	}
	sbc.initialRaftSnaps = sbc.NumRaftSnaps(t)
	return sbc
}

func TestSplitBurstWithSlowFollower(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	t.Run("forward", func(t *testing.T) {
		// When splitting at an increasing sequence of keys, in each step we split
		// the most recently split range, and we expect the splits to wait for that
		// range to have caught up its follower across the preceding split, which
		// was delayed as well. So when we're done splitting we should have seen at
		// least (numSplits-1) splits get applied on the slow follower.
		// This end-to-end exercises `splitDelayHelper`.
		//
		// This test is fairly slow because every split will incur a 1s penalty
		// (dictated by the raft leader's probing interval). We could fix this
		// delay here and in production if we had a way to send a signal from
		// the slow follower to the leader when the split trigger initializes
		// the right-hand side. This is actually an interesting point, because
		// the split trigger *replaces* a snapshot - but doesn't fully act like
		// one: when a raft group applies a snapshot, it generates an MsgAppResp
		// to the leader which will let the leader probe proactively. We could
		// signal the split trigger to the raft group as a snapshot being applied
		// (as opposed to recreating the in-memory instance as we do now), and
		// then this MsgAppResp should be created automatically.

		sbt := setupSplitBurstTest(t, 50*time.Millisecond)
		defer sbt.Stopper().Stop(ctx)

		const numSplits = byte(5)

		for i := byte(0); i < numSplits; i++ {
			sbt.SplitWithDelay(t, i)
		}
		// We should have applied all but perhaps the last split on the slow node.
		// If we didn't, that indicates a failure to delay the splits accordingly.
		require.GreaterOrEqual(t, atomic.LoadInt32(sbt.numSplitsSeenOnSlowFollower), int32(numSplits-1))
		require.Zero(t, sbt.NumRaftSnaps(t))
	})
	t.Run("backward", func(t *testing.T) {
		// When splitting at a decreasing sequence of keys, we're repeatedly splitting
		// the first range. All of its followers are initialized to begin with, and
		// even though there is a slow follower, `splitDelayHelper` isn't interested in
		// delaying this here (which would imply that it's trying to check that every-
		// one is "caught up").
		// We set a 100s timeout so that below we can assert that `splitDelayHelper`
		// isn't somehow forcing us to wait here.
		infiniteDelay := 100 * time.Second
		sbt := setupSplitBurstTest(t, infiniteDelay)
		defer sbt.Stopper().Stop(ctx)

		const numSplits = byte(50)

		for i := byte(0); i < numSplits; i++ {
			tBegin := timeutil.Now()
			sbt.SplitWithDelay(t, numSplits-i)
			if dur := timeutil.Since(tBegin); dur > infiniteDelay {
				t.Fatalf("waited %s for split #%d", dur, i+1)
			}
		}
		require.Zero(t, atomic.LoadInt32(sbt.numSplitsSeenOnSlowFollower))
		require.Zero(t, sbt.NumRaftSnaps(t))
	})
	t.Run("random", func(t *testing.T) {
		// When splitting randomly, we'll see a mixture of forward and backward
		// splits, so we can't assert on how many split triggers we observe.
		// However, there still shouldn't be any snapshots.
		sbt := setupSplitBurstTest(t, 10*time.Millisecond)
		defer sbt.Stopper().Stop(ctx)

		const numSplits = 20
		perm := rand.Perm(numSplits)

		doSplit := func(ctx context.Context, i int) error {
			return sbt.SplitWithDelayE(byte(perm[i]))
		}
		require.NoError(t, ctxgroup.GroupWorkers(ctx, numSplits, doSplit))

		require.Zero(t, sbt.NumRaftSnaps(t))
	})
}
