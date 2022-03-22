// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	raft "go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/tracker"
)

func TestShouldTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		truncatableIndexes uint64
		raftLogSize        int64
		expected           bool
	}{
		{RaftLogQueueStaleThreshold - 1, 0, false},
		{RaftLogQueueStaleThreshold, 0, true},
		{0, RaftLogQueueStaleSize, false},
		{1, RaftLogQueueStaleSize - 1, false},
		{1, RaftLogQueueStaleSize, true},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var d truncateDecision
			d.Input.LogSize = c.raftLogSize
			d.Input.FirstIndex = 123
			d.NewFirstIndex = d.Input.FirstIndex + c.truncatableIndexes
			v := d.ShouldTruncate()
			if c.expected != v {
				t.Fatalf("expected %v, but found %v", c.expected, v)
			}
		})
	}
}

func TestComputeTruncateDecision(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	const targetSize = 1000

	// NB: all tests here have a truncateDecisions which starts with "should
	// truncate: false", because these tests don't simulate enough data to be over
	// the truncation threshold.
	testCases := []struct {
		commit          uint64
		progress        []uint64
		raftLogSize     int64
		firstIndex      uint64
		lastIndex       uint64
		pendingSnapshot uint64
		exp             string
	}{
		{
			// Nothing to truncate.
			1, []uint64{1, 2}, 100, 1, 1, 0,
			"should truncate: false [truncate 0 entries to first index 1 (chosen via: last index)]",
		},
		{
			// Nothing to truncate on this replica, though a quorum elsewhere has more progress.
			// NB this couldn't happen if we're truly the Raft leader, unless we appended to our
			// own log asynchronously.
			1, []uint64{1, 5, 5}, 100, 1, 1, 0,
			"should truncate: false [truncate 0 entries to first index 1 (chosen via: last index)]",
		},
		{
			// We're not truncating anything, but one follower is already cut off. There's no pending
			// snapshot so we shouldn't be causing any additional snapshots.
			2, []uint64{1, 5, 5}, 100, 2, 2, 0,
			"should truncate: false [truncate 0 entries to first index 2 (chosen via: first index)]",
		},
		{
			// The happy case.
			5, []uint64{5, 5, 5}, 100, 2, 5, 0,
			"should truncate: false [truncate 3 entries to first index 5 (chosen via: last index)]",
		},
		{
			// No truncation, but the outstanding snapshot is made obsolete by the truncation. However
			// it was already obsolete before. (This example is also not one you could manufacture in
			// a real system).
			2, []uint64{5, 5, 5}, 100, 2, 2, 1,
			"should truncate: false [truncate 0 entries to first index 2 (chosen via: first index)]",
		},
		{
			// Respecting the pending snapshot.
			5, []uint64{5, 5, 5}, 100, 2, 5, 3,
			"should truncate: false [truncate 1 entries to first index 3 (chosen via: pending snapshot)]",
		},
		{
			// Log is below target size, so respecting the slowest follower.
			3, []uint64{1, 2, 3, 4}, 100, 1, 5, 0,
			"should truncate: false [truncate 0 entries to first index 1 (chosen via: followers)]",
		},
		{
			// Truncating since local log starts at 2. One follower is already cut off without a pending
			// snapshot.
			2, []uint64{1, 2, 3, 4}, 100, 2, 2, 0,
			"should truncate: false [truncate 0 entries to first index 2 (chosen via: first index)]",
		},
		// Don't truncate off active followers, even if over targetSize.
		{
			3, []uint64{1, 3, 3, 4}, 2000, 1, 3, 0,
			"should truncate: false [truncate 0 entries to first index 1 (chosen via: followers); log too large (2.0 KiB > 1000 B)]",
		},
		// Don't truncate away pending snapshot, even when log too large.
		{
			100, []uint64{100, 100}, 2000, 1, 100, 50,
			"should truncate: false [truncate 49 entries to first index 50 (chosen via: pending snapshot); log too large (2.0 KiB > 1000 B)]",
		},
		{
			3, []uint64{1, 3, 3, 4}, 2000, 2, 3, 0,
			"should truncate: false [truncate 0 entries to first index 2 (chosen via: first index); log too large (2.0 KiB > 1000 B)]",
		},
		{
			3, []uint64{1, 3, 3, 4}, 2000, 3, 3, 0,
			"should truncate: false [truncate 0 entries to first index 3 (chosen via: first index); log too large (2.0 KiB > 1000 B)]",
		},
		// Respecting the pending snapshot.
		{
			7, []uint64{4}, 2000, 1, 7, 1,
			"should truncate: false [truncate 0 entries to first index 1 (chosen via: pending snapshot); log too large (2.0 KiB > 1000 B)]",
		},
		// Never truncate past the commit index.
		{
			3, []uint64{3, 3, 6}, 100, 2, 7, 0,
			"should truncate: false [truncate 1 entries to first index 3 (chosen via: commit)]",
		},
		// Never truncate past the last index.
		{
			3, []uint64{5}, 100, 1, 3, 0,
			"should truncate: false [truncate 2 entries to first index 3 (chosen via: last index)]",
		},
		// Never truncate "before the first index".
		{
			3, []uint64{5}, 100, 2, 3, 1,
			"should truncate: false [truncate 0 entries to first index 2 (chosen via: first index)]",
		},
	}
	for i, c := range testCases {
		t.Run("", func(t *testing.T) {
			status := raft.Status{
				Progress: make(map[uint64]tracker.Progress),
			}
			for j, v := range c.progress {
				status.Progress[uint64(j)] = tracker.Progress{
					RecentActive: true,
					State:        tracker.StateReplicate,
					Match:        v,
					Next:         v + 1,
				}
			}
			status.Commit = c.commit
			input := truncateDecisionInput{
				RaftStatus:           status,
				LogSize:              c.raftLogSize,
				MaxLogSize:           targetSize,
				LogSizeTrusted:       true,
				FirstIndex:           c.firstIndex,
				LastIndex:            c.lastIndex,
				PendingSnapshotIndex: c.pendingSnapshot,
			}
			decision := computeTruncateDecision(input)
			if act, exp := decision.String(), c.exp; act != exp {
				t.Errorf("%d:\ngot:\n%s\nwanted:\n%s", i, act, exp)
			}

			// Verify the triggers that queue a range for recomputation. In
			// essence, when the raft log size is not trusted we want to suggest
			// a truncation and also a recomputation. If the size *is* trusted,
			// we'll just see the decision play out as before.
			// The real tests for this are in TestRaftLogQueueShouldQueue, but this is
			// some nice extra coverage.
			should, recompute, prio := (*raftLogQueue)(nil).shouldQueueImpl(ctx, decision)
			assert.Equal(t, decision.ShouldTruncate(), should)
			assert.False(t, recompute)
			assert.Equal(t, decision.ShouldTruncate(), prio != 0)
			input.LogSizeTrusted = false
			input.RaftStatus.RaftState = raft.StateLeader
			if input.LastIndex <= input.FirstIndex {
				input.LastIndex = input.FirstIndex + 1
			}
			decision = computeTruncateDecision(input)
			should, recompute, prio = (*raftLogQueue)(nil).shouldQueueImpl(ctx, decision)
			assert.True(t, should)
			assert.True(t, prio > 0)
			assert.True(t, recompute)
		})
	}
}

// TestComputeTruncateDecisionProgressStatusProbe verifies that when a follower
// is marked as active and is being probed for its log index, we don't truncate
// the log out from under it.
func TestComputeTruncateDecisionProgressStatusProbe(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// NB: most tests here have a truncateDecisions which starts with "should
	// truncate: false", because these tests don't simulate enough data to be over
	// the truncation threshold.
	exp := map[bool]map[bool]string{ // (tooLarge, active)
		false: {
			true:  "should truncate: false [truncate 0 entries to first index 10 (chosen via: probing follower)]",
			false: "should truncate: false [truncate 0 entries to first index 10 (chosen via: first index)]",
		},
		true: {
			true:  "should truncate: false [truncate 0 entries to first index 10 (chosen via: probing follower); log too large (2.0 KiB > 1.0 KiB)]",
			false: "should truncate: true [truncate 190 entries to first index 200 (chosen via: followers); log too large (2.0 KiB > 1.0 KiB)]",
		},
	}

	testutils.RunTrueAndFalse(t, "tooLarge", func(t *testing.T, tooLarge bool) {
		testutils.RunTrueAndFalse(t, "active", func(t *testing.T, active bool) {
			status := raft.Status{
				Progress: make(map[uint64]tracker.Progress),
			}
			progress := []uint64{100, 200, 300, 400, 500}
			lastIndex := uint64(500)
			status.Commit = 300

			for i, v := range progress {
				var pr tracker.Progress
				if v == 100 {
					// A probing follower is probed with some index (Next) but
					// it has a zero Match (i.e. no idea how much of its log
					// agrees with ours).
					pr = tracker.Progress{
						RecentActive: active,
						State:        tracker.StateProbe,
						Match:        0,
						Next:         v,
					}
				} else { // everyone else
					pr = tracker.Progress{
						Match:        v,
						Next:         v + 1,
						RecentActive: true,
						State:        tracker.StateReplicate,
					}
				}
				status.Progress[uint64(i)] = pr
			}

			input := truncateDecisionInput{
				RaftStatus:     status,
				MaxLogSize:     1024,
				FirstIndex:     10,
				LastIndex:      lastIndex,
				LogSizeTrusted: true,
			}
			if tooLarge {
				input.LogSize += 2 * input.MaxLogSize
			}

			decision := computeTruncateDecision(input)
			if s, exp := decision.String(), exp[tooLarge][active]; s != exp {
				t.Errorf("expected %q, got %q", exp, s)
			}
		})
	})
}

func TestTruncateDecisionZeroValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var decision truncateDecision
	assert.False(t, decision.ShouldTruncate())
	assert.Zero(t, decision.NumNewRaftSnapshots())
	assert.Zero(t, decision.NumTruncatableIndexes())
	assert.Equal(t, "should truncate: false [truncate 0 entries to first index 0 (chosen via: ); log size untrusted]", decision.String())
}

func TestTruncateDecisionNumSnapshots(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	status := raft.Status{
		Progress: map[uint64]tracker.Progress{
			// Fully caught up.
			5: {State: tracker.StateReplicate, Match: 11, Next: 12},
			// Behind.
			6: {State: tracker.StateReplicate, Match: 10, Next: 11},
			// Last MsgApp in flight, so basically caught up.
			7: {State: tracker.StateReplicate, Match: 10, Next: 12},
			8: {State: tracker.StateProbe},    // irrelevant
			9: {State: tracker.StateSnapshot}, // irrelevant
		},
	}

	decision := truncateDecision{Input: truncateDecisionInput{RaftStatus: status}}
	assert.Equal(t, 0, decision.raftSnapshotsForIndex(10))
	assert.Equal(t, 1, decision.raftSnapshotsForIndex(11))
	assert.Equal(t, 3, decision.raftSnapshotsForIndex(12))
	assert.Equal(t, 3, decision.raftSnapshotsForIndex(13))
}

func verifyLogSizeInSync(t *testing.T, r *Replica) {
	t.Helper()
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	raftLogSize := r.mu.raftLogSize
	r.mu.Unlock()
	actualRaftLogSize, err := ComputeRaftLogSize(context.Background(), r.RangeID, r.Engine(), r.SideloadedRaftMuLocked())
	if err != nil {
		t.Fatal(err)
	}
	if actualRaftLogSize != raftLogSize {
		t.Fatalf("replica claims raft log size %d, but computed %d", raftLogSize, actualRaftLogSize)
	}
}

func TestUpdateRaftStatusActivity(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type testCase struct {
		prs        []tracker.Progress
		replicas   []roachpb.ReplicaDescriptor
		lastUpdate lastUpdateTimesMap
		now        time.Time

		exp []tracker.Progress
	}

	now := timeutil.Now()

	inactivityThreashold := time.Second

	tcs := []testCase{
		// No data, no crash.
		{},
		// No knowledge = no update.
		{prs: []tracker.Progress{{RecentActive: true}}, exp: []tracker.Progress{{RecentActive: true}}},
		{prs: []tracker.Progress{{RecentActive: false}}, exp: []tracker.Progress{{RecentActive: false}}},
		// See replica in descriptor but then don't find it in the map. Assumes the follower is not
		// active.
		{
			replicas: []roachpb.ReplicaDescriptor{{ReplicaID: 1}},
			prs:      []tracker.Progress{{RecentActive: true}},
			exp:      []tracker.Progress{{RecentActive: false}},
		},
		// Three replicas in descriptor. The first one responded recently, the second didn't,
		// the third did but it doesn't have a Progress.
		{
			replicas: []roachpb.ReplicaDescriptor{{ReplicaID: 1}, {ReplicaID: 2}, {ReplicaID: 3}},
			prs:      []tracker.Progress{{RecentActive: false}, {RecentActive: true}},
			lastUpdate: map[roachpb.ReplicaID]time.Time{
				1: now.Add(-1 * inactivityThreashold / 2),
				2: now.Add(-1 - inactivityThreashold),
				3: now,
			},
			now: now,

			exp: []tracker.Progress{{RecentActive: true}, {RecentActive: false}},
		},
	}

	ctx := context.Background()

	for _, tc := range tcs {
		t.Run("", func(t *testing.T) {
			prs := make(map[uint64]tracker.Progress)
			for i, pr := range tc.prs {
				prs[uint64(i+1)] = pr
			}
			expPRs := make(map[uint64]tracker.Progress)
			for i, pr := range tc.exp {
				expPRs[uint64(i+1)] = pr
			}
			updateRaftProgressFromActivity(ctx, prs, tc.replicas,
				func(replicaID roachpb.ReplicaID) bool {
					return tc.lastUpdate.isFollowerActiveSince(ctx, replicaID, tc.now, inactivityThreashold)
				},
			)

			assert.Equal(t, expPRs, prs)
		})
	}
}

func TestNewTruncateDecisionMaxSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	cfg := TestStoreConfig(hlc.NewClock(hlc.NewManualClock(123).UnixNano, time.Nanosecond))
	const exp = 1881
	cfg.RaftLogTruncationThreshold = exp
	ctx := context.Background()
	store := createTestStoreWithConfig(
		ctx, t, stopper, testStoreOpts{createSystemRanges: true}, &cfg,
	)

	repl, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	td, err := newTruncateDecision(ctx, repl)
	if err != nil {
		t.Fatal(err)
	}

	if td.Input.MaxLogSize != exp {
		t.Fatalf("MaxLogSize %d is unexpected, wanted %d", td.Input.MaxLogSize, exp)
	}
}

// TestNewTruncateDecision verifies that old raft log entries are correctly
// removed.
func TestNewTruncateDecision(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.WithIssue(t, 38584)

	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store, _ := createTestStore(ctx, t,
		testStoreOpts{
			// This test was written before test stores could start with more than one
			// range and was not adapted.
			createSystemRanges: false,
		},
		stopper)
	store.SetRaftLogQueueActive(false)

	r, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	getIndexes := func() (uint64, int, uint64, error) {
		d, err := newTruncateDecision(ctx, r)
		if err != nil {
			return 0, 0, 0, err
		}
		return d.Input.FirstIndex, d.NumTruncatableIndexes(), d.NewFirstIndex, nil
	}

	aFirst, aTruncatable, aOldest, err := getIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if aFirst == 0 {
		t.Errorf("expected first index to be greater than 0, got %d", aFirst)
	}

	// Write a few keys to the range.
	for i := 0; i < RaftLogQueueStaleThreshold+1; i++ {
		key := roachpb.Key(fmt.Sprintf("key%02d", i))
		args := putArgs(key, []byte(fmt.Sprintf("value%02d", i)))
		if _, err := kv.SendWrapped(ctx, store.TestSender(), &args); err != nil {
			t.Fatal(err)
		}
	}

	bFirst, bTruncatable, bOldest, err := getIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if aFirst != bFirst {
		t.Fatalf("expected firstIndex to not change, instead it changed from %d -> %d", aFirst, bFirst)
	}
	if aTruncatable >= bTruncatable {
		t.Fatalf("expected truncatableIndexes to increase, instead it changed from %d -> %d", aTruncatable, bTruncatable)
	}
	if aOldest >= bOldest {
		t.Fatalf("expected oldestIndex to increase, instead it changed from %d -> %d", aOldest, bOldest)
	}

	// Enable the raft log scanner and and force a truncation.
	store.SetRaftLogQueueActive(true)
	store.MustForceRaftLogScanAndProcess()
	store.SetRaftLogQueueActive(false)

	// There can be a delay from when the truncation command is issued and the
	// indexes updating.
	var cFirst, cOldest uint64
	var numTruncatable int
	testutils.SucceedsSoon(t, func() error {
		var err error
		cFirst, numTruncatable, cOldest, err = getIndexes()
		if err != nil {
			t.Fatal(err)
		}
		if bFirst == cFirst {
			return errors.Errorf("truncation did not occur, expected firstIndex to change, instead it remained at %d", cFirst)
		}
		return nil
	})
	if bTruncatable < numTruncatable {
		t.Errorf("expected numTruncatable to decrease, instead it changed from %d -> %d", bTruncatable, numTruncatable)
	}
	if bOldest >= cOldest {
		t.Errorf("expected oldestIndex to increase, instead it changed from %d -> %d", bOldest, cOldest)
	}

	verifyLogSizeInSync(t, r)

	// Again, enable the raft log scanner and and force a truncation. This time
	// we expect no truncation to occur.
	store.SetRaftLogQueueActive(true)
	store.MustForceRaftLogScanAndProcess()
	store.SetRaftLogQueueActive(false)

	// Unlike the last iteration, where we expect a truncation and can wait on
	// it with succeedsSoon, we can't do that here. This check is fragile in
	// that the truncation triggered here may lose the race against the call to
	// GetFirstIndex or newTruncateDecision, giving a false negative. Fixing
	// this requires additional instrumentation of the queues, which was deemed
	// to require too much work at the time of this writing.
	dFirst, dTruncatable, dOldest, err := getIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if cFirst != dFirst {
		t.Errorf("truncation should not have occurred, but firstIndex changed from %d -> %d", cFirst, dFirst)
	}
	if numTruncatable != dTruncatable {
		t.Errorf("truncation should not have occurred, but truncatableIndexes changed from %d -> %d", numTruncatable, dTruncatable)
	}
	if cOldest != dOldest {
		t.Errorf("truncation should not have occurred, but oldestIndex changed from %d -> %d", cOldest, dOldest)
	}
}

// TestProactiveRaftLogTruncate verifies that we proactively truncate the raft
// log even when replica scanning is disabled.
func TestProactiveRaftLogTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testCases := []struct {
		count     int
		valueSize int
	}{
		// Lots of small KVs.
		{RaftLogQueueStaleSize / 100, 5},
		// One big KV.
		{1, RaftLogQueueStaleSize},
	}
	for _, c := range testCases {
		testutils.RunTrueAndFalse(t, "loosely-coupled", func(t *testing.T, looselyCoupled bool) {
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			store, _ := createTestStore(ctx, t,
				testStoreOpts{
					// This test was written before test stores could start with more than one
					// range and was not adapted.
					createSystemRanges: false,
				},
				stopper)
			st := store.ClusterSettings()
			looselyCoupledTruncationEnabled.Override(ctx, &st.SV, looselyCoupled)
			// Note that turning off the replica scanner does not prevent the queues
			// from processing entries (in this case specifically the raftLogQueue),
			// just that the scanner will not try to push all replicas onto the queues.
			store.SetReplicaScannerActive(false)

			r, err := store.GetReplica(1)
			if err != nil {
				t.Fatal(err)
			}

			oldFirstIndex, err := r.GetFirstIndex()
			if err != nil {
				t.Fatal(err)
			}

			for i := 0; i < c.count; i++ {
				key := roachpb.Key(fmt.Sprintf("key%02d", i))
				args := putArgs(key, []byte(fmt.Sprintf("%s%02d", strings.Repeat("v", c.valueSize), i)))
				if _, err := kv.SendWrapped(ctx, store.TestSender(), &args); err != nil {
					t.Fatal(err)
				}
			}

			// Log truncation is an asynchronous process and while it will usually occur
			// fairly quickly, there is a slight race between this check and the
			// truncation, especially when under stress.
			testutils.SucceedsSoon(t, func() error {
				if looselyCoupled {
					// Flush the engine to advance durability, which triggers truncation.
					require.NoError(t, store.engine.Flush())
				}
				newFirstIndex, err := r.GetFirstIndex()
				if err != nil {
					t.Fatal(err)
				}
				if newFirstIndex <= oldFirstIndex {
					return errors.Errorf("log was not correctly truncated, old first index:%d, current first index:%d",
						oldFirstIndex, newFirstIndex)
				}
				return nil
			})
		})
	}
}

func TestSnapshotLogTruncationConstraints(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	r := &Replica{}
	var storeID roachpb.StoreID
	id1, id2 := uuid.MakeV4(), uuid.MakeV4()
	const (
		index1 = 50
		index2 = 60
	)

	// Add first constraint.
	r.addSnapshotLogTruncationConstraintLocked(ctx, id1, index1, storeID)
	exp1 := map[uuid.UUID]snapTruncationInfo{id1: {index: index1}}

	// Make sure it registered.
	assert.Equal(t, r.mu.snapshotLogTruncationConstraints, exp1)

	// Add another constraint with the same id. Extremely unlikely in practice
	// but we want to make sure it doesn't blow anything up. Collisions are
	// handled by ignoring the colliding update.
	r.addSnapshotLogTruncationConstraintLocked(ctx, id1, index2, storeID)
	assert.Equal(t, r.mu.snapshotLogTruncationConstraints, exp1)

	// Helper that grabs the min constraint index (which can trigger GC as a
	// byproduct) and asserts.
	assertMin := func(exp uint64, now time.Time) {
		t.Helper()
		const anyRecipientStore roachpb.StoreID = 0
		if maxIndex := r.getAndGCSnapshotLogTruncationConstraintsLocked(now, anyRecipientStore); maxIndex != exp {
			t.Fatalf("unexpected max index %d, wanted %d", maxIndex, exp)
		}
	}

	// Queue should be told index1 is the highest pending one. Note that the
	// colliding update at index2 is not represented.
	assertMin(index1, time.Time{})

	// Add another, higher, index. We're not going to notice it's around
	// until the lower one disappears.
	r.addSnapshotLogTruncationConstraintLocked(ctx, id2, index2, storeID)

	now := timeutil.Now()
	// The colliding snapshot comes back. Or the original, we can't tell.
	r.completeSnapshotLogTruncationConstraint(ctx, id1, now)
	// The index should show up when its deadline isn't hit.
	assertMin(index1, now)
	assertMin(index1, now.Add(RaftLogQueuePendingSnapshotGracePeriod))
	assertMin(index1, now.Add(RaftLogQueuePendingSnapshotGracePeriod))
	// Once we're over deadline, the index returned so far disappears.
	assertMin(index2, now.Add(RaftLogQueuePendingSnapshotGracePeriod+1))
	assertMin(index2, time.Time{})
	assertMin(index2, now.Add(10*RaftLogQueuePendingSnapshotGracePeriod))

	r.completeSnapshotLogTruncationConstraint(ctx, id2, now)
	assertMin(index2, now)
	assertMin(index2, now.Add(RaftLogQueuePendingSnapshotGracePeriod))
	assertMin(0, now.Add(2*RaftLogQueuePendingSnapshotGracePeriod))

	assert.Equal(t, r.mu.snapshotLogTruncationConstraints, map[uuid.UUID]snapTruncationInfo(nil))
}

// TestTruncateLog verifies that the TruncateLog command removes a
// prefix of the raft logs (modifying FirstIndex() and making them
// inaccessible via Entries()).
func TestTruncateLog(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testutils.RunTrueAndFalse(t, "loosely-coupled", func(t *testing.T, looselyCoupled bool) {
		tc := testContext{}
		ctx := context.Background()
		cfg := TestStoreConfig(nil)
		cfg.TestingKnobs.DisableRaftLogQueue = true
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		tc.StartWithStoreConfig(ctx, t, stopper, cfg)
		st := tc.store.ClusterSettings()
		looselyCoupledTruncationEnabled.Override(ctx, &st.SV, looselyCoupled)

		// Populate the log with 10 entries. Save the LastIndex after each write.
		var indexes []uint64
		for i := 0; i < 10; i++ {
			args := incrementArgs([]byte("a"), int64(i))

			if _, pErr := tc.SendWrapped(args); pErr != nil {
				t.Fatal(pErr)
			}
			idx, err := tc.repl.GetLastIndex()
			if err != nil {
				t.Fatal(err)
			}
			indexes = append(indexes, idx)
		}

		rangeID := tc.repl.RangeID

		// Discard the first half of the log.
		truncateArgs := truncateLogArgs(indexes[5], rangeID)
		if _, pErr := tc.SendWrappedWith(roachpb.Header{RangeID: 1}, &truncateArgs); pErr != nil {
			t.Fatal(pErr)
		}

		waitForTruncationForTesting(t, tc.repl, indexes[5], looselyCoupled)

		// We can still get what remains of the log.
		tc.repl.mu.Lock()
		entries, err := tc.repl.raftEntriesLocked(indexes[5], indexes[9], math.MaxUint64)
		tc.repl.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != int(indexes[9]-indexes[5]) {
			t.Errorf("expected %d entries, got %d", indexes[9]-indexes[5], len(entries))
		}

		// But any range that includes the truncated entries returns an error.
		tc.repl.mu.Lock()
		_, err = tc.repl.raftEntriesLocked(indexes[4], indexes[9], math.MaxUint64)
		tc.repl.mu.Unlock()
		if !errors.Is(err, raft.ErrCompacted) {
			t.Errorf("expected ErrCompacted, got %s", err)
		}

		// The term of the last truncated entry is still available.
		tc.repl.mu.Lock()
		term, err := tc.repl.raftTermRLocked(indexes[4])
		tc.repl.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if term == 0 {
			t.Errorf("invalid term 0 for truncated entry")
		}

		// The terms of older entries are gone.
		tc.repl.mu.Lock()
		_, err = tc.repl.raftTermRLocked(indexes[3])
		tc.repl.mu.Unlock()
		if !errors.Is(err, raft.ErrCompacted) {
			t.Errorf("expected ErrCompacted, got %s", err)
		}

		// Truncating logs that have already been truncated should not return an
		// error.
		truncateArgs = truncateLogArgs(indexes[3], rangeID)
		if _, pErr := tc.SendWrapped(&truncateArgs); pErr != nil {
			t.Fatal(pErr)
		}

		// Truncating logs that have the wrong rangeID included should not return
		// an error but should not truncate any logs.
		truncateArgs = truncateLogArgs(indexes[9], rangeID+1)
		if _, pErr := tc.SendWrapped(&truncateArgs); pErr != nil {
			t.Fatal(pErr)
		}

		tc.repl.mu.Lock()
		// The term of the last truncated entry is still available.
		term, err = tc.repl.raftTermRLocked(indexes[4])
		tc.repl.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if term == 0 {
			t.Errorf("invalid term 0 for truncated entry")
		}
	})
}

func TestRaftLogQueueShouldQueueRecompute(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var rlq *raftLogQueue

	_ = ctx
	_ = rlq

	// NB: Cases for which decision.ShouldTruncate() is true are tested in
	// TestComputeTruncateDecision, so here the decision itself is never
	// positive.
	var decision truncateDecision
	decision.Input.LogSizeTrusted = true
	decision.Input.LogSize = 12
	decision.Input.MaxLogSize = 1000

	verify := func(shouldQ bool, recompute bool, prio float64) {
		t.Helper()
		isQ, isR, isP := rlq.shouldQueueImpl(ctx, decision)
		assert.Equal(t, shouldQ, isQ)
		assert.Equal(t, recompute, isR)
		assert.Equal(t, prio, isP)
	}

	verify(false, false, 0)

	// Check all the boxes: unknown log size, leader, and non-empty log.
	decision.Input.LogSize = 123
	decision.Input.LogSizeTrusted = false
	decision.Input.FirstIndex = 10
	decision.Input.LastIndex = 20

	verify(true, true, 1+float64(decision.Input.MaxLogSize)/2)

	golden := decision

	// Check all boxes except that log is empty.
	decision = golden
	decision.Input.LastIndex = decision.Input.FirstIndex
	verify(false, false, 0)
}

// TestTruncateLogRecompute checks that if raftLogSize is not trusted, the raft
// log queue picks up the replica, recomputes the log size, and considers a
// truncation.
func TestTruncateLogRecompute(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc := testContext{}
	cfg := TestStoreConfig(nil)
	cfg.TestingKnobs.DisableRaftLogQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	tc.StartWithStoreConfig(ctx, t, stopper, cfg)

	key := roachpb.Key("a")
	repl := tc.store.LookupReplica(keys.MustAddr(key))

	trusted := func() bool {
		repl.mu.Lock()
		defer repl.mu.Unlock()
		return repl.mu.raftLogSizeTrusted
	}

	put := func() {
		var v roachpb.Value
		v.SetBytes(bytes.Repeat([]byte("x"), RaftLogQueueStaleSize*5))
		put := roachpb.NewPut(key, v)
		var ba roachpb.BatchRequest
		ba.Add(put)
		ba.RangeID = repl.RangeID

		if _, pErr := tc.store.Send(ctx, ba); pErr != nil {
			t.Fatal(pErr)
		}
	}

	put()

	decision, err := newTruncateDecision(ctx, repl)
	assert.NoError(t, err)
	assert.True(t, decision.ShouldTruncate())
	// Should never trust initially, until recomputed at least once.
	assert.False(t, trusted())

	repl.mu.Lock()
	repl.mu.raftLogSizeTrusted = false
	repl.mu.raftLogSize += 12          // garbage
	repl.mu.raftLogLastCheckSize += 12 // garbage
	repl.mu.Unlock()

	// Force a raft log queue run. The result should be a nonzero Raft log of
	// size below the threshold (though we won't check that since it could have
	// grown over threshold again; we compute instead that its size is correct).
	tc.store.SetRaftLogQueueActive(true)
	tc.store.MustForceRaftLogScanAndProcess()

	for i := 0; i < 2; i++ {
		verifyLogSizeInSync(t, repl)
		assert.True(t, trusted())
		put() // make sure we remain trusted and in sync
	}
}

func waitForTruncationForTesting(
	t *testing.T, r *Replica, newFirstIndex uint64, looselyCoupled bool,
) {
	testutils.SucceedsSoon(t, func() error {
		if looselyCoupled {
			// Flush the engine to advance durability, which triggers truncation.
			require.NoError(t, r.Engine().Flush())
		}
		// FirstIndex should have changed.
		firstIndex, err := r.GetFirstIndex()
		require.NoError(t, err)
		if firstIndex != newFirstIndex {
			return errors.Errorf("expected firstIndex == %d, got %d", newFirstIndex, firstIndex)
		}
		// Some low-level tests also look at the raftEntryCache or sideloaded
		// storage, which are updated after, and non-atomically with the change to
		// first index (latter holds Replica.mu). Since the raftLogTruncator holds Replica.raftMu
		// for the duration of its work, we can, by acquiring and releasing raftMu here, ensure
		// that we have waited for it to finish.
		r.raftMu.Lock()
		defer r.raftMu.Unlock()
		return nil
	})
}
