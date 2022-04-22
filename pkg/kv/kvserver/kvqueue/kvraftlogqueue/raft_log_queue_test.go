// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvraftlogqueue

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvqueue"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
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
		{kvqueue.RaftLogQueueStaleThreshold - 1, 0, false},
		{kvqueue.RaftLogQueueStaleThreshold, 0, true},
		{0, kvqueue.RaftLogQueueStaleSize, false},
		{1, kvqueue.RaftLogQueueStaleSize - 1, false},
		{1, kvqueue.RaftLogQueueStaleSize, true},
	}
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var d kvqueue.TruncateDecision
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
			input := kvqueue.TruncateDecisionInput{
				RaftStatus:           status,
				LogSize:              c.raftLogSize,
				MaxLogSize:           targetSize,
				LogSizeTrusted:       true,
				FirstIndex:           c.firstIndex,
				LastIndex:            c.lastIndex,
				PendingSnapshotIndex: c.pendingSnapshot,
			}
			decision := kvqueue.ComputeTruncateDecision(input)
			if act, exp := decision.String(), c.exp; act != exp {
				t.Errorf("%d:\ngot:\n%s\nwanted:\n%s", i, act, exp)
			}

			// Verify the triggers that queue a range for recomputation. In
			// essence, when the raft log size is not trusted we want to suggest
			// a truncation and also a recomputation. If the size *is* trusted,
			// we'll just see the decision play out as before.
			// The real tests for this are in TestRaftLogQueueShouldQueue, but this is
			// some nice extra coverage.
			should, recompute, prio := (*RaftLogQueue)(nil).ShouldQueueImpl(ctx, decision)
			assert.Equal(t, decision.ShouldTruncate(), should)
			assert.False(t, recompute)
			assert.Equal(t, decision.ShouldTruncate(), prio != 0)
			input.LogSizeTrusted = false
			input.RaftStatus.RaftState = raft.StateLeader
			if input.LastIndex <= input.FirstIndex {
				input.LastIndex = input.FirstIndex + 1
			}
			decision = kvqueue.ComputeTruncateDecision(input)
			should, recompute, prio = (*RaftLogQueue)(nil).ShouldQueueImpl(ctx, decision)
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

			input := kvqueue.TruncateDecisionInput{
				RaftStatus:     status,
				MaxLogSize:     1024,
				FirstIndex:     10,
				LastIndex:      lastIndex,
				LogSizeTrusted: true,
			}
			if tooLarge {
				input.LogSize += 2 * input.MaxLogSize
			}

			decision := kvqueue.ComputeTruncateDecision(input)
			if s, exp := decision.String(), exp[tooLarge][active]; s != exp {
				t.Errorf("expected %q, got %q", exp, s)
			}
		})
	})
}

func TestTruncateDecisionZeroValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var decision kvqueue.TruncateDecision
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

	decision := kvqueue.TruncateDecision{Input: kvqueue.TruncateDecisionInput{RaftStatus: status}}
	assert.Equal(t, 0, decision.RaftSnapshotsForIndex(10))
	assert.Equal(t, 1, decision.RaftSnapshotsForIndex(11))
	assert.Equal(t, 3, decision.RaftSnapshotsForIndex(12))
	assert.Equal(t, 3, decision.RaftSnapshotsForIndex(13))
}

func TestRaftLogQueueShouldQueueRecompute(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var rlq *RaftLogQueue

	_ = ctx
	_ = rlq

	// NB: Cases for which decision.ShouldTruncate() is true are tested in
	// TestComputeTruncateDecision, so here the decision itself is never
	// positive.
	var decision kvqueue.TruncateDecision
	decision.Input.LogSizeTrusted = true
	decision.Input.LogSize = 12
	decision.Input.MaxLogSize = 1000

	verify := func(shouldQ bool, recompute bool, prio float64) {
		t.Helper()
		isQ, isR, isP := rlq.ShouldQueueImpl(ctx, decision)
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
