// Copyright 2016 The Cockroach Authors.
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

package storage

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/raft"
)

func TestShouldTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

func TestGetQuorumIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		progress []uint64
		expected uint64
	}{
		// Basic cases.
		{[]uint64{1}, 1},
		{[]uint64{2}, 2},
		{[]uint64{1, 2}, 1},
		{[]uint64{2, 3}, 2},
		{[]uint64{1, 2, 3}, 2},
		{[]uint64{2, 3, 4}, 3},
		{[]uint64{1, 2, 3, 4}, 2},
		{[]uint64{2, 3, 4, 5}, 3},
		{[]uint64{1, 2, 3, 4, 5}, 3},
		{[]uint64{2, 3, 4, 5, 6}, 4},
		// Sorting.
		{[]uint64{5, 4, 3, 2, 1}, 3},
	}
	for i, c := range testCases {
		status := &raft.Status{
			Progress: make(map[uint64]raft.Progress),
		}
		for j, v := range c.progress {
			status.Progress[uint64(j)] = raft.Progress{Match: v}
		}
		quorumMatchedIndex := getQuorumIndex(status)
		if c.expected != quorumMatchedIndex {
			t.Fatalf("%d: expected %d, but got %d", i, c.expected, quorumMatchedIndex)
		}
	}
}

func TestComputeTruncateDecision(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const targetSize = 1000

	testCases := []struct {
		progress        []uint64
		raftLogSize     int64
		firstIndex      uint64
		lastIndex       uint64
		pendingSnapshot uint64
		exp             string
	}{
		{
			// Nothing to truncate.
			[]uint64{1, 2}, 100, 1, 1, 0,
			"truncate 0 entries to first index 1 (chosen via: quorum)"},
		{
			// Nothing to truncate on this replica, though a quorum elsewhere has more progress.
			// NB this couldn't happen if we're truly the Raft leader, unless we appended to our
			// own log asynchronously.
			[]uint64{1, 5, 5}, 100, 1, 1, 0,
			"truncate 0 entries to first index 1 (chosen via: followers)",
		},
		{
			// We're not truncating anything, but one follower is already cut off. There's no pending
			// snapshot so we shouldn't be causing any additional snapshots.
			[]uint64{1, 5, 5}, 100, 2, 2, 0,
			"truncate 0 entries to first index 2 (chosen via: first index)",
		},
		{
			// The happy case.
			[]uint64{5, 5, 5}, 100, 2, 5, 0,
			"truncate 3 entries to first index 5 (chosen via: quorum)",
		},
		{
			// No truncation, but the outstanding snapshot is made obsolete by the truncation. However
			// it was already obsolete before. (This example is also not one you could manufacture in
			// a real system).
			[]uint64{5, 5, 5}, 100, 2, 2, 1,
			"truncate 0 entries to first index 2 (chosen via: first index)",
		},
		{
			// Respecting the pending snapshot.
			[]uint64{5, 5, 5}, 100, 2, 5, 3,
			"truncate 1 entries to first index 3 (chosen via: pending snapshot)",
		},
		{
			// Log is below target size, so respecting the slowest follower.
			[]uint64{1, 2, 3, 4}, 100, 1, 5, 0,
			"truncate 0 entries to first index 1 (chosen via: followers)",
		},
		{
			// Truncating since local log starts at 2. One follower is already cut off without a pending
			// snapshot.
			[]uint64{1, 2, 3, 4}, 100, 2, 2, 0,
			"truncate 0 entries to first index 2 (chosen via: first index)",
		},
		// If over targetSize, should truncate to quorum committed index. Minority will need snapshots.
		{
			[]uint64{1, 3, 3, 4}, 2000, 1, 3, 0,
			"truncate 2 entries to first index 3 (chosen via: quorum); log too large (2.0 KiB > 1000 B); implies 1 Raft snapshot",
		},
		// Don't truncate away pending snapshot, even when log too large.
		{
			[]uint64{100, 100}, 2000, 1, 100, 50,
			"truncate 49 entries to first index 50 (chosen via: pending snapshot); log too large (2.0 KiB > 1000 B)",
		},
		{
			[]uint64{1, 3, 3, 4}, 2000, 2, 3, 0,
			"truncate 1 entries to first index 3 (chosen via: quorum); log too large (2.0 KiB > 1000 B)",
		},
		{
			[]uint64{1, 3, 3, 4}, 2000, 3, 3, 0,
			"truncate 0 entries to first index 3 (chosen via: quorum); log too large (2.0 KiB > 1000 B)",
		},
		// The pending snapshot index affects the quorum commit index.
		{
			[]uint64{4}, 2000, 1, 7, 1,
			"truncate 0 entries to first index 1 (chosen via: pending snapshot); log too large (2.0 KiB > 1000 B)",
		},
		// Never truncate past the quorum commit index.
		{
			[]uint64{3, 3, 6}, 100, 2, 7, 0,
			"truncate 1 entries to first index 3 (chosen via: quorum)",
		},
		// Never truncate past the last index.
		{
			[]uint64{5}, 100, 1, 3, 0,
			"truncate 2 entries to first index 3 (chosen via: last index)",
		},
		// Never truncate "before the first index".
		{
			[]uint64{5}, 100, 2, 3, 1,
			"truncate 0 entries to first index 2 (chosen via: first index)",
		}}
	for i, c := range testCases {
		status := &raft.Status{
			Progress: make(map[uint64]raft.Progress),
		}
		for j, v := range c.progress {
			status.Progress[uint64(j)] = raft.Progress{Match: v}
		}
		decision := computeTruncateDecision(truncateDecisionInput{
			RaftStatus:                     status,
			LogSize:                        c.raftLogSize,
			MaxLogSize:                     targetSize,
			FirstIndex:                     c.firstIndex,
			LastIndex:                      c.lastIndex,
			PendingPreemptiveSnapshotIndex: c.pendingSnapshot,
		})
		if act, exp := decision.String(), c.exp; act != exp {
			t.Errorf("%d: got:\n%s\nwanted:\n%s", i, act, exp)
		}
	}
}

func verifyLogSizeInSync(t *testing.T, r *Replica) {
	r.raftMu.Lock()
	defer r.raftMu.Unlock()
	r.mu.Lock()
	raftLogSize := r.mu.raftLogSize
	r.mu.Unlock()
	start := engine.MakeMVCCMetadataKey(keys.RaftLogKey(r.RangeID, 1))
	end := engine.MakeMVCCMetadataKey(keys.RaftLogKey(r.RangeID, math.MaxUint64))

	var ms enginepb.MVCCStats
	iter := r.store.engine.NewIterator(engine.IterOptions{UpperBound: end.Key})
	defer iter.Close()
	ms, err := iter.ComputeStats(start, end, 0 /* nowNanos */)
	if err != nil {
		t.Fatal(err)
	}
	actualRaftLogSize := ms.SysBytes
	if actualRaftLogSize != raftLogSize {
		t.Fatalf("replica claims raft log size %d, but computed %d", raftLogSize, actualRaftLogSize)
	}
}

// TestNewTruncateDecision verifies that old raft log entries are correctly
// removed.
func TestNewTruncateDecision(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store, _ := createTestStore(t, stopper)
	store.SetRaftLogQueueActive(false)

	r, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	getIndexes := func() (uint64, int, uint64, error) {
		d, err := newTruncateDecision(context.Background(), r)
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
		if _, err := client.SendWrapped(context.Background(), store.TestSender(), &args); err != nil {
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
	store.ForceRaftLogScanAndProcess()
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
	store.ForceRaftLogScanAndProcess()
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
		t.Run("", func(t *testing.T) {
			stopper := stop.NewStopper()
			defer stopper.Stop(ctx)
			store, _ := createTestStore(t, stopper)

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
				if _, err := client.SendWrapped(ctx, store.TestSender(), &args); err != nil {
					t.Fatal(err)
				}
			}

			// Log truncation is an asynchronous process and while it will usually occur
			// fairly quickly, there is a slight race between this check and the
			// truncation, especially when under stress.
			testutils.SucceedsSoon(t, func() error {
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

	ctx := context.Background()
	r := &Replica{}
	id1, id2 := uuid.MakeV4(), uuid.MakeV4()
	const (
		index1 = 50
		index2 = 60
	)

	// Add first constraint.
	r.addSnapshotLogTruncationConstraintLocked(ctx, id1, index1)
	exp1 := map[uuid.UUID]snapTruncationInfo{id1: {index: index1}}

	// Make sure it registered.
	assert.Equal(t, r.mu.snapshotLogTruncationConstraints, exp1)

	// Add another constraint with the same id. Extremely unlikely in practice
	// but we want to make sure it doesn't blow anything up. Collisions are
	// handled by ignoring the colliding update.
	r.addSnapshotLogTruncationConstraintLocked(ctx, id1, index2)
	assert.Equal(t, r.mu.snapshotLogTruncationConstraints, exp1)

	// Helper that grabs the min constraint index (which can trigger GC as a
	// byproduct) and asserts.
	assertMin := func(exp uint64, now time.Time) {
		t.Helper()
		if maxIndex := r.getAndGCSnapshotLogTruncationConstraintsLocked(now); maxIndex != exp {
			t.Fatalf("unexpected max index %d, wanted %d", maxIndex, exp)
		}
	}

	// Queue should be told index1 is the highest pending one. Note that the
	// colliding update at index2 is not represented.
	assertMin(index1, time.Time{})

	// Add another, higher, index. We're not going to notice it's around
	// until the lower one disappears.
	r.addSnapshotLogTruncationConstraintLocked(ctx, id2, index2)

	now := timeutil.Now()
	// The colliding snapshot comes back. Or the original, we can't tell.
	r.completeSnapshotLogTruncationConstraint(ctx, id1, now)
	// The index should show up when its deadline isn't hit.
	assertMin(index1, now)
	assertMin(index1, now.Add(raftLogQueuePendingSnapshotGracePeriod))
	assertMin(index1, now.Add(raftLogQueuePendingSnapshotGracePeriod))
	// Once we're over deadline, the index returned so far disappears.
	assertMin(index2, now.Add(raftLogQueuePendingSnapshotGracePeriod+1))
	assertMin(index2, time.Time{})
	assertMin(index2, now.Add(10*raftLogQueuePendingSnapshotGracePeriod))

	r.completeSnapshotLogTruncationConstraint(ctx, id2, now)
	assertMin(index2, now)
	assertMin(index2, now.Add(raftLogQueuePendingSnapshotGracePeriod))
	assertMin(0, now.Add(2*raftLogQueuePendingSnapshotGracePeriod))

	assert.Equal(t, r.mu.snapshotLogTruncationConstraints, map[uuid.UUID]snapTruncationInfo(nil))
}
