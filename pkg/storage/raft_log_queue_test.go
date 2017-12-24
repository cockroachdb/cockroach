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
	"reflect"
	"strings"
	"testing"

	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
			v := shouldTruncate(c.truncatableIndexes, c.raftLogSize)
			if c.expected != v {
				t.Fatalf("expected %v, but found %v", c.expected, v)
			}
		})
	}
}

func TestGetQuorumIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		progress             []uint64
		pendingSnapshotIndex uint64
		expected             uint64
	}{
		// Basic cases.
		{[]uint64{1}, 0, 1},
		{[]uint64{2}, 1, 1},
		{[]uint64{1, 2}, 0, 1},
		{[]uint64{2, 3}, 1, 2},
		{[]uint64{1, 2, 3}, 0, 2},
		{[]uint64{2, 3, 4}, 1, 2},
		{[]uint64{1, 2, 3, 4}, 0, 2},
		{[]uint64{2, 3, 4, 5}, 1, 3},
		{[]uint64{1, 2, 3, 4, 5}, 0, 3},
		{[]uint64{2, 3, 4, 5, 6}, 1, 3},
		// Sorting.
		{[]uint64{5, 4, 3, 2, 1}, 0, 3},
	}
	for i, c := range testCases {
		status := &raft.Status{
			Progress: make(map[uint64]raft.Progress),
		}
		for j, v := range c.progress {
			status.Progress[uint64(j)] = raft.Progress{Match: v}
		}
		quorumMatchedIndex := getQuorumIndex(status, c.pendingSnapshotIndex)
		if c.expected != quorumMatchedIndex {
			t.Fatalf("%d: expected %d, but got %d", i, c.expected, quorumMatchedIndex)
		}
	}
}

func TestComputeTruncatableIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const targetSize = 1000

	testCases := []struct {
		progress        []uint64
		raftLogSize     int64
		firstIndex      uint64
		lastIndex       uint64
		pendingSnapshot uint64
		expected        uint64
	}{
		{[]uint64{1, 2}, 100, 1, 1, 0, 1},
		{[]uint64{1, 5, 5}, 100, 1, 1, 0, 1},
		{[]uint64{1, 5, 5}, 100, 2, 2, 0, 2},
		{[]uint64{5, 5, 5}, 100, 2, 5, 0, 5},
		{[]uint64{5, 5, 5}, 100, 2, 2, 1, 2},
		{[]uint64{5, 5, 5}, 100, 2, 3, 3, 3},
		{[]uint64{1, 2, 3, 4}, 100, 1, 1, 0, 1},
		{[]uint64{1, 2, 3, 4}, 100, 2, 2, 0, 2},
		// If over targetSize, should truncate to quorum committed index.
		{[]uint64{1, 3, 3, 4}, 2000, 1, 3, 0, 3},
		{[]uint64{1, 3, 3, 4}, 2000, 2, 3, 0, 3},
		{[]uint64{1, 3, 3, 4}, 2000, 3, 3, 0, 3},
		// The pending snapshot index affects the quorum commit index.
		{[]uint64{4}, 2000, 1, 1, 1, 1},
		// Never truncate past the quorum commit index.
		{[]uint64{3, 3, 6}, 100, 4, 4, 0, 3},
		// Never truncate past the last index.
		{[]uint64{5}, 100, 1, 3, 0, 3},
	}
	for i, c := range testCases {
		status := &raft.Status{
			Progress: make(map[uint64]raft.Progress),
		}
		for j, v := range c.progress {
			status.Progress[uint64(j)] = raft.Progress{Match: v}
		}
		out := computeTruncatableIndex(status, c.raftLogSize, targetSize,
			c.firstIndex, c.lastIndex, c.pendingSnapshot)
		if !reflect.DeepEqual(c.expected, out) {
			t.Errorf("%d: computeTruncatableIndex(...) expected %d, but got %d", i, c.expected, out)
		}
	}
}

// TestGetTruncatableIndexes verifies that old raft log entries are correctly
// removed.
func TestGetTruncatableIndexes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store, _ := createTestStore(t, stopper)
	store.SetRaftLogQueueActive(false)

	r, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	getIndexes := func() (uint64, uint64, uint64, error) {
		r.mu.Lock()
		firstIndex, err := r.raftFirstIndexLocked()
		r.mu.Unlock()
		if err != nil {
			return 0, 0, 0, err
		}
		truncatableIndexes, oldestIndex, _, err := getTruncatableIndexes(context.Background(), r)
		if err != nil {
			return 0, 0, 0, err
		}
		return firstIndex, truncatableIndexes, oldestIndex, nil
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
		if _, err := client.SendWrapped(context.Background(), store.testSender(), &args); err != nil {
			t.Fatal(err)
		}
	}

	bFirst, bTruncatable, bOldest, err := getIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if aFirst != bFirst {
		t.Errorf("expected firstIndex to not change, instead it changed from %d -> %d", aFirst, bFirst)
	}
	if aTruncatable >= bTruncatable {
		t.Errorf("expected truncatableIndexes to increase, instead it changed from %d -> %d", aTruncatable, bTruncatable)
	}
	if aOldest >= bOldest {
		t.Errorf("expected oldestIndex to increase, instead it changed from %d -> %d", aOldest, bOldest)
	}

	// Enable the raft log scanner and and force a truncation.
	store.SetRaftLogQueueActive(true)
	store.ForceRaftLogScanAndProcess()
	store.SetRaftLogQueueActive(false)

	// There can be a delay from when the truncation command is issued and the
	// indexes updating.
	var cFirst, cTruncatable, cOldest uint64
	testutils.SucceedsSoon(t, func() error {
		var err error
		cFirst, cTruncatable, cOldest, err = getIndexes()
		if err != nil {
			t.Fatal(err)
		}
		if bFirst == cFirst {
			return errors.Errorf("truncation did not occur, expected firstIndex to change, instead it remained at %d", cFirst)
		}
		return nil
	})
	if bTruncatable < cTruncatable {
		t.Errorf("expected truncatableIndexes to decrease, instead it changed from %d -> %d", bTruncatable, cTruncatable)
	}
	if bOldest >= cOldest {
		t.Errorf("expected oldestIndex to increase, instead it changed from %d -> %d", bOldest, cOldest)
	}

	// Again, enable the raft log scanner and and force a truncation. This time
	// we expect no truncation to occur.
	store.SetRaftLogQueueActive(true)
	store.ForceRaftLogScanAndProcess()
	store.SetRaftLogQueueActive(false)

	// Unlike the last iteration, where we expect a truncation and can wait on
	// it with succeedsSoon, we can't do that here. This check is fragile in
	// that the truncation triggered here may lose the race against the call to
	// GetFirstIndex or getTruncatableIndexes, giving a false negative. Fixing
	// this requires additional instrumentation of the queues, which was deemed
	// to require too much work at the time of this writing.
	dFirst, dTruncatable, dOldest, err := getIndexes()
	if err != nil {
		t.Fatal(err)
	}
	if cFirst != dFirst {
		t.Errorf("truncation should not have occurred, but firstIndex changed from %d -> %d", cFirst, dFirst)
	}
	if cTruncatable != dTruncatable {
		t.Errorf("truncation should not have occurred, but truncatableIndexes changed from %d -> %d", cTruncatable, dTruncatable)
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
				if _, err := client.SendWrapped(ctx, store.testSender(), &args); err != nil {
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
