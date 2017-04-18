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
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/coreos/etcd/raft"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

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
		pendingSnapshot uint64
		expected        uint64
	}{
		{[]uint64{1, 2}, 100, 1, 0, 1},
		{[]uint64{1, 5, 5}, 100, 1, 0, 1},
		{[]uint64{1, 5, 5}, 100, 2, 0, 2},
		{[]uint64{5, 5, 5}, 100, 2, 0, 5},
		{[]uint64{5, 5, 5}, 100, 2, 1, 2},
		{[]uint64{5, 5, 5}, 100, 2, 3, 3},
		{[]uint64{1, 2, 3, 4}, 100, 1, 0, 1},
		{[]uint64{1, 2, 3, 4}, 100, 2, 0, 2},
		// If over targetSize, should truncate to quorum committed index.
		{[]uint64{1, 3, 3, 4}, 2000, 1, 0, 3},
		{[]uint64{1, 3, 3, 4}, 2000, 2, 0, 3},
		{[]uint64{1, 3, 3, 4}, 2000, 3, 0, 3},
		// The pending snapshot index affects the quorum commit index.
		{[]uint64{4}, 2000, 1, 1, 1},
		// Never truncate past the quorum commit index.
		{[]uint64{3, 3, 6}, 100, 4, 0, 3},
	}
	for i, c := range testCases {
		status := &raft.Status{
			Progress: make(map[uint64]raft.Progress),
		}
		for j, v := range c.progress {
			status.Progress[uint64(j)] = raft.Progress{Match: v}
		}
		out := computeTruncatableIndex(status, c.raftLogSize, targetSize, c.firstIndex, c.pendingSnapshot)
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
		firstIndex, err := r.FirstIndex()
		r.mu.Unlock()
		if err != nil {
			return 0, 0, 0, err
		}
		truncatableIndexes, oldestIndex, err := getTruncatableIndexes(context.Background(), r)
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
	t.Skip("#9772")

	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store, _ := createTestStore(t, stopper)

	store.SetReplicaScannerActive(false)

	r, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	r.mu.Lock()
	oldFirstIndex, err := r.FirstIndex()
	r.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	// Write a few keys to the range. While writing these keys, the raft log
	// should be proactively truncated even though replica scanning is disabled.
	for i := 0; i < 2*RaftLogQueueStaleThreshold; i++ {
		key := roachpb.Key(fmt.Sprintf("key%02d", i))
		args := putArgs(key, []byte(fmt.Sprintf("value%02d", i)))
		if _, err := client.SendWrapped(context.Background(), store.testSender(), &args); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for any asynchronous tasks to finish.
	stopper.Quiesce(context.TODO())

	r.mu.Lock()
	newFirstIndex, err := r.FirstIndex()
	r.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	if newFirstIndex <= oldFirstIndex {
		t.Errorf("log was not correctly truncated, old first index:%d, current first index:%d",
			oldFirstIndex, newFirstIndex)
	}
}
