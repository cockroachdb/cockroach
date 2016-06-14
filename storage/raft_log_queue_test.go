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
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

func TestGetQuorumMatchedIndex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		commit   uint64
		progress []uint64
		padding  uint64
		expected uint64
	}{
		// Basic cases.
		{1, []uint64{1}, 0, 1},
		{1, []uint64{1, 2}, 0, 1},
		{2, []uint64{1, 2, 3}, 0, 2},
		{2, []uint64{1, 2, 3, 4}, 0, 2},
		{3, []uint64{1, 2, 3, 4, 5}, 0, 3},
		// Sorting.
		{3, []uint64{5, 4, 3, 2, 1}, 0, 3},
		// Padding.
		{3, []uint64{1, 3, 3}, 1, 2},
		{3, []uint64{1, 3, 3}, 2, 1},
		{3, []uint64{1, 3, 3}, 3, 1},
		// Minimum progress value limits padding.
		{3, []uint64{2, 3, 3}, 3, 2},
	}
	for i, c := range testCases {
		status := &raft.Status{
			HardState: raftpb.HardState{
				Commit: c.commit,
			},
			Progress: make(map[uint64]raft.Progress),
		}
		for j, v := range c.progress {
			status.Progress[uint64(j)] = raft.Progress{Match: v}
		}
		quorumMatchedIndex := getQuorumMatchedIndex(status, c.padding)
		if c.expected != quorumMatchedIndex {
			t.Fatalf("%d: expected %d, but got %d", i, c.expected, quorumMatchedIndex)
		}
	}
}

// TestGetTruncatableIndexes verifies that the correctly returns when there are
// indexes to be truncated.
func TestGetTruncatableIndexes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("TODO(bram): #7056")
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()
	if _, err := store.GetReplica(0); err == nil {
		t.Fatal("expected GetRange to fail on missing range")
	}

	store.DisableRaftLogQueue(true)

	// Test on a new range which should not have a raft group yet.
	rngNew := createRange(store, 100, roachpb.RKey("a"), roachpb.RKey("c"))
	truncatableIndexes, oldestIndex, err := getTruncatableIndexes(rngNew)
	if err != nil {
		t.Errorf("expected no error, got %s", err)
	}
	if truncatableIndexes != 0 {
		t.Errorf("expected 0 for truncatable index, got %d", truncatableIndexes)
	}
	if oldestIndex != 0 {
		t.Errorf("expected 0 for oldest index, got %d", oldestIndex)
	}

	r, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	r.mu.Lock()
	firstIndex, err := r.FirstIndex()
	r.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	// Write a few keys to the range.
	for i := 0; i < RaftLogQueueStaleThreshold+1; i++ {
		key := roachpb.Key(fmt.Sprintf("key%02d", i))
		args := putArgs(key, []byte(fmt.Sprintf("value%02d", i)))
		if _, err := client.SendWrapped(store.testSender(), nil, &args); err != nil {
			t.Fatal(err)
		}
	}

	truncatableIndexes, oldestIndex, err = getTruncatableIndexes(r)
	if err != nil {
		t.Errorf("expected no error, got %s", err)
	}
	if truncatableIndexes == 0 {
		t.Errorf("expected a value for truncatable index, got 0")
	}
	if oldestIndex < firstIndex {
		t.Errorf("expected oldest index (%d) to be greater than or equal to first index (%d)", oldestIndex,
			firstIndex)
	}

	// Enable the raft log scanner and and force a truncation.
	store.DisableRaftLogQueue(false)
	store.ForceRaftLogScanAndProcess()
	// Wait for tasks to finish, in case the processLoop grabbed the event
	// before ForceRaftLogScanAndProcess but is still working on it.
	stopper.Quiesce()

	r.mu.Lock()
	newFirstIndex, err := r.FirstIndex()
	r.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}

	if newFirstIndex <= firstIndex {
		t.Errorf("log was not correctly truncated, older first index:%d, current first index:%d", firstIndex,
			newFirstIndex)
	}

	// Once truncated, we should have no truncatable indexes. If this turns out
	// to be flaky, we can remove it as the same functionality is tested in
	// client_raft_log_queue_test.
	util.SucceedsSoon(t, func() error {
		store.ForceRaftLogScanAndProcess()
		truncatableIndexes, oldestIndex, err := getTruncatableIndexes(rngNew)
		if err != nil {
			return util.Errorf("expected no error, got %s", err)
		}
		if truncatableIndexes != 0 {
			return util.Errorf("expected 0 for truncatable index, got %d", truncatableIndexes)
		}
		if oldestIndex != 0 {
			return util.Errorf("expected 0 for oldest index, got %d", oldestIndex)
		}
		return nil
	})
}

// TestProactiveRaftLogTruncate verifies that we proactively truncate the raft
// log even when replica scanning is disabled.
func TestProactiveRaftLogTruncate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	defer stopper.Stop()

	store.scanner.SetDisabled(true)

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
		if _, err := client.SendWrapped(store.testSender(), nil, &args); err != nil {
			t.Fatal(err)
		}
	}

	// Wait for tasks to finish, in case the processLoop grabbed the event
	// before ForceRaftLogScanAndProcess but is still working on it.
	stopper.Quiesce()

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
