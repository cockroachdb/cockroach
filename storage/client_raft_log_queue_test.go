// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Bram Gruneir (bram+code@cockroachlabs.com)

package storage_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestReplicaGCQueueDropReplicaOnScan verifies that the range GC queue
// removes a range from a store that no longer should have a replica.
func TestRaftLogQueue(t *testing.T) {
	defer leaktest.AfterTest(t)

	mtc := startMultiTestContext(t, 3)
	defer mtc.Stop()

	// Write a single values to ensure we have a leader.
	pArgs := putArgs([]byte("key"), []byte("value"))
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &pArgs); err != nil {
		t.Fatal(err)
	}

	rep := mtc.stores[0].LookupReplica([]byte("a"), nil)
	firstIndex, err := rep.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}

	// Write a collection of values to increase the raft log.
	for i := 0; i < storage.RaftLogQueueLogSizeThreshold+1; i++ {
		pArgs = putArgs([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
		if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &pArgs); err != nil {
			t.Fatal(err)
		}
	}

	// Check the raft log and make sure it has advanced.
	desc := rep.Desc()
	raftStatus := mtc.stores[0].RaftStatus(desc.RangeID)
	if raftStatus == nil {
		t.Fatalf("can't get raft status for range %s", desc)
	}

	for _, progress := range raftStatus.Progress {
		if progress.Match <= firstIndex {
			t.Fatalf("raft log has not advanced, match:%d firstIndex:%d", progress.Match, firstIndex)
		}
	}

	// Force a truncation check.
	for _, store := range mtc.stores {
		store.ForceRaftLogScan(t)
	}

	// Wait until the firstIndex has increased indicating that the log
	// truncation has occurred.
	var currentIndex uint64
	util.SucceedsWithin(t, time.Second, func() error {
		var err error
		currentIndex, err = rep.FirstIndex()
		if err != nil {
			return err
		}
		if currentIndex <= firstIndex {
			return util.Errorf("raft log not truncated yet")
		}
		return nil
	})

	// Force a truncation check again to ensure that attempting to truncate an
	// already truncated log has no effect.
	for _, store := range mtc.stores {
		store.ForceRaftLogScan(t)
	}

	finalIndex, err := rep.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if currentIndex != finalIndex {
		t.Errorf("raft log was truncated again and it shouldn't have been")
	}
}
