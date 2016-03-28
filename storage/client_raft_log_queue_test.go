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
// permissions and limitations under the License.
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

// TestRaftLogQueue verifies that the raft log queue correctly truncates the
// raft log.
func TestRaftLogQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var mtc multiTestContext

	// Turn off raft elections so the raft leader won't change out from under
	// us in this test.
	sc := storage.TestStoreContext()
	sc.RaftTickInterval = time.Hour * 24
	sc.RaftElectionTimeoutTicks = 1000000
	mtc.storeContext = &sc

	mtc.Start(t, 3)
	defer mtc.Stop()

	// Write a single value to ensure we have a leader.
	pArgs := putArgs([]byte("key"), []byte("value"))
	if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &pArgs); err != nil {
		t.Fatal(err)
	}

	// Get the raft leader (and ensure one exists).
	rangeID := mtc.stores[0].LookupReplica([]byte("a"), nil).RangeID
	raftLeaderRepl := mtc.getRaftLeader(rangeID)
	if raftLeaderRepl == nil {
		t.Fatalf("could not find raft leader replica for range %d", rangeID)
	}
	originalIndex, err := raftLeaderRepl.GetFirstIndex()
	if err != nil {
		t.Fatal(err)
	}

	// Write a collection of values to increase the raft log.
	for i := 0; i < storage.RaftLogQueueStaleThreshold+1; i++ {
		pArgs = putArgs([]byte(fmt.Sprintf("key-%d", i)), []byte("value"))
		if _, err := client.SendWrapped(rg1(mtc.stores[0]), nil, &pArgs); err != nil {
			t.Fatal(err)
		}
	}

	// Sadly, occasionally the queue has a race with the force processing so
	// this succeeds within will captures those rare cases.
	var afterTruncationIndex uint64
	util.SucceedsSoon(t, func() error {
		// Force a truncation check.
		for _, store := range mtc.stores {
			store.ForceRaftLogScanAndProcess()
		}

		// Ensure that firstIndex has increased indicating that the log
		// truncation has occurred.
		var err error
		afterTruncationIndex, err = raftLeaderRepl.GetFirstIndex()
		if err != nil {
			t.Fatal(err)
		}
		if afterTruncationIndex <= originalIndex {
			return util.Errorf("raft log has not been truncated yet, afterTruncationIndex:%d originalIndex:%d",
				afterTruncationIndex, originalIndex)
		}
		return nil
	})

	// Force a truncation check again to ensure that attempting to truncate an
	// already truncated log has no effect.
	for _, store := range mtc.stores {
		store.ForceRaftLogScanAndProcess()
	}

	after2ndTruncationIndex, err := raftLeaderRepl.GetFirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if afterTruncationIndex > after2ndTruncationIndex {
		t.Fatalf("second truncation destroyed state: afterTruncationIndex:%d after2ndTruncationIndex:%d",
			afterTruncationIndex, after2ndTruncationIndex)
	}
}
