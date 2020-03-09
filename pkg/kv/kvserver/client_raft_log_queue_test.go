// Copyright 2015 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
)

// TestRaftLogQueue verifies that the raft log queue correctly truncates the
// raft log.
func TestRaftLogQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mtc := &multiTestContext{}

	// Set maxBytes to something small so we can trigger the raft log truncation
	// without adding 64MB of logs.
	const maxBytes = 1 << 16

	// Turn off raft elections so the raft leader won't change out from under
	// us in this test.
	sc := kvserver.TestStoreConfig(nil)
	sc.DefaultZoneConfig.RangeMaxBytes = proto.Int64(maxBytes)
	sc.RaftTickInterval = math.MaxInt32
	sc.RaftElectionTimeoutTicks = 1000000
	mtc.storeConfig = &sc

	defer mtc.Stop()
	mtc.Start(t, 3)

	// Write a single value to ensure we have a leader.
	pArgs := putArgs([]byte("key"), []byte("value"))
	if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), pArgs); err != nil {
		t.Fatal(err)
	}

	// Get the raft leader (and ensure one exists).
	rangeID := mtc.stores[0].LookupReplica([]byte("a")).RangeID
	raftLeaderRepl := mtc.getRaftLeader(rangeID)
	if raftLeaderRepl == nil {
		t.Fatalf("could not find raft leader replica for range %d", rangeID)
	}
	originalIndex, err := raftLeaderRepl.GetFirstIndex()
	if err != nil {
		t.Fatal(err)
	}

	// Disable splits since we're increasing the raft log with puts.
	for _, store := range mtc.stores {
		store.SetSplitQueueActive(false)
	}

	// Write a collection of values to increase the raft log.
	value := bytes.Repeat([]byte("a"), 1000) // 1KB
	for size := int64(0); size < 2*maxBytes; size += int64(len(value)) {
		pArgs = putArgs([]byte(fmt.Sprintf("key-%d", size)), value)
		if _, err := kv.SendWrapped(context.Background(), mtc.stores[0].TestSender(), pArgs); err != nil {
			t.Fatal(err)
		}
	}

	// Force a truncation check.
	for _, store := range mtc.stores {
		store.MustForceRaftLogScanAndProcess()
	}

	// Ensure that firstIndex has increased indicating that the log
	// truncation has occurred.
	afterTruncationIndex, err := raftLeaderRepl.GetFirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	if afterTruncationIndex <= originalIndex {
		t.Fatalf("raft log has not been truncated yet, afterTruncationIndex:%d originalIndex:%d",
			afterTruncationIndex, originalIndex)
	}

	// Force a truncation check again to ensure that attempting to truncate an
	// already truncated log has no effect. This check, unlike in the last
	// iteration, cannot use a succeedsSoon. This check is fragile in that the
	// truncation triggered here may lose the race against the call to
	// GetFirstIndex, giving a false negative. Fixing this requires additional
	// instrumentation of the queues, which was deemed to require too much work
	// at the time of this writing.
	for _, store := range mtc.stores {
		store.MustForceRaftLogScanAndProcess()
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
