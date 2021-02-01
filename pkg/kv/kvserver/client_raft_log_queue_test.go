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
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

// TestRaftLogQueue verifies that the raft log queue correctly truncates the
// raft log.
func TestRaftLogQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set maxBytes to something small so we can trigger the raft log truncation
	// without adding 64MB of logs.
	const maxBytes = 1 << 16

	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.RangeMaxBytes = proto.Int64(maxBytes)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						DefaultZoneConfigOverride: &zoneConfig,
					},
				},
				RaftConfig: base.RaftConfig{
					// Turn off raft elections so the raft leader won't change out from under
					// us in this test.
					RaftTickInterval:         math.MaxInt32,
					RaftElectionTimeoutTicks: 1000000,
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	key := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
	// Write a single value to ensure we have a leader.
	pArgs := putArgs(key, []byte("value"))
	if _, err := kv.SendWrapped(ctx, store.TestSender(), pArgs); err != nil {
		t.Fatal(err)
	}

	// Get the raft leader (and ensure one exists).
	raftLeaderRepl := tc.GetRaftLeader(t, roachpb.RKey(key))
	require.NotNil(t, raftLeaderRepl)
	originalIndex, err := raftLeaderRepl.GetFirstIndex()
	if err != nil {
		t.Fatal(err)
	}

	// Write a collection of values to increase the raft log.
	value := bytes.Repeat(key, 1000) // 1KB
	for size := int64(0); size < 2*maxBytes; size += int64(len(value)) {
		key = key.Next()
		pArgs = putArgs(key, value)
		if _, err := kv.SendWrapped(ctx, store.TestSender(), pArgs); err != nil {
			t.Fatal(err)
		}
	}

	var afterTruncationIndex uint64
	testutils.SucceedsSoon(t, func() error {
		// Force a truncation check.
		for i := range tc.Servers {
			tc.GetFirstStoreFromServer(t, i).MustForceRaftLogScanAndProcess()
		}
		// Ensure that firstIndex has increased indicating that the log
		// truncation has occurred.
		afterTruncationIndex, err = raftLeaderRepl.GetFirstIndex()
		if err != nil {
			return err
		}
		if afterTruncationIndex <= originalIndex {
			return errors.Errorf("raft log has not been truncated yet, afterTruncationIndex:%d originalIndex:%d",
				afterTruncationIndex, originalIndex)
		}
		return nil
	})

	// Force a truncation check again to ensure that attempting to truncate an
	// already truncated log has no effect. This check, unlike in the last
	// iteration, cannot use a succeedsSoon. This check is fragile in that the
	// truncation triggered here may lose the race against the call to
	// GetFirstIndex, giving a false negative. Fixing this requires additional
	// instrumentation of the queues, which was deemed to require too much work
	// at the time of this writing.
	for i := range tc.Servers {
		tc.GetFirstStoreFromServer(t, i).MustForceRaftLogScanAndProcess()
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
