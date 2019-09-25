// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"go.etcd.io/etcd/raft/raftpb"
	"golang.org/x/time/rate"
)

func TestSnapshotRaftLogLimit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	store, _ := createTestStore(t,
		testStoreOpts{
			// This test was written before test stores could start with more than one
			// range and was not adapted.
			createSystemRanges: false,
		},
		stopper)
	store.SetRaftLogQueueActive(false)
	repl, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	var bytesWritten int64
	blob := []byte(strings.Repeat("a", 1024*1024))
	for i := 0; bytesWritten < 5*store.cfg.RaftLogTruncationThreshold; i++ {
		pArgs := putArgs(roachpb.Key("a"), blob)
		_, pErr := client.SendWrappedWith(ctx, store, roachpb.Header{RangeID: 1}, &pArgs)
		if pErr != nil {
			t.Fatal(pErr)
		}
		bytesWritten += int64(len(blob))
	}

	for _, snapType := range []SnapshotRequest_Type{SnapshotRequest_PREEMPTIVE, SnapshotRequest_RAFT} {
		t.Run(snapType.String(), func(t *testing.T) {
			lastIndex, err := repl.GetLastIndex()
			if err != nil {
				t.Fatal(err)
			}
			eng := store.Engine()
			snap := eng.NewSnapshot()
			defer snap.Close()

			ss := kvBatchSnapshotStrategy{
				raftCfg:  &store.cfg.RaftConfig,
				limiter:  rate.NewLimiter(1<<10, 1),
				newBatch: eng.NewBatch,
			}
			iter := rditer.NewReplicaDataIterator(repl.Desc(), snap, true /* replicatedOnly */)
			defer iter.Close()
			outSnap := &OutgoingSnapshot{
				Iter:       iter,
				EngineSnap: snap,
				snapType:   snapType,
				RaftSnap: raftpb.Snapshot{
					Metadata: raftpb.SnapshotMetadata{
						Index: lastIndex,
					},
				},
			}

			var stream fakeSnapshotStream
			header := SnapshotRequest_Header{
				State: repl.State().ReplicaState,
			}

			err = ss.Send(ctx, stream, header, outSnap)
			if snapType == SnapshotRequest_PREEMPTIVE {
				if !testutils.IsError(err, "aborting snapshot because raft log is too large") {
					t.Fatalf("unexpected error: %+v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// TestSnapshotPreemptiveOnUninitializedReplica is a targeted regression test
// against a bug that once accepted these snapshots without forcing them to
// check for overlapping ranges.
func TestSnapshotPreemptiveOnUninitializedReplica(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	store, _ := createTestStore(t, testStoreOpts{}, stopper)

	// Create an uninitialized replica.
	repl, created, err := store.getOrCreateReplica(ctx, 77, 1, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("no replica created")
	}

	// Make a descriptor that overlaps r1 (any descriptor does because r1 covers
	// all of the keyspace).
	desc := *repl.Desc()
	desc.StartKey = roachpb.RKey("a")
	desc.EndKey = roachpb.RKey("b")

	header := &SnapshotRequest_Header{}
	header.State.Desc = &desc

	if !header.IsPreemptive() {
		t.Fatal("mock snapshot isn't preemptive")
	}

	if _, err := store.canApplyPreemptiveSnapshot(
		ctx, header, true, /* authoritative */
	); !testutils.IsError(err, "intersects existing range") {
		t.Fatal(err)
	}
}
