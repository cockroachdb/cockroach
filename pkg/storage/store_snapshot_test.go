// Copyright 2018 The Cockroach Authors.
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

	store, _ := createTestStore(t, stopper)
	store.SetRaftLogQueueActive(false)
	repl, err := store.GetReplica(1)
	if err != nil {
		t.Fatal(err)
	}

	var bytesWritten int64
	blob := []byte(strings.Repeat("a", 1024*1024))
	for i := 0; bytesWritten < 5*raftLogMaxSize; i++ {
		pArgs := putArgs(roachpb.Key("a"), blob)
		_, pErr := client.SendWrappedWith(ctx, store, roachpb.Header{RangeID: 1}, &pArgs)
		if pErr != nil {
			t.Fatal(pErr)
		}
		bytesWritten += int64(len(blob))
	}

	for _, snapType := range []string{snapTypePreemptive, snapTypeRaft} {
		t.Run(snapType, func(t *testing.T) {
			lastIndex, err := (*replicaRaftStorage)(repl).LastIndex()
			if err != nil {
				t.Fatal(err)
			}
			eng := store.Engine()
			snap := eng.NewSnapshot()
			defer snap.Close()

			ss := kvBatchSnapshotStrategy{
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
			if snapType == snapTypePreemptive {
				if !testutils.IsError(err, "aborting snapshot because raft log is too large") {
					t.Fatalf("unexpected error: %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
