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
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package storage

import (
	"math/rand"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/randutil"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/pkg/errors"
)

func TestApplySnapshotDenyPreemptive(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var tc testContext
	tc.Start(t)
	defer tc.Stop()

	key := roachpb.RKey("a")
	realRng := tc.store.LookupReplica(key, nil)

	// Use Raft to get a nontrivial term for our snapshot.
	if pErr := realRng.redirectOnOrAcquireLease(context.Background()); pErr != nil {
		t.Fatal(pErr)
	}

	snap, err := realRng.GetSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Make sure that the Term is behind our first range term (raftInitialLogTerm)
	snap.Metadata.Term--

	// Create an uninitialized version of the first range. This is only ok
	// because in the case we test, there's an error (and so we don't clobber
	// our actual first range in the Store). If we want snapshots to apply
	// successfully during tests, we need to adapt the snapshots to a new
	// RangeID first and generally do a lot more work.
	rng, err := NewReplica(&roachpb.RangeDescriptor{RangeID: 1}, tc.store, 0)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := rng.applySnapshot(snap, raftpb.HardState{}); !testutils.IsError(
		err, "cannot apply preemptive snapshot from past term",
	) {
		t.Fatal(err)
	}

	// Do something that extends the Raft log past what we have in the
	// snapshot.
	put := putArgs(roachpb.Key("a"), []byte("foo"))
	if _, pErr := tc.SendWrapped(&put); pErr != nil {
		t.Fatal(pErr)
	}
	snap.Metadata.Term++ // restore the "real" term of the snapshot

	if _, err := rng.applySnapshot(snap, raftpb.HardState{}); !testutils.IsError(
		err, "would erase acknowledged log entries",
	) {
		t.Fatal(err)
	}

}

const rangeID = 1
const keySize = 1 << 7   // 128 B
const valSize = 1 << 10  // 1 KiB
const snapSize = 1 << 25 // 32 MiB

func fillTestRange(t testing.TB, rep *Replica, size int) {
	src := rand.New(rand.NewSource(0))
	for i := 0; i < snapSize/(keySize+valSize); i++ {
		key := keys.MakeRowSentinelKey(randutil.RandBytes(src, keySize))
		val := randutil.RandBytes(src, valSize)
		pArgs := putArgs(key, val)
		if _, pErr := client.SendWrappedWith(rep, nil, roachpb.Header{
			RangeID: rangeID,
		}, &pArgs); pErr != nil {
			t.Fatal(pErr)
		}
	}
}

func TestSkipLargeReplicaSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, _, stopper := createTestStore(t)
	store.ctx.TestingKnobs.DisableSplitQueue = true
	// We want to manually control the size of the raft log.
	defer stopper.Stop()

	rep, err := store.GetReplica(rangeID)
	if err != nil {
		t.Fatal(err)
	}

	if pErr := rep.redirectOnOrAcquireLease(context.Background()); pErr != nil {
		t.Fatal(pErr)
	}

	fillTestRange(t, rep, snapSize)

	if _, err := rep.Snapshot(); err != nil {
		t.Fatalf("err on snapshot: %+v", err)
	}

	fillTestRange(t, rep, snapSize*2)

	if _, err := rep.Snapshot(); errors.Cause(err) != raft.ErrSnapshotTemporarilyUnavailable {
		t.Fatalf("snapshot of a very large range should fail but got %+v", err)
	}
}
