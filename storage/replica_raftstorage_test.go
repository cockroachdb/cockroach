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
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/coreos/etcd/raft/raftpb"
)

func TestApplySnapshotDenyPreemptive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("TODO(tschottdorf): test doesn't apply in this WIP")

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
