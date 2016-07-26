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

	"github.com/coreos/etcd/raft"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/randutil"
)

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

	if _, err := rep.GetSnapshot(context.Background()); err != nil {
		t.Fatal(err)
	}

	fillTestRange(t, rep, snapSize*2)

	if _, err := rep.Snapshot(); err != raft.ErrSnapshotTemporarilyUnavailable {
		t.Fatalf("snapshot of a very large range should fail but got %v", err)
	}
}
