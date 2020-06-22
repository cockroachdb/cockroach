// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func TestReplicaUpdateLastReplicaAdded(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	desc := func(replicaIDs ...roachpb.ReplicaID) roachpb.RangeDescriptor {
		d := roachpb.RangeDescriptor{
			StartKey:         roachpb.RKey("a"),
			EndKey:           roachpb.RKey("b"),
			InternalReplicas: make([]roachpb.ReplicaDescriptor, len(replicaIDs)),
		}
		for i, id := range replicaIDs {
			d.InternalReplicas[i].ReplicaID = id
		}
		return d
	}

	testCases := []struct {
		oldDesc                  roachpb.RangeDescriptor
		newDesc                  roachpb.RangeDescriptor
		lastReplicaAdded         roachpb.ReplicaID
		expectedLastReplicaAdded roachpb.ReplicaID
	}{
		// Adding a replica. In normal operation, Replica IDs always increase.
		{desc(), desc(1), 0, 1},
		{desc(1), desc(1, 2), 0, 2},
		{desc(1, 2), desc(1, 2, 3), 0, 3},
		// Add a replica with an out-of-order ID (this shouldn't happen in practice).
		{desc(2, 3), desc(1, 2, 3), 0, 0},
		// Removing a replica has no-effect.
		{desc(1, 2, 3), desc(2, 3), 3, 3},
		{desc(1, 2, 3), desc(1, 3), 3, 3},
		{desc(1, 2, 3), desc(1, 2), 3, 0},
	}

	tc := testContext{}
	stopper := stop.NewStopper()
	ctx := context.Background()
	defer stopper.Stop(ctx)
	tc.Start(t, stopper)
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var r Replica
			r.mu.state.Desc = &c.oldDesc
			r.mu.lastReplicaAdded = c.lastReplicaAdded
			r.store = tc.store
			r.concMgr = tc.repl.concMgr
			r.setDescRaftMuLocked(context.Background(), &c.newDesc)
			if c.expectedLastReplicaAdded != r.mu.lastReplicaAdded {
				t.Fatalf("expected %d, but found %d",
					c.expectedLastReplicaAdded, r.mu.lastReplicaAdded)
			}
		})
	}
}
