// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/replica_rac2"
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
	tc.Start(ctx, t, stopper)
	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			var r Replica
			r.shMu.state.Desc = &c.oldDesc
			r.mu.lastReplicaAdded = c.lastReplicaAdded
			r.mu.replicaFlowControlIntegration = newReplicaFlowControlIntegration((*replicaFlowControl)(&r), nil, nil)
			r.flowControlV2 = noopProcessor{}
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

// noopProcessor provides a noop implementation of OnDescChangedLocked, since
// the test does not initialize any of the dependencies needed by a real
// replica_rac2.Processor.
type noopProcessor struct {
	// Always nil
	replica_rac2.Processor
}

func (p noopProcessor) OnDescChangedLocked(
	ctx context.Context, desc *roachpb.RangeDescriptor, tenantID roachpb.TenantID,
) {
}
