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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
)

// EmitMLAI registers the replica's last assigned max lease index with the
// closed timestamp tracker. This is called to emit an update about this
// replica in the absence of write activity.
func (r *Replica) EmitMLAI() {
	r.mu.RLock()
	lai := r.mu.proposalBuf.LastAssignedLeaseIndexRLocked()
	if r.mu.state.LeaseAppliedIndex > lai {
		lai = r.mu.state.LeaseAppliedIndex
	}
	epoch := r.mu.state.Lease.Epoch
	isLeaseholder := r.mu.state.Lease.Replica.ReplicaID == r.mu.replicaID
	r.mu.RUnlock()

	// If we're the leaseholder of an epoch-based lease, notify the minPropTracker
	// of the current LAI to trigger a re-broadcast of this range's LAI.
	if isLeaseholder && epoch > 0 {
		ctx := r.AnnotateCtx(context.Background())
		_, untrack := r.store.cfg.ClosedTimestamp.Tracker.Track(ctx)
		untrack(ctx, ctpb.Epoch(epoch), r.RangeID, ctpb.LAI(lai))
	}
}
