// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
)

// EmitMLAI registers the replica's last assigned max lease index with the
// closed timestamp tracker. This is called to emit an update about this
// replica in the absence of write activity.
func (r *Replica) EmitMLAI() {
	r.mu.Lock()
	lai := r.mu.lastAssignedLeaseIndex
	if r.mu.state.LeaseAppliedIndex > lai {
		lai = r.mu.state.LeaseAppliedIndex
	}
	epoch := r.mu.state.Lease.Epoch
	r.mu.Unlock()

	ctx := r.AnnotateCtx(context.Background())
	_, untrack := r.store.cfg.ClosedTimestamp.Tracker.Track(ctx)
	untrack(ctx, ctpb.Epoch(epoch), r.RangeID, ctpb.LAI(lai))
}
