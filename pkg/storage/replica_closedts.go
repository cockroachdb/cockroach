// Copyright 2019 The Cockroach Authors.
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
	r.mu.Unlock()

	ctx := r.AnnotateCtx(context.Background())
	_, untrack := r.store.cfg.ClosedTimestamp.Tracker.Track(ctx)
	untrack(ctx, r.RangeID, ctpb.LAI(lai))
}
