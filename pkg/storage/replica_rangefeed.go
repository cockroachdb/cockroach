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
// permissions and limitations under the License.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/rangefeed"
)

// RangeFeed ... WIP:
func (r *Replica) RangeFeed(
	ctx context.Context, args *roachpb.RangeFeedRequest, stream roachpb.Internal_RangeFeedServer,
) error {
	// Add the range log tag
	ctx = r.AnnotateCtx(ctx)

	// rSpan := roachpb.RSpan{Key: roachpb.RKey(args.Span.Key), EndKey: roachpb.RKey(args.Span.EndKey)}
	// if err := r.requestCanProceed(rSpan, args.Timestamp); err != nil {
	// 	return err
	// }

	// If the internal Raft group is not initialized, create it and wake the leader.
	r.maybeInitializeRaftGroup(ctx)

	// Register the range feed ...
	errC := make(chan error)
	r.mu.Lock()
	r.maybeInitializeRangefeedLocked()
	defer func() {
		r.mu.Lock()
		r.maybeFinalizeRangefeedLocked()
		r.mu.Unlock()
	}()

	r.mu.rangeFeed.Register(args.Span, stream, errC)
	r.mu.Unlock()

	return <-errC
}

func (r *Replica) maybeInitializeRangefeedLocked() {
	if r.mu.rangeFeed != nil {
		return
	}
	r.mu.rangeFeed = rangefeed.NewProcessor(r.store.Clock(), r.store.Engine(), r.mu.state.Desc.RSpan())
	r.mu.rangeFeed.Start(r.store.Stopper())
}

func (r *Replica) maybeFinalizeRangefeedLocked() {
	if r.mu.rangeFeed == nil {
		return
	}
	if r.mu.rangeFeed.Len() == 0 {
		r.mu.rangeFeed.Stop()
		r.mu.rangeFeed = nil
	}
}
