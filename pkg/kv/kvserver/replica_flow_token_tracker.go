// Copyright 2023 The Cockroach Authors.
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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type flowTokenTracker struct {
	tenID              roachpb.TenantID
	IOGrantCoordinator *admission.IOGrantCoordinator

	mu struct {
		syncutil.Mutex
		acquiredFlowTokens map[roachpb.StoreID]map[admissionpb.WorkPriority][]acquiredFlowToken
	}
}

type acquiredFlowToken struct {
	count int64
	entry admission.TermIndexTuple
}

var _ admission.FlowTokenTracker = &Replica{}

func newFlowTokenTracker(
	tenID roachpb.TenantID, iog *admission.IOGrantCoordinator,
) *flowTokenTracker {
	t := &flowTokenTracker{
		tenID:              tenID,
		IOGrantCoordinator: iog,
	}
	t.mu.acquiredFlowTokens = make(map[roachpb.StoreID]map[admissionpb.WorkPriority][]acquiredFlowToken)
	return t
}

func (f *flowTokenTracker) Track(
	ctx context.Context,
	storeID roachpb.StoreID,
	pri admissionpb.WorkPriority,
	count int64,
	entry admission.TermIndexTuple,
	rid, originalRid roachpb.RangeID,
) bool {
	if f == nil {
		// XXX: Can happen if proposing immediately on a newly split off RHS,
		// that doesn't know it's a leader yet. Don't track or deduct flow
		// tokens for it. Flow token tracker only has a lifetime while we
		// explicitly know that we're the leader. When we no longer are, we'll
		// release flow tokens. For future responses, they should not find the
		// flow token tracker given we're no longer the leader. If we've
		// regained leadership, we need to ignore those re-additions by checking
		// the raft term. TODO.
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	newlyAcquired := acquiredFlowToken{
		count: count,
		entry: entry,
	}
	if _, ok := f.mu.acquiredFlowTokens[storeID]; !ok {
		f.mu.acquiredFlowTokens[storeID] = map[admissionpb.WorkPriority][]acquiredFlowToken{}
	}
	if _, ok := f.mu.acquiredFlowTokens[storeID][pri]; !ok {
		f.mu.acquiredFlowTokens[storeID][pri] = []acquiredFlowToken{}
	}

	var lastAcquisition acquiredFlowToken
	if len(f.mu.acquiredFlowTokens[storeID][pri]) >= 1 {
		lastAcquisition = f.mu.acquiredFlowTokens[storeID][pri][len(f.mu.acquiredFlowTokens[storeID][pri])-1]
	}
	f.mu.acquiredFlowTokens[storeID][pri] = append(f.mu.acquiredFlowTokens[storeID][pri], newlyAcquired)
	if !lastAcquisition.entry.Less(newlyAcquired.entry) {
		log.Fatalf(ctx, "expected in order acquisitions (%s < %s): tracker held on r%d: handle originally referred to r%d", lastAcquisition.entry, newlyAcquired.entry, rid, originalRid)
	}
	return true
}

func (f *flowTokenTracker) Release(
	ctx context.Context,
	storeID roachpb.StoreID,
	pri admissionpb.WorkPriority,
	entry admission.TermIndexTuple,
) {
	if f == nil {
		// XXX: we're trying to release tokens to a bucket that no longer
		// exists, likely because we've lost leadership since we've acquired it
		// originally. But it's possible to have reacquired the entry, and we do
		// want to reject it, so compare raft terms. Should initialize the flow
		// token tracker with the raft term it's tracking things over.
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.mu.acquiredFlowTokens[storeID]; !ok {
		return
	}
	if _, ok := f.mu.acquiredFlowTokens[storeID][pri]; !ok {
		return
	}

	ts := admission.TenantStoreTuple{
		TenantID: f.tenID,
		StoreID:  storeID,
	}
	var i int
	for {
		if i == len(f.mu.acquiredFlowTokens[storeID][pri]) {
			break
		}

		acquired := f.mu.acquiredFlowTokens[storeID][pri][i]
		if !acquired.entry.LessEq(entry) {
			break
		}

		f.IOGrantCoordinator.AdjustBurstCapacity(ctx, ts, pri, acquired.count, acquired.entry)
		i += 1 // processed
	}

	before := len(f.mu.acquiredFlowTokens[storeID][pri])
	next := ""
	f.mu.acquiredFlowTokens[storeID][pri] = f.mu.acquiredFlowTokens[storeID][pri][i:]
	if len(f.mu.acquiredFlowTokens[storeID][pri]) > 0 {
		next = fmt.Sprintf(" (%s, ...)", f.mu.acquiredFlowTokens[storeID][pri][0].entry)
	}
	log.VInfof(ctx, 1, "released flow control tokens for %d/%d pri=%s %s proposals, until %s; %d proposal(s) remain%s",
		i, before, pri, ts, entry, len(f.mu.acquiredFlowTokens[storeID][pri]), next)
}

func (f *flowTokenTracker) ReleaseAll(ctx context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for storeID, workPriToAcquiredFlowTokens := range f.mu.acquiredFlowTokens {
		for workPri, acquiredFlowTokens := range workPriToAcquiredFlowTokens {
			for _, acquired := range acquiredFlowTokens {
				f.IOGrantCoordinator.AdjustBurstCapacity(ctx, admission.TenantStoreTuple{
					TenantID: f.tenID,
					StoreID:  storeID,
				}, workPri, acquired.count, acquired.entry)
			}
		}
		delete(f.mu.acquiredFlowTokens, storeID)
	}
	// XXX: XXX: Log something under verbosity here.
}

func (r *Replica) TrackReplicaMuLocked(
	ctx context.Context,
	storeID roachpb.StoreID,
	pri admissionpb.WorkPriority,
	count int64,
	entry admission.TermIndexTuple,
	originalRangeID roachpb.RangeID,
) bool {
	return r.mu.flowTokenTracker.Track(ctx, storeID, pri, count, entry, r.RangeID, originalRangeID)
}

func (r *Replica) Release(
	ctx context.Context,
	storeID roachpb.StoreID,
	pri admissionpb.WorkPriority,
	entry admission.TermIndexTuple,
) {
	r.mu.flowTokenTracker.Release(ctx, storeID, pri, entry)
}
