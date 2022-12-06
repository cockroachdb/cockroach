// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package admission

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// XXX: Ideally we don't introduce yet-another-grant-coordinator type and
// duplicate code dealing with the corresponding work queue. This is the same
// problem with the ElasticCPUGranter. Consolidate.
type ioGrantCoordinator struct {
	// XXX: Could also directly key with struct with intern tuple <tenantID,
	// storeID>. Can't use syncutil.IntMap but maybe that's fine. Or maybe we
	// can use go's generics (generated or the one that shipped with go1.18) to
	// make it also work for this custom type.
	inflightWritesTracker *inflightWritesTracker
	workQueue             *WorkQueue
	workQueueMetrics      *WorkQueueMetrics

	// XXX: Granter intercepts all {local,remote} logical IO token consumption,
	// and returns to inflightWritesTracker. After doing so, it grants admission
	// to a request for a named tenant. The requester that accepts the grant
	// will do its internal prioritization.
	//
	// Since deduction from this pool happens after proposal evaluation, we need
	// to be careful to not over-admit at this point, lest our burst budget be
	// completely bypassed. We can use an estimate here to make sure we're not
	// admitting significantly more than we should. Once the request has passed
	// the proposal evaluation stage and actually deducted, we can drop it from
	// our "between gate and prop-eval inflight estimate". If we had
	// over-estimated, we can try and grant another request right then. We don't
	// need to do anything if we had under estimated.
	granter granter
	// XXX: Backed by a work queue. Needs interface addition to support
	// `granted()` to a specific tenant. Or `hasWaitingRequests` for a specific
	// tenant. So its able to peek into the heap.
	requester requester

	// XXX: Need to make sure that the pool is reset on failure (define some
	// interface), or {tenant,store} {additions,removals}. Or perhaps even other
	// conditions.

	// XXX: Make sure there's a top-level feature gate to switch all of this
	// off.

	// XXX: Given a batch request, find out the replica ID, use it to find the
	// locally held replica, look at its embedded range descriptor, to find all
	// the right StoreIDs. Those StoreIDs will be used here when finding the
	// right window to wait on.
	// - See kvadmission.ReplicaStoreIDsResolver

	// XXX: When interacting with today's StoreGrantCoordinators, do it through
	// some interface. And also have that interface be stubbed out by something
	// that's backed by RPC responses from remote nodes. Remote and local stores
	// should look identical.

	// XXX: If we returned once flushed out of the proposal quota buffer, would
	// that work? That thing maintains a map of things that are inflight -- we
	// could do the same.

	// XXX: Is this supposed to be a granter? Or a GrantCoordinator?
	// XXX: Note that the worst case burst will be reduced once we do copysets.
	// XXX: In the single node, or rf=1x case, everything should look the way it
	// does today. i.e. we have a WorkQueue for the LSM that's consuming logical
	// IO tokens by which it's enabling intra-tenant isolation and inter-tenant
	// prioritization. These tokens, when consumed, are re-added to the
	// inflightWritesTracker which permits more write work.
	// - XXX: When re-adding tokens for regular work, do we add to both pools?
	//   Perhaps we do if we've consumed from both pools. If we've consumed from
	//   just the one, then we add to just the one pool
}

type inflightWritesTracker struct {
	perTenantWritesTracker syncutil.IntMap // map[int64(roachpb.TenantID)]*tenantInflightWritesTracker
}

type tenantInflightWritesTracker struct {
	perStoreTracker syncutil.IntMap // map[int64(roachpb.StoreID)]*storeInflightWritesTracker
}

// XXX: Not correct to call this "inflight tracker". It's the available window,
// or "burst", or "burst budget", or "receive window" (from TCP), or
// "{send,usable} window". But it's not a sliding window since we're not
// tracking indexes that are being rolled over. There's no refill rate, we're
// working with just the burst. Talk about what we would do if we had perfect
// information with zero delay. This ability to burst lets us avoid the remote
// prediction problem and avoid the risk of under/over utilizing.
type storeInflightWritesTracker struct {
	regularBytes, elasticBytes int
}
