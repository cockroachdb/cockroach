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
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func newIOGrantCoordinator() *IOGrantCoordinator {
	i := &IOGrantCoordinator{
		maxBurstCapacity: map[workClass]int{
			regularWorkClass: 16 << 20, // 16 MiB
			elasticWorkClass: 8 << 20,  // 8 MiB
		},
	}
	i.mu.writeBurstCapacities = make(map[TenantStoreTuple]writesBurstCapacity)
	return i
}

func (i *IOGrantCoordinator) getWriteBurstCapacityLocked(ts TenantStoreTuple) writesBurstCapacity {
	c, ok := i.mu.writeBurstCapacities[ts]
	if !ok {
		c = newWritesBurstCapacity(
			i.maxBurstCapacity[regularWorkClass],
			i.maxBurstCapacity[elasticWorkClass],
			&i.mu,
		)
		i.mu.writeBurstCapacities[ts] = c
	}
	return c
}

func (i *IOGrantCoordinator) hasBurstCapacity(ts TenantStoreTuple, class workClass) bool {
	i.mu.Lock()
	defer i.mu.Unlock()

	c := i.getWriteBurstCapacityLocked(ts)
	return c.m[class] > 0
}

func (i *IOGrantCoordinator) WaitForBurstCapacity(ts TenantStoreTuple, pri admissionpb.WorkPriority) {
	class := regularWorkClass
	if pri < admissionpb.NormalPri {
		class = elasticWorkClass
	}

	i.mu.Lock()
	defer i.mu.Unlock()

	for {
		c := i.getWriteBurstCapacityLocked(ts)
		if c.m[class] > 0 {
			c.cond.Signal() // XXX: Signal the next waiter, if any
			return
		}

		c.cond.Wait()
	}
}

// XXX: Hook this up to the StoresTokenGranter/WorkQueue, which should adjust it
// once (logical) tokens are consumed.
func (i *IOGrantCoordinator) AdjustBurstCapacity(ts TenantStoreTuple, class workClass, delta int) {
	i.mu.Lock()
	defer i.mu.Unlock()

	c := i.getWriteBurstCapacityLocked(ts)
	switch class {
	case regularWorkClass:
		c.m[regularWorkClass] += delta
		c.m[elasticWorkClass] += delta
	case elasticWorkClass:
		c.m[elasticWorkClass] += delta
	}
	if delta < 0 {
		return
	}

	for _, cl := range []workClass{regularWorkClass, elasticWorkClass} {
		if c.m[cl] > i.maxBurstCapacity[cl] {
			c.m[cl] = i.maxBurstCapacity[cl]
		}
	}
	c.cond.Signal() // XXX: Signal the next waiter, if any
}

// XXX: Ideally we don't introduce yet-another-grant-coordinator type and
// duplicate code dealing with the corresponding work queue. This is the same
// problem with the ElasticCPUGranter. Consolidate.
// XXX: Pick a better name, since we have an kvStoreTokenGranter. Something
// something "burst"? Or "replicated write"?
type IOGrantCoordinator struct {
	// XXX: Can't use syncutil.IntMap but maybe that's fine. Or maybe we can use
	// go's generics (generated or the one that shipped with go1.18) to make it
	// also work for this custom type.
	mu struct {
		syncutil.Mutex // XXX: Shared across all cond variables

		writeBurstCapacities map[TenantStoreTuple]writesBurstCapacity // lazily instantiated
		// XXX: Need to prune records from this map as tenants/stores disappear,
		// or stores 'fail' (whatever that means).
	}
	maxBurstCapacity map[workClass]int

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
	//    granter granter
	// XXX: Backed by a work queue. Needs interface addition to support
	// `granted()` to a specific tenant. Or `hasWaitingRequests` for a specific
	// tenant. So its able to peek into the heap.
	//    requester requester

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

type TenantStoreTuple struct {
	roachpb.StoreID
	roachpb.TenantID
}

// XXX: Not correct to call this "inflight tracker". It's the available window,
// or "burst", or "burst budget", or "receive window" (from TCP), or
// "{send,usable} window". But it's not a sliding window since we're not
// tracking indexes that are being rolled over. There's no refill rate, we're
// working with just the burst. Talk about what we would do if we had perfect
// information with zero delay. This ability to burst lets us avoid the remote
// prediction problem and avoid the risk of under/over utilizing.
type writesBurstCapacity struct {
	m    map[workClass]int
	cond *sync.Cond
}

func newWritesBurstCapacity(regular, elastic int, l sync.Locker) writesBurstCapacity {
	return writesBurstCapacity{
		m: map[workClass]int{
			regularWorkClass: regular,
			elasticWorkClass: elastic,
		},
		cond: sync.NewCond(l),
	}
}
