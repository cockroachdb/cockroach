// Copyright 2018 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

const maxDelaySplitTriggerTicks = 100

type replicaMsgAppDropper Replica

func (rd *replicaMsgAppDropper) Args() (initialized bool, ticks int) {
	r := (*Replica)(rd)
	r.mu.RLock()
	initialized = r.isInitializedRLocked()
	ticks = r.mu.ticks
	r.mu.RUnlock()
	return initialized, ticks
}

func (rd *replicaMsgAppDropper) ShouldDrop(startKey roachpb.RKey) (fmt.Stringer, bool) {
	lhsRepl := (*Replica)(rd).store.LookupReplica(startKey)
	if lhsRepl == nil {
		return nil, false
	}
	lhsRepl.store.gcQueue.AddAsync(context.Background(), lhsRepl, replicaGCPriorityDefault)
	return lhsRepl, true
}

type msgAppDropper interface {
	Args() (initialized bool, ticks int)
	ShouldDrop(key roachpb.RKey) (fmt.Stringer, bool)
}

// maybeDropMsgApp returns true if the incoming Raft message should be dropped.
// It does so if the recipient replica is uninitialized (i.e. has no state) and
// is waiting for a split trigger to apply,in which case  delivering the message
// in this situation would result in an unnecessary Raft snapshot: the MsgApp
// would be rejected and the rejection would prompt the leader to send a
// snapshot, while the split trigger would likely populate the replica "for
// free". However, there are some situations in which this is not the case (all
// taken into account by this method by allowing the MsgApp through).
func maybeDropMsgApp(
	ctx context.Context, r msgAppDropper, msg *raftpb.Message, startKey roachpb.RKey,
) (drop bool) {
	// Run the cheapest check first. If the leader doesn't think this replica is
	// probing, it won't set msg.Context (the common case).
	// Note that startKey could be of length zero (not nil) if the sender is a
	// replica of the first range.
	if msg.Type != raftpb.MsgApp || startKey == nil {
		return false
	}

	// The leader doesn't know our state, so it injected its start key into the
	// message via msg.Context. Check if this replica might be waiting for a
	// split trigger. The first condition for that is not knowing the key
	// bounds, i.e. not being initialized.
	initialized, ticks := r.Args()

	if initialized {
		return false
	}

	// The goal is to find out if this replica is waiting for a split trigger.
	// We do so by looking up the start key in the local store. If we find a
	// replica for the start key, we know that that replica is in theory going
	// to apply the split trigger and populate the right hand side (i.e. this
	// replica):
	//
	// sender  (leader)    [a--lhs--b)[b---rhs----c)
	//                                             \
	//                                              \
	//                                            (1)\ MsgApp (startKey='b')
	//                                                \
	//                                                 v
	// recipient           [a----------lhs--------c) (this uninitialized replica)
	//                                 ʌ                /
	//                                  \______________/ (2)
	//                                         'b'
	//
	// However, it's also possible that the left hand side has been rebalanced
	// away and is going to be GC'ed soon; queue a check to make sure this would
	// happen ASAP. (The leader will probe this replica only once per heartbeat
	// interval, so we're not going to queue these checks at some high rate).
	//
	// New replicas only get created through splits or rebalances, so if we
	// don't find a left hand side, it was either garbage collected after having
	// been removed from the store (see the above comment), or there wasn't a
	// split in the first case and this replica was instead created through an
	// up-replication for which the preemptive snapshot had been lost (i.e.
	// accidentally GC'ed before the replication change succeeded).
	//
	// Note that there's a subtle case in which the left hand side is caught up
	// across the split trigger via a snapshot. In that case, since we're looking
	// up the start key of the right-hand side, we have the following picture:
	//
	// sender  (leader)    [a--lhs--b)[b---rhs----c)
	//                                             \
	//                                              \
	//                                            (1)\ MsgApp (startKey='b')
	//                                                \
	//                                                 v
	// recipient           [a--lhs--b)               (this uninitialized replica)
	//
	// Trying to look up the replica for 'b', we'd come up empty and deliver the
	// message, resulting in a snapshot, as intended.
	//
	// Note that the invariant that the start key points at a replica that will
	// definitely apply the split trigger holds even if the left-hand range
	// carries out splits (as that doesn't change its start key) or gets merged
	// away (as this entails either a removal of the follower's replica during
	// colocation, or waiting for the follower to have caught up which implies
	// executing all pending split triggers).

	verbose := verboseRaftLoggingEnabled()

	// NB: the caller is likely holding r.raftMu, but that's OK according to
	// the lock order. We're not allowed to hold r.mu, but we don't.
	lhsRepl, drop := r.ShouldDrop(startKey)
	if !drop {
		return false
	}

	if verbose {
		log.Infof(ctx, "start key is contained in replica %v", lhsRepl)
	}
	if ticks > maxDelaySplitTriggerTicks {
		// This is an escape hatch in case there are other scenarios (missed in
		// the above analysis) in which a split trigger just isn't coming. If
		// there are, the idea is that we notice this log message and improve
		// the heuristics.
		log.Warningf(
			ctx,
			"would have dropped incoming MsgApp to wait for split trigger, "+
				"but allowing due to %d (>%d) ticks",
			ticks, maxDelaySplitTriggerTicks)
		return false
	}
	if verbose {
		log.Infof(ctx, "dropping MsgApp at index %d to wait for split trigger", msg.Index)
	}
	return true
}
