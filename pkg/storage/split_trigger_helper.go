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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"go.etcd.io/etcd/raft/raftpb"
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
	_, _ = lhsRepl.store.gcQueue.Add(lhsRepl, replicaGCPriorityDefault)
	return lhsRepl, true
}

// maybeDropMsgApp returns true if the incoming Raft message should be
// dropped. It does so if the recipient replica is blank (i.e. has no state)
// and is waiting for a split trigger to apply. Delivering the message in this
// situation may result in an unnecessary Raft snapshot, for an MsgApp would
// be rejected and the rejection would prompt the leader to send a snapshot,
// while the split trigger would likely populate the replica "for free".
// However, there are some situations in which this is not the case (all taken
// into account by this method by allowing the MsgApp through).
func maybeDropMsgApp(
	ctx context.Context,
	r interface {
		Args() (initialized bool, ticks int)
		ShouldDrop(key roachpb.RKey) (fmt.Stringer, bool)
	},
	req *RaftMessageRequest,
) (drop bool) {
	// Run the cheapest check first. If the leader doesn't think this replica is
	// probing, it won't set msg.Context (the common case).
	msg := req.Message
	if msg.Type != raftpb.MsgApp || len(msg.Context) == 0 {
		return false
	}

	// The leader doesn't know our state, so we injected its start key into the
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
	//                                            (1)\ MsgApp (Context='b')
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
	//                                            (1)\ MsgApp (Context='b')
	//                                                \
	//                                                 v
	// recipient           [a--lhs--b)               (this uninitialized replica)
	//
	// Trying to look up the replica for 'b', we'd come up empty and deliver the
	// message, resulting in a snapshot, as intended.

	verbose := verboseRaftLoggingEnabled()
	eventf := func(ctx context.Context, format string, args ...interface{}) {}
	if verbose {
		eventf = func(ctx context.Context, format string, args ...interface{}) {
			log.InfofDepth(ctx, 1, format, args...)
		}
	}

	startKey := roachpb.RKey(msg.Context)
	// NB: the caller is likely holding r.raftMu, but that's OK according to
	// the lock order. We're not allowed to hold r.mu, but we don't.
	lhsRepl, drop := r.ShouldDrop(startKey)
	if !drop {
		return false
	}

	eventf(ctx, "start key is contained in replica %v", lhsRepl)
	if ticks > maxDelaySplitTriggerTicks {
		// This is an escape hatch in case there are other scenarios (missed in
		// the above analysis) in which a split trigger just isn't coming. If
		// there are, the idea is that we notice this log message and improve
		// the heuristics.
		log.Warning(
			ctx,
			"would have dropped incoming MsgApp to wait for split trigger, "+
				"but allowing due to %d (>%d) ticks",
			ticks, maxDelaySplitTriggerTicks)
		return false
	}
	eventf(ctx, "dropping MsgApp at index %d to wait for split trigger", msg.Index)
	return true
}
