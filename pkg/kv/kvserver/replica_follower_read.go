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

	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// FollowerReadsEnabled controls whether replicas attempt to serve follower
// reads. The closed timestamp machinery is unaffected by this, i.e. the same
// information is collected and passed around, regardless of the value of this
// setting.
var FollowerReadsEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"kv.closed_timestamp.follower_reads_enabled",
	"allow (all) replicas to serve consistent historical reads based on closed timestamp information",
	true,
).WithPublic()

// BatchCanBeEvaluatedOnFollower determines if a batch consists exclusively of
// requests that can be evaluated on a follower replica, given a sufficiently
// advanced closed timestamp.
func BatchCanBeEvaluatedOnFollower(ba roachpb.BatchRequest) bool {
	// Explanation of conditions:
	// 1. the batch cannot have or intend to receive a timestamp set from a
	//    server-side clock. If a follower with a lagging clock sets its timestamp
	//    and this then allows the follower to evaluate the batch as a follower
	//    read, then the batch might miss past writes served at higher timestamps
	//    on the leaseholder.
	// 2. each request in the batch needs to be "transactional", because those are
	//    the only ones that have clearly defined semantics when served under the
	//    closed timestamp.
	// 3. the batch needs to be read-only, because a follower replica cannot
	//    propose writes to Raft.
	// 4. the batch needs to be non-locking, because unreplicated locks are only
	//    held on the leaseholder.
	tsFromServerClock := ba.Txn == nil && (ba.Timestamp.IsEmpty() || ba.TimestampFromServerClock != nil)
	if tsFromServerClock {
		return false
	}
	return ba.IsAllTransactional() && ba.IsReadOnly() && !ba.IsLocking()
}

// canServeFollowerReadRLocked tests, when a range lease could not be acquired,
// whether the batch can be served as a follower read despite the error. Only
// non-locking, read-only requests can be served as follower reads. The batch
// must be transactional and composed exclusively of this kind of request to be
// accepted as a follower read.
func (r *Replica) canServeFollowerReadRLocked(ctx context.Context, ba *roachpb.BatchRequest) bool {
	eligible := BatchCanBeEvaluatedOnFollower(*ba) && FollowerReadsEnabled.Get(&r.store.cfg.Settings.SV)
	if !eligible {
		// We couldn't do anything with the error, propagate it.
		return false
	}

	repDesc, err := r.getReplicaDescriptorRLocked()
	if err != nil {
		return false
	}

	switch typ := repDesc.GetType(); typ {
	case roachpb.VOTER_FULL, roachpb.VOTER_INCOMING, roachpb.NON_VOTER:
	default:
		log.Eventf(ctx, "%s replicas cannot serve follower reads", typ)
		return false
	}

	requiredFrontier := ba.RequiredFrontier()
	maxClosed := r.getClosedTimestampRLocked(ctx, requiredFrontier /* sufficient */)
	canServeFollowerRead := requiredFrontier.LessEq(maxClosed)
	tsDiff := requiredFrontier.GoTime().Sub(maxClosed.GoTime())
	if !canServeFollowerRead {
		uncertaintyLimitStr := "n/a"
		if ba.Txn != nil {
			uncertaintyLimitStr = ba.Txn.GlobalUncertaintyLimit.String()
		}

		// We can't actually serve the read based on the closed timestamp.
		// Signal the clients that we want an update so that future requests can succeed.
		log.Eventf(ctx, "can't serve follower read; closed timestamp too low by: %s; maxClosed: %s ts: %s uncertaintyLimit: %s",
			tsDiff, maxClosed, ba.Timestamp, uncertaintyLimitStr)
		return false
	}

	// This replica can serve this read!
	//
	// TODO(tschottdorf): once a read for a timestamp T has been served, the replica may
	// serve reads for that and smaller timestamps forever.
	log.Eventf(ctx, "%s; query timestamp below closed timestamp by %s", kvbase.FollowerReadServingMsg, -tsDiff)
	r.store.metrics.FollowerReadsCount.Inc(1)
	return true
}

// getClosedTimestampRLocked is like maxClosed, except that it requires r.mu to be
// rlocked. It also optionally takes a hint: if sufficient is not
// empty, getClosedTimestampRLocked might return a timestamp that's lower than the
// maximum closed timestamp that we know about, as long as the returned
// timestamp is still >= sufficient. This is a performance optimization because
// we can avoid consulting the ClosedTimestampReceiver.
func (r *Replica) getClosedTimestampRLocked(
	ctx context.Context, sufficient hlc.Timestamp,
) hlc.Timestamp {
	appliedLAI := ctpb.LAI(r.mu.state.LeaseAppliedIndex)
	leaseholder := r.mu.state.Lease.Replica.NodeID
	raftClosed := r.mu.state.RaftClosedTimestamp
	sideTransportClosed := r.sideTransportClosedTimestamp.get(ctx, leaseholder, appliedLAI, sufficient)

	var maxClosed hlc.Timestamp
	maxClosed.Forward(raftClosed)
	maxClosed.Forward(sideTransportClosed)
	return maxClosed
}

// GetClosedTimestamp returns the maximum closed timestamp for this range.
//
// GetClosedTimestamp is part of the EvalContext interface.
func (r *Replica) GetClosedTimestamp(ctx context.Context) hlc.Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getClosedTimestampRLocked(ctx, hlc.Timestamp{} /* sufficient */)
}
