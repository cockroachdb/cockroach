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
	ctstorage "github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/storage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// FollowerReadsEnabled controls whether replicas attempt to serve follower
// reads. The closed timestamp machinery is unaffected by this, i.e. the same
// information is collected and passed around, regardless of the value of this
// setting.
var FollowerReadsEnabled = settings.RegisterBoolSetting(
	"kv.closed_timestamp.follower_reads_enabled",
	"allow (all) replicas to serve consistent historical reads based on closed timestamp information",
	true,
).WithPublic()

// BatchCanBeEvaluatedOnFollower determines if a batch consists exclusively of
// requests that can be evaluated on a follower replica, given a sufficiently
// advanced closed timestamp.
func BatchCanBeEvaluatedOnFollower(ba roachpb.BatchRequest) bool {
	// Explanation of conditions:
	// 1. the batch needs to be part of a transaction, because non-transactional
	//    batches often rely on the server setting their timestamp. If a follower
	//    with a lagging clock sets their timestamp then they might miss past
	//    writes served at higher timestamps.
	// 2. each request in the batch needs to be "transactional", because those are
	//    the only ones that have clearly defined semantics when served under the
	//    closed timestamp.
	// 3. the batch needs to be read-only, because a follower replica cannot
	//    propose writes to Raft.
	// 4. the batch needs to be non-locking, because unreplicated locks are only
	//    held on the leaseholder.
	return ba.Txn != nil && ba.IsAllTransactional() && ba.IsReadOnly() && !ba.IsLocking()
}

// replicaUpdate contains updates to be applied to a replica. It's intended to
// be returned by functions holding r.mu in reader mode, to be applied later
// when the mutex can be taken in write mode.
type replicaUpdate struct {
	sideTransportClosedTimestamp hlc.Timestamp
	sideTransportClosedLAI       ctpb.LAI
}

// apply copies the information into the replica. This cannot be called with r.mu held.
func (u replicaUpdate) apply(ctx context.Context, r *Replica) {
	if u == (replicaUpdate{}) {
		return
	}
	r.ForwardSideTransportClosedTimestamp(ctx, u.sideTransportClosedTimestamp, u.sideTransportClosedLAI)
}

// canServeFollowerReadRLocked tests, when a range lease could not be acquired,
// whether the batch can be served as a follower read despite the error. Only
// non-locking, read-only requests can be served as follower reads. The batch
// must be transactional and composed exclusively of this kind of request to be
// accepted as a follower read.
func (r *Replica) canServeFollowerReadRLocked(
	ctx context.Context, ba *roachpb.BatchRequest, err error,
) (bool, replicaUpdate) {
	var lErr *roachpb.NotLeaseHolderError
	eligible := errors.As(err, &lErr) &&
		lErr.LeaseHolder != nil && lErr.Lease.Type() == roachpb.LeaseEpoch &&
		BatchCanBeEvaluatedOnFollower(*ba) &&
		FollowerReadsEnabled.Get(&r.store.cfg.Settings.SV)

	if !eligible {
		// We couldn't do anything with the error, propagate it.
		return false, replicaUpdate{}
	}

	repDesc, err := r.getReplicaDescriptorRLocked()
	if err != nil {
		return false, replicaUpdate{}
	}

	switch typ := repDesc.GetType(); typ {
	case roachpb.VOTER_FULL, roachpb.VOTER_INCOMING, roachpb.NON_VOTER:
	default:
		log.Eventf(ctx, "%s replicas cannot serve follower reads", typ)
		return false, replicaUpdate{}
	}

	requiredFrontier := ba.Txn.RequiredFrontier()
	maxClosed, _, update := r.maxClosedRLocked(ctx, requiredFrontier /* sufficient */)
	canServeFollowerRead := requiredFrontier.LessEq(maxClosed)
	tsDiff := requiredFrontier.GoTime().Sub(maxClosed.GoTime())
	if !canServeFollowerRead {
		uncertaintyLimitStr := "n/a"
		if ba.Txn != nil {
			uncertaintyLimitStr = ba.Txn.GlobalUncertaintyLimit.String()
		}

		// We can't actually serve the read based on the closed timestamp.
		// Signal the clients that we want an update so that future requests can succeed.
		r.store.cfg.ClosedTimestamp.Clients.Request(lErr.LeaseHolder.NodeID, r.RangeID)
		log.Eventf(ctx, "can't serve follower read; closed timestamp too low by: %s; maxClosed: %s ts: %s uncertaintyLimit: %s",
			tsDiff, maxClosed, ba.Timestamp, uncertaintyLimitStr)

		if false {
			// NB: this can't go behind V(x) because the log message created by the
			// storage might be gigantic in real clusters, and we don't want to trip it
			// using logspy.
			log.Warningf(ctx, "can't serve follower read for %s at epo %d, storage is %s",
				ba.Timestamp, lErr.Lease.Epoch,
				r.store.cfg.ClosedTimestamp.Storage.(*ctstorage.MultiStorage).StringForNodes(lErr.LeaseHolder.NodeID),
			)
		}
		return false, update
	}

	// This replica can serve this read!
	//
	// TODO(tschottdorf): once a read for a timestamp T has been served, the replica may
	// serve reads for that and smaller timestamps forever.
	log.Eventf(ctx, "%s; query timestamp below closed timestamp by %s", kvbase.FollowerReadServingMsg, -tsDiff)
	r.store.metrics.FollowerReadsCount.Inc(1)
	return true, update
}

// maxClosed returns the maximum closed timestamp for this range.
// It is computed as the most recent of the known closed timestamp for the
// current lease holder for this range as tracked by the closed timestamp
// subsystem and the start time of the current lease. It is safe to use the
// start time of the current lease because leasePostApply bumps the timestamp
// cache forward to at least the new lease start time. Using this combination
// allows the closed timestamp mechanism to be robust to lease transfers.
// If the ok return value is false, the Replica is a member of a range which
// uses an expiration-based lease. Expiration-based leases do not support the
// closed timestamp subsystem. A zero-value timestamp will be returned if ok
// is false.
//
// TODO(andrei): Remove the bool retval once we remove the old closed timestamp
// mechanism.
func (r *Replica) maxClosed(ctx context.Context) (_ hlc.Timestamp, ok bool) {
	r.mu.RLock()
	res, ok, update := r.maxClosedRLocked(ctx, hlc.Timestamp{} /* sufficient */)
	r.mu.RUnlock()
	update.apply(ctx, r)
	return res, ok
}

// maxClosedRLocked is like maxClosed, except that it requires r.mu to be
// rlocked. It also optionally takes a hint: if sufficient is not
// empty, maxClosedRLocked might return a timestamp that's lower than the
// maximum closed timestamp that we know about, as long as the returned
// timestamp is still >= sufficient. This is a performance optimization because
// we can avoid consulting the ClosedTimestampReceiver.
func (r *Replica) maxClosedRLocked(
	ctx context.Context, sufficient hlc.Timestamp,
) (_ hlc.Timestamp, ok bool, _ replicaUpdate) {
	appliedLAI := ctpb.LAI(r.mu.state.LeaseAppliedIndex)
	lease := r.mu.state.Lease
	initialMaxClosed := r.mu.initialMaxClosed
	replicaStateClosed := r.mu.state.RaftClosedTimestamp
	// Consider the timestamp closed through the side-transport. Such a timestamp
	// can be in two places:
	// - r.mu.sideTransportClosedTimestamp
	// - in the sidetransport.Receiver
	// We check the former here. We check the latter further down, only if we have
	// to.
	var sideTransportClosed hlc.Timestamp
	sideTransportClosedMaybe, minLAI := r.getSideTransportClosedTimestampRLocked()
	// We can use sideTransportClosedMaybe if we've applied at least up to minLAI.
	// The replica could in theory maintain more information about what lower
	// timestamps the side transport had closed with lower LAIs, but we don't
	// bother.
	replicationBehind := appliedLAI < minLAI
	if !replicationBehind {
		sideTransportClosed = sideTransportClosedMaybe
	}

	// TODO(andrei): In 21.1 we added support for closed timestamps on ranges with
	// expiration-based leases. Once the old closed timestamp transport is gone in
	// 21.2, this can go away.
	if lease.Expiration != nil {
		return hlc.Timestamp{}, false, replicaUpdate{}
	}
	// Look at the legacy closed timestamp propagation mechanism.
	maxClosed := r.store.cfg.ClosedTimestamp.Provider.MaxClosed(
		lease.Replica.NodeID, r.RangeID, ctpb.Epoch(lease.Epoch), appliedLAI)
	maxClosed.Forward(lease.Start.ToTimestamp())
	maxClosed.Forward(initialMaxClosed)

	// Look at the "new" closed timestamp propagation mechanism.
	maxClosed.Forward(replicaStateClosed)
	maxClosed.Forward(sideTransportClosed)

	// If the closed timestamp we know so far is sufficient, we return early
	// without consulting the ClosedTimestampReceiver.
	if !sufficient.IsEmpty() && sufficient.LessEq(maxClosed) {
		return maxClosed, true, replicaUpdate{}
	}

	// We now look at sidetransport.Receiver, unless replicationBehind was set;
	// the LAIs in the Receiver are >= the one returned by
	// getSideTransportClosedTimestampRLocked(), so there's no point in even
	// checking.
	var update replicaUpdate
	// In some tests the lease can be empty, or the ClosedTimestampReceiver might
	// not be set.
	// TODO(andrei): Remove the ClosedTimestampReceiver == nil protection once the
	// multiTestContext goes away.
	if !replicationBehind && !lease.Empty() && r.store.cfg.ClosedTimestampReceiver != nil {
		otherSideTransportClosed, otherSideTransportLAI :=
			r.store.cfg.ClosedTimestampReceiver.GetClosedTimestamp(ctx, r.RangeID, lease.Replica.NodeID)
		if appliedLAI < otherSideTransportLAI {
			otherSideTransportClosed = hlc.Timestamp{}
		}
		// If otherSideTransportClosed ends up winning, we return it in update so
		// that the caller copies it into the Replica. Hopefully, future calls with
		// `sufficient` set don't need to go to the Receiver for a while.
		if maxClosed.Forward(otherSideTransportClosed) {
			update = replicaUpdate{
				sideTransportClosedTimestamp: otherSideTransportClosed,
				sideTransportClosedLAI:       otherSideTransportLAI,
			}
		}
	}

	return maxClosed, true, update
}

// ClosedTimestampV2 returns the closed timestamp. Unlike MaxClosedTimestamp, it
// only looks at the "new" closed timestamp mechanism, ignoring the old one. It
// returns an empty result if the new mechanism is not enabled yet. The new
// mechanism has better properties than the old one - namely the closing of
// timestamps is synchronized with lease transfers and subsumption requests.
// Callers who need that property should be prepared to get an empty result
// back, meaning that the closed timestamp cannot be known.
func (r *Replica) ClosedTimestampV2() hlc.Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.closedTimestampV2RLocked()
}

func (r *Replica) closedTimestampV2RLocked() hlc.Timestamp {
	// TODO(andrei,nvanbenschoten): include sideTransportClosedTimestamp.
	return r.mu.state.RaftClosedTimestamp
}
