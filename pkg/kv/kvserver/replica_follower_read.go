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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	ctstorage "github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/storage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// FollowerReadsEnabled controls whether replicas attempt to serve follower
// reads. The closed timestamp machinery is unaffected by this, i.e. the same
// information is collected and passed around, regardless of the value of this
// setting.
var FollowerReadsEnabled = settings.RegisterPublicBoolSetting(
	"kv.closed_timestamp.follower_reads_enabled",
	"allow (all) replicas to serve consistent historical reads based on closed timestamp information",
	true,
)

// canServeFollowerRead tests, when a range lease could not be acquired, whether
// the batch can be served as a follower read despite the error. Only
// non-locking, read-only requests can be served as follower reads. The batch
// must be composed exclusively only this kind of request to be accepted as a
// follower read.
func (r *Replica) canServeFollowerRead(
	ctx context.Context, ba *roachpb.BatchRequest, pErr *roachpb.Error,
) *roachpb.Error {
	canServeFollowerRead := false
	if lErr, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); ok &&
		lErr.LeaseHolder != nil && lErr.Lease.Type() == roachpb.LeaseEpoch &&
		(!ba.IsLocking() && ba.IsAllTransactional()) && // followerreadsccl.batchCanBeEvaluatedOnFollower
		(ba.Txn == nil || !ba.Txn.IsLocking()) && // followerreadsccl.txnCanPerformFollowerRead
		FollowerReadsEnabled.Get(&r.store.cfg.Settings.SV) {

		// There's no known reason that a non-VOTER_FULL replica couldn't serve follower
		// reads (or RangeFeed), but as of the time of writing, these are expected
		// to be short-lived, so it's not worth working out the edge-cases. Revisit if
		// we add long-lived learners or feel that incoming/outgoing voters also need
		// to be able to serve follower reads.
		repDesc, err := r.GetReplicaDescriptor()
		if err != nil {
			return roachpb.NewError(err)
		}
		if typ := repDesc.GetType(); typ != roachpb.VOTER_FULL {
			log.Eventf(ctx, "%s replicas cannot serve follower reads", typ)
			return pErr
		}

		ts := ba.Timestamp
		if ba.Txn != nil {
			ts.Forward(ba.Txn.MaxTimestamp)
		}

		maxClosed, _ := r.maxClosed(ctx)
		canServeFollowerRead = ts.LessEq(maxClosed)
		if !canServeFollowerRead {
			// We can't actually serve the read based on the closed timestamp.
			// Signal the clients that we want an update so that future requests can succeed.
			r.store.cfg.ClosedTimestamp.Clients.Request(lErr.LeaseHolder.NodeID, r.RangeID)

			if false {
				// NB: this can't go behind V(x) because the log message created by the
				// storage might be gigantic in real clusters, and we don't want to trip it
				// using logspy.
				log.Warningf(ctx, "can't serve follower read for %s at epo %d, storage is %s",
					ba.Timestamp, lErr.Lease.Epoch,
					r.store.cfg.ClosedTimestamp.Storage.(*ctstorage.MultiStorage).StringForNodes(lErr.LeaseHolder.NodeID),
				)
			}
		}
	}

	if !canServeFollowerRead {
		// We couldn't do anything with the error, propagate it.
		return pErr
	}

	// This replica can serve this read!
	//
	// TODO(tschottdorf): once a read for a timestamp T has been served, the replica may
	// serve reads for that and smaller timestamps forever.
	log.Event(ctx, "serving via follower read")
	r.store.metrics.FollowerReadsCount.Inc(1)
	return nil
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
func (r *Replica) maxClosed(ctx context.Context) (_ hlc.Timestamp, ok bool) {
	r.mu.RLock()
	lai := r.mu.state.LeaseAppliedIndex
	lease := *r.mu.state.Lease
	initialMaxClosed := r.mu.initialMaxClosed
	r.mu.RUnlock()
	if lease.Expiration != nil {
		return hlc.Timestamp{}, false
	}
	maxClosed := r.store.cfg.ClosedTimestamp.Provider.MaxClosed(
		lease.Replica.NodeID, r.RangeID, ctpb.Epoch(lease.Epoch), ctpb.LAI(lai))
	maxClosed.Forward(lease.Start)
	maxClosed.Forward(initialMaxClosed)
	return maxClosed, true
}
