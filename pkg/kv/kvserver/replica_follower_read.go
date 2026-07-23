// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/redact"
)

// canServeFollowerRead tests, when a range lease could not be acquired,
// whether the batch can be served as a follower read despite the error. Only
// non-locking, read-only requests can be served as follower reads. The batch
// must be transactional and composed exclusively of this kind of request to be
// accepted as a follower read.
func (r *Replica) canServeFollowerRead(
	ctx context.Context,
	ba *kvpb.BatchRequest,
	desc *roachpb.RangeDescriptor,
	appliedLAI kvpb.LeaseAppliedIndex,
	leaseholderNodeId roachpb.NodeID,
	raftClosed hlc.Timestamp,
) bool {
	eligible := kvpb.BatchCanBeEvaluatedOnFollower(ctx, ba) && closedts.FollowerReadsEnabled.Get(&r.store.cfg.Settings.SV)
	if !eligible {
		// We couldn't do anything with the error, propagate it.
		return false
	}

	repDesc, err := getReplicaDescriptor(desc, r.RangeID, r.StoreID())
	if err != nil {
		return false
	}

	switch repDesc.Type {
	case roachpb.VOTER_FULL, roachpb.VOTER_INCOMING, roachpb.NON_VOTER:
	default:
		log.Eventf(ctx, "%s replicas cannot serve follower reads", repDesc.Type)
		return false
	}

	requiredFrontier := ba.RequiredFrontier()
	maxClosed := r.getCurrentClosedTimestamp(ctx, requiredFrontier /* sufficient */, appliedLAI,
		leaseholderNodeId, raftClosed)
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
	log.Eventf(ctx, "%s; query timestamp below closed timestamp by %s", redact.Safe(kvbase.FollowerReadServingMsg), -tsDiff)
	r.store.metrics.FollowerReadsCount.Inc(1)
	if sp := tracing.SpanFromContext(ctx); sp.RecordingType() != tracingpb.RecordingOff {
		sp.RecordStructured(&kvpb.UsedFollowerRead{})
	}
	return true
}

// getCurrentClosedTimestampRLocked is like GetCurrentClosedTimestamp, except
// that it requires r.mu to be RLocked. It also optionally takes a hint: if
// sufficient is not empty, getClosedTimestampRLocked might return a timestamp
// that's lower than the maximum closed timestamp that we know about, as long as
// the returned timestamp is still >= sufficient. This is a performance
// optimization because we can avoid consulting the ClosedTimestampReceiver.
func (r *Replica) getCurrentClosedTimestamp(
	ctx context.Context,
	sufficient hlc.Timestamp,
	appliedLAI kvpb.LeaseAppliedIndex,
	leaseholderNodeId roachpb.NodeID,
	raftClosed hlc.Timestamp,
) hlc.Timestamp {
	sideTransportClosed := r.sideTransportClosedTimestamp.get(ctx, leaseholderNodeId,
		appliedLAI, sufficient)
	var maxClosed hlc.Timestamp
	maxClosed.Forward(raftClosed)
	maxClosed.Forward(sideTransportClosed)
	return maxClosed
}

// GetCurrentClosedTimestamp returns the current maximum closed timestamp for
// this range.
func (r *Replica) GetCurrentClosedTimestamp(ctx context.Context) hlc.Timestamp {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.getCurrentClosedTimestamp(ctx, hlc.Timestamp{}, /* sufficient */
		r.shMu.state.LeaseAppliedIndex, r.shMu.state.Lease.Replica.NodeID,
		r.shMu.state.RaftClosedTimestamp)
}
