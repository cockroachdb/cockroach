// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/redact"
)

// FollowerReadsEnabled controls whether replicas attempt to serve follower
// reads. The closed timestamp machinery is unaffected by this, i.e. the same
// information is collected and passed around, regardless of the value of this
// setting.
var FollowerReadsEnabled = settings.RegisterBoolSetting(
	settings.SystemVisible, // needed for planning in SQL
	"kv.closed_timestamp.follower_reads_enabled",
	"allow (all) replicas to serve consistent historical reads based on closed timestamp information",
	true,
	settings.WithName("kv.closed_timestamp.follower_reads.enabled"),
	settings.WithPublic)

// BatchCanBeEvaluatedOnFollower determines if a batch consists exclusively of
// requests that can be evaluated on a follower replica, given a sufficiently
// advanced closed timestamp.
func BatchCanBeEvaluatedOnFollower(ctx context.Context, ba *kvpb.BatchRequest) bool {
	// Various restrictions apply to a batch for it to be successfully considered
	// for evaluation on a follower replica, which are described inline.
	//
	// The batch cannot have or intend to receive a timestamp set from a
	// server-side clock. If follower with a lagging clock sets its timestamp
	// and this then allows the follower to evaluate the batch as a follower read,
	// then the batch might miss past writes served at higher timestamps on the
	// leaseholder.
	tsFromServerClock := ba.Txn == nil && (ba.Timestamp.IsEmpty() || ba.TimestampFromServerClock != nil)
	if tsFromServerClock {
		return false
	}
	if len(ba.Requests) == 0 {
		// No requests to evaluate.
		return false
	}
	// Each request in the batch needs to have clearly defined semantics when
	// served under the closed timestamp.
	for _, ru := range ba.Requests {
		r := ru.GetInner()
		switch {
		case kvpb.IsTransactional(r):
			// Transactional requests have clear semantics when served under the
			// closed timestamp. The request must be read-only, as follower replicas
			// cannot propose writes to Raft. The request also needs to be
			// non-locking, because unreplicated locks are only held on the
			// leaseholder.
			if !kvpb.IsReadOnly(r) || kvpb.IsLocking(r) {
				return false
			}
		case r.Method() == kvpb.Export:
			// Export requests also have clear semantics when served under the closed
			// timestamp as well, even though they are non-transactional, as they
			// define the start and end timestamp to export data over.
			if r.(*kvpb.ExportRequest).ExportFingerprint {
				// Fingerprint reuses a lot of the backup code by sending export requests,
				// but unlike backup requests, doesn't have a job and multiple backup processors
				// to spread the workload around the cluster. In a 3-node cluster, the request
				// routing logic will determine that all replicas exist on the gateway node and
				// with following reads allowed, will attempt to route all the export requests to
				// the gateway node. This leads to one node doing all the work while the others sit
				// idle. Return false to prevent follower reads for fingerprinting
				return false
			}
		default:
			return false
		}
	}
	return true
}

// canServeFollowerReadRLocked tests, when a range lease could not be acquired,
// whether the batch can be served as a follower read despite the error. Only
// non-locking, read-only requests can be served as follower reads. The batch
// must be transactional and composed exclusively of this kind of request to be
// accepted as a follower read.
func (r *Replica) canServeFollowerReadRLocked(ctx context.Context, ba *kvpb.BatchRequest) bool {
	eligible := BatchCanBeEvaluatedOnFollower(ctx, ba) && FollowerReadsEnabled.Get(&r.store.cfg.Settings.SV)
	if !eligible {
		// We couldn't do anything with the error, propagate it.
		return false
	}

	repDesc, err := r.getReplicaDescriptorRLocked()
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
	maxClosed := r.getCurrentClosedTimestampLocked(ctx, requiredFrontier /* sufficient */)
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
func (r *Replica) getCurrentClosedTimestampLocked(
	ctx context.Context, sufficient hlc.Timestamp,
) hlc.Timestamp {
	appliedLAI := r.shMu.state.LeaseAppliedIndex
	leaseholder := r.shMu.state.Lease.Replica.NodeID
	raftClosed := r.shMu.state.RaftClosedTimestamp
	sideTransportClosed := r.sideTransportClosedTimestamp.get(ctx, leaseholder, appliedLAI, sufficient)

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
	return r.getCurrentClosedTimestampLocked(ctx, hlc.Timestamp{} /* sufficient */)
}
