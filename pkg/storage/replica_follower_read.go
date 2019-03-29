// Copyright 2019 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts/ctpb"
	ctstorage "github.com/cockroachdb/cockroach/pkg/storage/closedts/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// FollowerReadsEnabled controls whether replicas attempt to serve follower
// reads. The closed timestamp machinery is unaffected by this, i.e. the same
// information is collected and passed around, regardless of the value of this
// setting.
var FollowerReadsEnabled = settings.RegisterBoolSetting(
	"kv.closed_timestamp.follower_reads_enabled",
	"allow (all) replicas to serve consistent historical reads based on closed timestamp information",
	true,
)

// canServeFollowerRead tests, when a range lease could not be
// acquired, whether the read only batch can be served as a follower
// read despite the error.
func (r *Replica) canServeFollowerRead(
	ctx context.Context, ba roachpb.BatchRequest, pErr *roachpb.Error,
) *roachpb.Error {
	canServeFollowerRead := false
	if lErr, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); ok &&
		lErr.LeaseHolder != nil && lErr.Lease.Type() == roachpb.LeaseEpoch &&
		ba.IsAllTransactional() && // followerreadsccl.batchCanBeEvaluatedOnFollower
		(ba.Txn == nil || !ba.Txn.IsWriting()) && // followerreadsccl.txnCanPerformFollowerRead
		FollowerReadsEnabled.Get(&r.store.cfg.Settings.SV) {

		ts := ba.Timestamp
		if ba.Txn != nil {
			ts.Forward(ba.Txn.MaxTimestamp)
		}

		canServeFollowerRead = !r.maxClosed(ctx).Less(ts)
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
	return nil
}

// maxClosed returns the maximum closed timestamp for this range.
// It is computed as the most recent of the known closed timestamp for the
// current lease holder for this range as tracked by the closed timestamp
// subsystem and the start time of the current lease. It is safe to use the
// start time of the current lease because leasePostApply bumps the timestamp
// cache forward to at least the new lease start time. Using this combination
// allows the closed timestamp mechanism to be robust to lease transfers.
func (r *Replica) maxClosed(ctx context.Context) hlc.Timestamp {
	r.mu.RLock()
	lai := r.mu.state.LeaseAppliedIndex
	lease := *r.mu.state.Lease
	initialMaxClosed := r.mu.initialMaxClosed
	r.mu.RUnlock()
	maxClosed := r.store.cfg.ClosedTimestamp.Provider.MaxClosed(
		lease.Replica.NodeID, r.RangeID, ctpb.Epoch(lease.Epoch), ctpb.LAI(lai))
	maxClosed.Forward(lease.Start)
	maxClosed.Forward(initialMaxClosed)
	return maxClosed
}
