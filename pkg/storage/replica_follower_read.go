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
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// FollowerReadsEnabled controls whether replicas attempt to serve follower
// reads. The closed timestamp machinery is unaffected by this, i.e. the same
// information is collected and passed around, regardless of the value of this
// setting.
var FollowerReadsEnabled = settings.RegisterBoolSetting(
	"kv.closed_timestamp.follower_reads_enabled",
	"allow (all) replicas to serve consistent historical reads based on closed timestamp information",
	false,
)

// canServeFollowerRead tests, when a range lease could not be
// acquired, whether the read only batch can be served as a follower
// read despite the error.
func (r *Replica) canServeFollowerRead(
	ctx context.Context, ba roachpb.BatchRequest, pErr *roachpb.Error,
) *roachpb.Error {
	canServeFollowerRead := false
	if lErr, ok := pErr.GetDetail().(*roachpb.NotLeaseHolderError); ok &&
		FollowerReadsEnabled.Get(&r.store.cfg.Settings.SV) &&
		lErr.LeaseHolder != nil && lErr.Lease.Type() == roachpb.LeaseEpoch {

		r.mu.RLock()
		lai := r.mu.state.LeaseAppliedIndex
		r.mu.RUnlock()
		canServeFollowerRead = r.store.cfg.ClosedTimestamp.Provider.CanServe(
			lErr.LeaseHolder.NodeID, ba.Timestamp, r.RangeID, ctpb.Epoch(lErr.Lease.Epoch), ctpb.LAI(lai),
		)

		if !canServeFollowerRead {
			// We can't actually serve the read. Signal the clients that we want
			// an update so that future requests can succeed.
			r.store.cfg.ClosedTimestamp.Clients.Request(lErr.LeaseHolder.NodeID, r.RangeID)

			if false {
				// NB: this can't go behind V(x) because the log message created by the
				// storage might be gigantic in real clusters, and we don't want to trip it
				// using logspy.
				log.Warningf(ctx, "can't serve follower read for %s at epo %d lai %d, storage is %s",
					ba.Timestamp, lErr.Lease.Epoch, lai,
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
