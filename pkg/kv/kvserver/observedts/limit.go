// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package observedts

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// ComputeLocalUncertaintyLimit returns a limited version of the the
// transaction's global uncertainty limit so that it respects any timestamp
// already observed on the leaseholder's node. This prevents unnecessary
// uncertainty interval restarts caused by reading a value written at a
// timestamp between txn.ReadTimestamp and txn.GlobalUncertaintyLimit. The
// returned local uncertainty limit will either be empty (if observed
// timestamps could not be used) or will be a clock timestamp.
//
// The lease's start time is also taken into consideration to ensure that a
// lease transfer does not result in the observed timestamp for this node being
// inapplicable to data previously written by the former leaseholder. To wit:
//
//  1. put(k on leaseholder n1), gateway chooses t=1.0
//  2. begin; read(unrelated key on n2); gateway chooses t=0.98
//  3. pick up observed timestamp for n2 of t=0.99
//  4. n1 transfers lease for range with k to n2 @ t=1.1
//  5. read(k) on leaseholder n2 at ReadTimestamp=0.98 should get
//     ReadWithinUncertaintyInterval because of the write in step 1, so
//     even though we observed n2's timestamp in step 3 we must expand
//     the uncertainty interval to the lease's start time, which is
//     guaranteed to be greater than any write which occurred under
//     the previous leaseholder.
//
func ComputeLocalUncertaintyLimit(
	txn *roachpb.Transaction, status kvserverpb.LeaseStatus,
) hlc.Timestamp {
	if txn == nil {
		return hlc.Timestamp{}
	}
	if status.State != kvserverpb.LeaseState_VALID {
		return hlc.Timestamp{}
	}

	// For calls that read data within a txn, we keep track of timestamps
	// observed from the various participating nodes' HLC clocks. If we have a
	// timestamp on file for the leaseholder's node which is smaller than
	// GlobalUncertaintyLimit, we can lower localUncertaintyLimit accordingly.
	// If GlobalUncertaintyLimit drops below ReadTimestamp, we effectively can't
	// see uncertainty restarts anymore.
	//
	// Note that we care about an observed timestamp from the leaseholder's
	// node, even if this is a follower read on a different node. See the
	// comment in doc.go about "Follower Reads" for more.
	obsTs, ok := txn.GetObservedTimestamp(status.Lease.Replica.NodeID)
	if !ok {
		return hlc.Timestamp{}
	}
	// If the lease is valid, we use the greater of the observed timestamp and
	// the lease start time. This ensures we avoid incorrect assumptions about
	// when data was written, in absolute time on a different node, which held
	// the lease before this replica acquired it.
	obsTs.Forward(status.Lease.Start)

	localUncertaintyLimit := txn.GlobalUncertaintyLimit
	localUncertaintyLimit.Backward(obsTs.ToTimestamp())
	return localUncertaintyLimit
}
