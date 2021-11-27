// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package uncertainty

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ComputeInterval returns the provided transaction's uncertainty interval to be
// used when evaluating requests under the specified lease.
//
// The computation uses observed timestamps gathered from the leaseholder's node
// to limit the interval's local uncertainty limit. This prevents unnecessary
// uncertainty restarts caused by reading a value written at a timestamp between
// txn.ReadTimestamp and txn.GlobalUncertaintyLimit.
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
// A similar hazard applies to range merges, where we cannot apply observed
// timestamps captured from the leaseholder of the left-hand side of the merge
// to data written on the right-hand side of the merge, even after the merge has
// completed. To wit:
//
// 1. put(k2 on n2, r2); gateway chooses t=1.0
// 2. begin; read(k on n1, r1); gateway chooses t=0.98
// 3. pick up observed timestamp for n1 of t=0.99
// 4. r1 merged right-hand neighbor r2 @ t=1.1
// 5. read(k2) on joint range at ReadTimestamp=0.98 should get
//    ReadWithinUncertaintyInterval because of the write in step 1, so
//    even though we observed n1's timestamp in step 3 we must expand
//    the uncertainty interval to the range merge's freeze time, which
//    is guaranteed to be greater than any write which occurred on the
//    right-hand side.
// TODO(nvanbenschoten): fix this bug with range merges.
//
func ComputeInterval(txn *roachpb.Transaction, status kvserverpb.LeaseStatus) Interval {
	// Non-transactional requests do not have uncertainty intervals.
	// TODO(nvanbenschoten): Yet, they should. Fix this.
	//  See https://github.com/cockroachdb/cockroach/issues/58459.
	if txn == nil {
		return Interval{}
	}

	in := Interval{GlobalLimit: txn.GlobalUncertaintyLimit}
	if status.State != kvserverpb.LeaseState_VALID {
		return in
	}

	// For calls that read data within a txn, we keep track of timestamps observed
	// from the various participating nodes' HLC clocks. If we have a timestamp on
	// file for the leaseholder's node which is smaller than GlobalLimit, we can
	// lower LocalLimit accordingly. If GlobalLimit drops below ReadTimestamp, we
	// effectively can't see uncertainty restarts anymore.
	//
	// Note that we care about an observed timestamp from the leaseholder's node,
	// even if this is a follower read on a different node. See the comment in
	// doc.go about "Follower Reads" for more.
	obsTs, ok := txn.GetObservedTimestamp(status.Lease.Replica.NodeID)
	if !ok {
		return in
	}
	in.LocalLimit = obsTs

	// If the lease is valid, we use the greater of the observed timestamp and
	// the lease start time. This ensures we avoid incorrect assumptions about
	// when data was written, in absolute time on a different node, which held
	// the lease before this replica acquired it.
	in.LocalLimit.Forward(status.Lease.Start)

	// The local uncertainty limit should always be <= the global uncertainty
	// limit.
	in.LocalLimit.BackwardWithTimestamp(in.GlobalLimit)
	return in
}
