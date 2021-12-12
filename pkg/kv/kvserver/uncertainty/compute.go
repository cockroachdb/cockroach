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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// ComputeInterval returns the provided request's uncertainty interval to be
// used when evaluating under the specified lease.
//
// If the function returns an empty Interval{} then the request should bypass
// all uncertainty checks.
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
func ComputeInterval(
	h *roachpb.Header, status kvserverpb.LeaseStatus, maxOffset time.Duration,
) Interval {
	if h.Txn != nil {
		return computeIntervalForTxn(h.Txn, status)
	}
	return computeIntervalForNonTxn(h, status, maxOffset)
}

func computeIntervalForTxn(txn *roachpb.Transaction, status kvserverpb.LeaseStatus) Interval {
	in := Interval{
		// The transaction's global uncertainty limit is computed by its coordinator
		// when the transaction is initiated. It stays constant across all requests
		// issued by the transaction and across all retries.
		GlobalLimit: txn.GlobalUncertaintyLimit,
	}

	if status.State != kvserverpb.LeaseState_VALID {
		// If the lease is invalid, this must be a follower read. In such cases, we
		// must use the most pessimistic uncertainty limit.
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

	// Adjust the uncertainty interval for the lease it is being used under.
	in.LocalLimit.Forward(minimumLocalLimitForLeaseholder(status.Lease))

	// The local uncertainty limit should always be <= the global uncertainty
	// limit.
	in.LocalLimit.BackwardWithTimestamp(in.GlobalLimit)
	return in
}

func computeIntervalForNonTxn(
	h *roachpb.Header, status kvserverpb.LeaseStatus, maxOffset time.Duration,
) Interval {
	if h.TimestampFromServerClock == nil || h.ReadConsistency != roachpb.CONSISTENT {
		// Non-transactional requests with client-provided timestamps do not
		// guarantee linearizability. Neither do entirely inconsistent requests.
		// As a result, they do not have uncertainty intervals.
		return Interval{}
	}

	// Non-transactional requests that defer their timestamp allocation to the
	// leaseholder of their (single) range do have uncertainty intervals. As a
	// result, they do guarantee linearizability.
	in := Interval{
		// Even though the non-transactional request received its timestamp from the
		// leaseholder of its range, it can still observe writes that were performed
		// before it in real-time that have MVCC timestamps above its timestamp. In
		// these cases, it needs to perform an uncertainty restart.
		//
		// For example, the non-transactional request may observe an intent with a
		// provisional timestamp below its server-assigned timestamp. It will begin
		// waiting on this intent. It is possible for the intent to then be resolved
		// (asynchronously with respect to the intent's txn commit) with a timestamp
		// above its server-assigned timestamp. To guarantee linearizability, the
		// non-transactional request must observe the effect of the intent write, so
		// it must perform a (server-side) uncertainty restart to a timestamp above
		// the now-resolved write.
		//
		// See the comment on D7 in doc.go for an example.
		GlobalLimit: h.TimestampFromServerClock.ToTimestamp().Add(maxOffset.Nanoseconds(), 0),
	}

	if status.State != kvserverpb.LeaseState_VALID {
		// If the lease is invalid, this is either a lease request or we are computing
		// the request's uncertainty interval before grabbing latches and checking for
		// the current lease. Either way, return without a local limit.
		return in
	}

	// The request's timestamp was selected on this server, so it can serve the
	// role of an observed timestamp and as the local uncertainty limit.
	in.LocalLimit = *h.TimestampFromServerClock

	// Adjust the uncertainty interval for the lease it is being used under.
	in.LocalLimit.Forward(minimumLocalLimitForLeaseholder(status.Lease))

	// The local uncertainty limit should always be <= the global uncertainty
	// limit.
	in.LocalLimit.BackwardWithTimestamp(in.GlobalLimit)
	return in
}

// minimumLocalLimitForLeaseholder returns the minimum timestamp that can be
// used as a local limit when evaluating a request under the specified lease.
// See the comment on ComputeInterval for an explanation of cases where observed
// timestamps captured on the current leaseholder's node are not applicable to
// data written by prior leaseholders (either before a lease change or before a
// range merge).
func minimumLocalLimitForLeaseholder(lease roachpb.Lease) hlc.ClockTimestamp {
	// If the lease is valid, we use the greater of the observed timestamp and
	// the lease start time. This ensures we avoid incorrect assumptions about
	// when data was written, in absolute time on a different node, which held
	// the lease before this replica acquired it.
	min := lease.Start

	// TODO(nvanbenschoten): handle RHS freeze timestamp after merge here when
	// we fix https://github.com/cockroachdb/cockroach/issues/73292.

	return min
}
