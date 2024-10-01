// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package uncertainty

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
// There is another case that impacts the use of the transactions observed
// timestamp.
//
// If both these conditions hold:
//   - A transaction already has an observed timestamp value for a node
//   - That node was not the leaseholder for some or all of the range as of the
//     time of the observed timestamp, but it is now.
//
// Then the transaction's observed timestamp is not (entirely) respected when
// computing a local uncertainty limit.
//
// As background, for efficiency reasons, observed timestamp tracking is done at
// a node level, but more precisely it refers to the ranges that node is the
// leaseholder for. This discrepancy is accounted for by the
// minValidObservedTimestamp parameter, and two specific cases are covered in
// more detail below.
//
// Here is the hazard that can occur without this field for a lease transfer.
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
// A similar hazard applies to range merges.
//
//  1. put(k2 on n2, r2); gateway chooses t=1.0
//  2. begin; read(k on n1, r1); gateway chooses t=0.98
//  3. pick up observed timestamp for n1 of t=0.99
//  4. r1 merged right-hand neighbor r2 @ t=1.1
//  5. read(k2) on joint range at ReadTimestamp=0.98 should get
//     ReadWithinUncertaintyInterval because of the write in step 1, so
//     even though we observed n1's timestamp in step 3 we must expand
//     the uncertainty interval to the range merge freeze time, which
//     is guaranteed to be greater than any write which occurred on the
//     right-hand side.
func ComputeInterval(
	h *kvpb.Header, status kvserverpb.LeaseStatus, maxOffset time.Duration,
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

	// Adjust the uncertainty interval to account for lease changes or merges.
	// See the comment on ComputeInterval for an explanation of cases where observed
	// timestamps captured on the current leaseholder's node are not applicable to
	// data written by prior leaseholders.
	in.LocalLimit.Forward(status.MinValidObservedTimestamp)

	// The local uncertainty limit should always be <= the global uncertainty
	// limit.
	in.LocalLimit.BackwardWithTimestamp(in.GlobalLimit)
	return in
}

func computeIntervalForNonTxn(
	h *kvpb.Header, status kvserverpb.LeaseStatus, maxOffset time.Duration,
) Interval {
	if h.TimestampFromServerClock == nil || h.ReadConsistency != kvpb.CONSISTENT {
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

	// Adjust the uncertainty interval to account for lease changes or merges.
	// See the comment on ComputeInterval for an explanation of cases where observed
	// timestamps captured on the current leaseholder's node are not applicable to
	// data written by prior leaseholders.
	in.LocalLimit.Forward(status.MinValidObservedTimestamp)

	// The local uncertainty limit should always be <= the global uncertainty
	// limit.
	in.LocalLimit.BackwardWithTimestamp(in.GlobalLimit)
	return in
}
