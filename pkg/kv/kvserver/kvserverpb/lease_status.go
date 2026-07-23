// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserverpb

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// IsValid returns whether the lease was valid at the time that the
// lease status was computed.
func (st LeaseStatus) IsValid() bool {
	return st.State == LeaseState_VALID
}

// IsExpired returns whether the lease was expired at the time that the
// lease status was computed.
func (st LeaseStatus) IsExpired() bool {
	return st.State == LeaseState_EXPIRED
}

// OwnedBy returns whether the lease is owned by the given store.
func (st LeaseStatus) OwnedBy(storeID roachpb.StoreID) bool {
	return st.Lease.OwnedBy(storeID)
}

// Expiration returns the expiration of the lease.
func (st LeaseStatus) Expiration() hlc.Timestamp {
	switch st.Lease.Type() {
	case roachpb.LeaseExpiration:
		return st.Lease.GetExpiration()
	case roachpb.LeaseEpoch:
		exp := st.Lease.MinExpiration
		// The expiration of the liveness record is inherited by the lease iff the
		// lease epoch matches the liveness epoch.
		if st.Lease.Epoch == st.Liveness.Epoch {
			exp.Forward(st.Liveness.Expiration.ToTimestamp())
		}
		return exp
	case roachpb.LeaseLeader:
		exp := st.Lease.MinExpiration
		// The leader support applies to the lease iff the lease term matches the
		// raft term.
		if st.Lease.Term == st.LeaderSupport.Term {
			exp.Forward(st.LeaderSupport.LeadSupportUntil)
		}
		return exp
	default:
		panic("unexpected")
	}
}

// ClosedTimestampUpperBound represents the highest timestamp that can be closed
// under this lease.
//
// We can't close timestamps above the current lease's expiration. This is in
// order to keep the monotonic property of closed timestamps carried by
// commands, which makes for straight-forward closed timestamp management on the
// command application side: if we allowed requests to close timestamps above
// the lease's expiration, then a future LeaseRequest proposed by another node
// might carry a lower closed timestamp (i.e. the lease start time).
//
// Note that the stasis period doesn't affect closed timestamps; a request is
// only server under a particular closed timestamp if its read frontier is below
// the closed timestamp.
//
// We don't want to close timestamps within the same nanosecond as the lease
// expiration. If we did that, writes could not be processed under the current
// lease any more after such a close, and that would be a bit tricky to handle,
// particularly when the close time is in the future. To avoid having to deal
// with it, we leave one nanosecond (i.e. infinite logical time) in the lease.
// Until a new lease is acquired, all writes will be pushed into this last
// nanosecond of the lease.
func (st LeaseStatus) ClosedTimestampUpperBound() hlc.Timestamp {
	return st.Expiration().WallPrev()
}
