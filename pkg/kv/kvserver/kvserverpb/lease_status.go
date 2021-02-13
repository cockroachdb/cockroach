// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
		return st.Liveness.Expiration.ToTimestamp()
	default:
		panic("unexpected")
	}
}
