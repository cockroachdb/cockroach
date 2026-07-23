// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

// LeaseInfo describes a range's current and potentially future lease.
type LeaseInfo struct {
	cur, next Lease
}

// MakeLeaseInfo creates a LeaseInfo with the given current and next leases.
func MakeLeaseInfo(cur, next Lease) LeaseInfo {
	return LeaseInfo{cur: cur, next: next}
}

// Current returns the range's current lease.
func (l LeaseInfo) Current() Lease {
	return l.cur
}

// CurrentOrProspective returns the range's potential next lease, if a lease
// request is in progress, or the current lease otherwise.
func (l LeaseInfo) CurrentOrProspective() Lease {
	if !l.next.Empty() {
		return l.next
	}
	return l.cur
}

// LeaseInfoOpt enumerates options for GetRangeLease.
type LeaseInfoOpt int

const (
	// AllowQueryToBeForwardedToDifferentNode specifies that, if the current node
	// doesn't have a voter replica, the lease info can come from a different
	// node.
	AllowQueryToBeForwardedToDifferentNode LeaseInfoOpt = iota
	// QueryLocalNodeOnly specifies that an error should be returned if the node
	// is not able to serve the lease query (because it doesn't have a voting
	// replica).
	QueryLocalNodeOnly
)
