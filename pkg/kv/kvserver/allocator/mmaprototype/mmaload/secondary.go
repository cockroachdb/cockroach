// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaload

import "github.com/cockroachdb/redact"

// SecondaryLoadDimension represents secondary load dimensions that should be
// considered after we are done rebalancing using loadDimensions, since these
// don't represent "real" resources. Currently, only lease and replica counts
// are considered here. Lease rebalancing will see if there is scope to move
// some leases between stores that do not have any pending changes and are not
// overloaded (and will not get overloaded by the movement). This will happen
// in a separate pass (i.e., not in clusterState.rebalanceStores) -- the
// current plan is to continue using the leaseQueue and call from it into MMA.
//
// Note that lease rebalancing will only move leases and not replicas. Also,
// the rebalancing will take into account the lease preferences, as discussed
// in https://github.com/cockroachdb/cockroach/issues/93258, and the lease
// counts among the current candidates (see
// https://github.com/cockroachdb/cockroach/pull/98893).
//
// To use MMA for replica count rebalancing, done by the replicateQueue, we
// also have a ReplicaCount load dimension.
//
// These are currently unused, since the initial integration of MMA is to
// replace load-based rebalancing performed by the StoreRebalancer.
type SecondaryLoadDimension uint8

const (
	LeaseCount SecondaryLoadDimension = iota
	ReplicaCount
	NumSecondaryLoadDimensions
)

type SecondaryLoadVector [NumSecondaryLoadDimensions]LoadValue

func (lv *SecondaryLoadVector) Add(other SecondaryLoadVector) {
	for i := range other {
		(*lv)[i] += other[i]
	}
}

func (lv *SecondaryLoadVector) Subtract(other SecondaryLoadVector) {
	for i := range other {
		(*lv)[i] -= other[i]
	}
}

func (lv SecondaryLoadVector) String() string {
	return redact.StringWithoutMarkers(lv)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (lv SecondaryLoadVector) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("[lease:%d, replica:%d]", lv[LeaseCount], lv[ReplicaCount])
}
