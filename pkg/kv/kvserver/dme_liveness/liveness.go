// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package dme_liveness

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type SupportState int8

const (
	Supported SupportState = iota
	SupportWithdrawn
	CurrentlyUnsupported
)

// RangeLeaseSupportProvider is the interface implemented by the distributed
// multi-epoch (DME) liveness sub-system, for supporting dme-based leases etc.
type RangeLeaseSupportProvider interface {
	// EpochAndSupportForLeaseProposal checks if there is support for start
	// time, for storeID, given the voting replicas in descriptor (which is the latest
	// known committed descriptor). If there
	// isn't support, an error is returned. If there is support, the epoch to
	// use for the lease is returned. The storeID may not be local to this node,
	// in case of a lease transfer (though if we always switch to
	// expiration-based leases on transfer, it will be local).
	EpochAndSupportForLeaseProposal(
		start hlc.ClockTimestamp, storeID StoreIdentifier, descriptor roachpb.RangeDescriptor,
	) (epoch int64, err error)

	// TODO(sumeer): remember that the implementation needs to use information
	// provided to alter its state:
	// - bump up the epoch for a remote unsupported store based on the epoch
	//   parameter in HasSupport.
	//
	// - bump up the a local store epoch for a remote store if the lease epoch
	//   does not have quorum, and support has not been explicitly withdrawn.
	//
	// TODO(sumeer): evaluating validity from scratch is not going to be very
	// efficient, so will probably need to cache. Caching the current end time
	// when a lease is Supported will make the common case very efficient. And
	// caching a SupportWithdrawn lease is also easy since there is no cache
	// invalidation needed. The CurrentlyUnsupported is the hard case, since
	// messages received from other nodes can change the state -- we need to be
	// very efficient for this (some staleness is tolerable, but for liveness,
	// it should not be more than a few milliseconds).

	// HasSupport evaluates whether the given storeID has support at now for the
	// given epoch, assuming descriptors are the set of descriptors (there will
	// be at most two descriptors, the applied one, and a pending one) for the
	// range. If support is equal to Supported, expiration provides the time up
	// to which the state is guaranteed to stay Supported, assuming no more
	// descriptor changes.
	HasSupport(now hlc.ClockTimestamp, storeID StoreIdentifier, epoch int64, descriptors []roachpb.RangeDescriptor,
	) (support SupportState, expiration hlc.Timestamp, err error)
}

type StoreIdentifier struct {
	roachpb.NodeID
	roachpb.StoreID
}

// TODO(sumeer): remove this hack once we have an actual implementation.
var RangeLeaseSupportProviderSingleton RangeLeaseSupportProvider
