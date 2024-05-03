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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/dme_liveness/dme_livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

type SupportStatus int8

const (
	CurrentlyUnsupported SupportStatus = iota
	SupportWithdrawn
	Supported
)

func (ss SupportStatus) String() string {
	switch ss {
	case Supported:
		return "supported"
	case SupportWithdrawn:
		return "support-withdrawn"
	case CurrentlyUnsupported:
		return "currently-unsupported"
	default:
		panic(errors.AssertionFailedf("unknown SupportStatus %d", ss))
	}
}

// RangeLeaseSupportProvider is the interface implemented by the distributed
// multi-epoch (DME) liveness sub-system, for supporting dme-based leases etc.
type RangeLeaseSupportProvider interface {
	// EpochAndSupportForLeaseProposal checks if there is support for start
	// time, for storeID, given the voting replicas in descriptor (which is the
	// latest known committed descriptor). If there isn't support, an error is
	// returned. If there is support, the epoch to use for the lease is
	// returned. The storeID may not be local to this node, in case of a lease
	// transfer (though if we always switch to expiration-based leases on
	// transfer, it will be local).
	//
	// The implementation does not need to maintain history, so it is possible
	// that a slow goroutine calling this method, where start has become
	// slightly stale, sees a state reflecting t > start. The caller will sample
	// the clock again, and return an adjusted start.
	//
	// NB: the epoch could be slightly stale when it is a lease transfer, since
	// this node may not have information about the latest epoch at another
	// node. We can't help this staleness, and it does not affect safety.
	EpochAndSupportForLeaseProposal(
		start hlc.ClockTimestamp, storeID dme_livenesspb.StoreIdentifier, descriptor roachpb.RangeDescriptor,
	) (adjustedStart hlc.ClockTimestamp, epoch int64, err error)

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
	//
	// The implementation does not need to maintain history, so it is possible
	// that a slow goroutine calling this method, where now has become slightly
	// stale, sees a state reflecting t > now. When the return value is
	// Supported, this is safe. When the return value is SupportWithdrawn, the
	// caller, if it is different than the leaseholder, must sample the clock
	// again when proposing a lease (it cannot use now as the start time of the
	// new lease).
	//
	// TODO(sumeer): find the place where we sample the clock again --
	// apparently this already happens in the current code.
	HasSupport(
		now hlc.ClockTimestamp, storeID dme_livenesspb.StoreIdentifier, epoch int64,
		descriptors []roachpb.RangeDescriptor,
	) (support SupportStatus, expiration hlc.Timestamp, err error)
}

type LocalStore struct {
	roachpb.StoreID
	StorageProvider
	MessageSender
}

type Options struct {
	roachpb.NodeID
	*hlc.Clock
	// Allowed to change (since backed by cluster setting).
	LivenessExpiryInterval func() time.Duration
	MetricRegistry         *metric.Registry

	callbackSchedulerForTesting callbackScheduler
}

// TODO: update comment. SendHeartbeat can be retried for the same to, until
// superceded by a different SendHeartbeat.

// MessageSender abstracts sending messages on behalf of a particular
// local store.
//
// All message are one-way.
//
// The MessageSender may repeat a heartbeat message until it is certain that
// the receiver has received the message. It may do backoff if it suspects the
// receiver is down. It should not keep repeating a stale heartbeat after
// SendHeartbeat has been called again.
//
// Messages can be delivered out of order, and it is the receiver's
// responsibility to ignore stale messages.
//
// None of these have error return values since transient errors need to be
// handled internally. If a store is known by the MessageSender to be
// permanently removed, it can drop the messages (it is possible that
// Manager.RemoveRemoteStore has not yet been called, which is ok).
type MessageSender interface {
	SetSupportStateProvider(ssProvider SupportStateForPropagationProvider)
	SetHandleHeartbeatResponseInterface(hhri HandleHeartbeatResponseInterface)

	SendHeartbeat(header dme_livenesspb.Header, msg dme_livenesspb.Heartbeat)

	PropagateLocalSupportStateSoon()
	// AddRemoteStore and RemoveRemoteStore tell the MessageSender which stores
	// to propagate SupportState too.
	AddRemoteStore(store dme_livenesspb.StoreIdentifier)
	RemoveRemoteStore(store dme_livenesspb.StoreIdentifier)
}

type SupportStateForPropagationProvider interface {
	LocalSupportState() dme_livenesspb.SupportState
}

// StorageProvider is an interface to write to and retrieve persistent state
// for distributed multi-epoch liveness in a local store. It contains
// information about support this local store has from other stores, and the
// support this local store is giving to other stores.
type StorageProvider interface {
	// ReadCurrentEpoch returns the current epoch for this local store. Used at startup. If no
	// value is found, 0 is returned.
	ReadCurrentEpoch() (int64, error)
	// UpdateCurrentEpoch persists a new epoch for this local store.
	UpdateCurrentEpoch(epoch int64) error
	// ReadSupportBySelfFor iterates through all BySelfFor persistent support state. Used at startup.
	ReadSupportBySelfFor(
		f func(store dme_livenesspb.StoreIdentifier, support dme_livenesspb.Support)) error
	// UpdateSupportBySelfFor updates the support state this store gives to another store.
	UpdateSupportBySelfFor(store dme_livenesspb.StoreIdentifier, support dme_livenesspb.Support) error
}

type Manager interface {
	AddLocalStore(store LocalStore) error

	AddRemoteStore(storeID dme_livenesspb.StoreIdentifier)
	RemoveRemoteStore(storeID dme_livenesspb.StoreIdentifier)

	HandleHeartbeat(header dme_livenesspb.Header, msg dme_livenesspb.Heartbeat) (ack bool, err error)
	HandleSupportState(header dme_livenesspb.Header, msg dme_livenesspb.SupportState) error

	RangeLeaseSupportProvider

	Close()
}

type HandleHeartbeatResponseInterface interface {
	HandleHeartbeatResponse(
		from dme_livenesspb.StoreIdentifier, msg dme_livenesspb.Heartbeat,
		responderTime hlc.ClockTimestamp, ack bool) error
}
