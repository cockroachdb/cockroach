// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoreliveness

import (
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// StoreLiveness is a representation of the Store Liveness fabric. It provides
// information about uninterrupted periods of "support" between stores.
type StoreLiveness interface {
	// SupportFor returns the epoch of the current uninterrupted period of Store
	// Liveness support from the local store (S_local) for the store (S_remote)
	// corresponding to the specified id, and a boolean indicating whether S_local
	// supports S_remote.
	//
	// S_remote may not be aware of the full extent of support from S_local, as
	// Store Liveness heartbeat response messages may be lost or delayed. However,
	// S_local will never be unaware of support it is providing.
	//
	// If S_local cannot map the replica ID to a store ID, false will be returned.
	// It is therefore important to ensure that the replica ID to store ID mapping
	// is not lost during periods of support.
	SupportFor(id pb.PeerID) (pb.Epoch, bool)

	// SupportFrom returns the epoch of the current uninterrupted period of Store
	// Liveness support for the local store (S_local) from the store (S_remote)
	// corresponding to the specified id, and the timestamp until which the
	// support is provided (an expiration).
	//
	// Epochs returned by SupportFrom are guaranteed to be monotonically
	// increasing except after a restart, when a zero epoch and zero timestamp are
	// returned until support is established again. This is because support-from
	// state is not persisted to disk.
	//
	// A zero timestamp indicates that S_local does not have support from
	// S_remote.It is the caller's responsibility to infer whether support has
	// expired, by calling SupportExpired.
	//
	// S_local may not be aware of the full extent of support from S_remote, as
	// Store Liveness heartbeat response messages may be lost or delayed. However,
	// S_remote will never be unaware of support it is providing.
	//
	// If S_local cannot map the replica ID to a store ID, 0 and an empty
	// timestamp will be returned.
	SupportFrom(id pb.PeerID) (pb.Epoch, hlc.Timestamp)

	// SupportFromEnabled returns whether StoreLiveness is currently active, and
	// callers can rely on getting support from peers by calling SupportFrom.
	//
	// Note that even if StoreLiveness isn't currently active, replicas should not
	// break any previously made promises back when it was. As such, calls to
	// SupportFor should be made regardless of whether StoreLiveness is currently
	// active or not, which is what prompts the "SupportFrom" prefix.
	SupportFromEnabled() bool

	// SupportExpired returns whether the supplied expiration timestamp is before
	// the present time and has therefore expired. If the method returns false,
	// the timestamp is still in the future and still provides support up to that
	// point in time.
	SupportExpired(ts hlc.Timestamp) bool
}

// AlwaysLive is a mock implementation of StoreLiveness that treats all store
// to store connections as live.
type AlwaysLive struct{}

var _ StoreLiveness = AlwaysLive{}

// SupportFor implements the StoreLiveness interface.
func (AlwaysLive) SupportFor(pb.PeerID) (pb.Epoch, bool) {
	return pb.Epoch(1), true
}

// SupportFrom implements the StoreLiveness interface.
func (AlwaysLive) SupportFrom(pb.PeerID) (pb.Epoch, hlc.Timestamp) {
	return pb.Epoch(1), hlc.MaxTimestamp
}

// SupportFromEnabled implements the StoreLiveness interface.
func (AlwaysLive) SupportFromEnabled() bool {
	return true
}

// SupportExpired implements the StoreLiveness interface.
func (AlwaysLive) SupportExpired(hlc.Timestamp) bool {
	return false
}

// Disabled is a mock implementation of StoreLiveness where store liveness
// is disabled.
type Disabled struct{}

var _ StoreLiveness = Disabled{}

// SupportFor implements the StoreLiveness interface.
func (Disabled) SupportFor(pb.PeerID) (pb.Epoch, bool) {
	panic("unimplemented")
}

// SupportFrom implements the StoreLiveness interface.
func (Disabled) SupportFrom(pb.PeerID) (pb.Epoch, hlc.Timestamp) {
	panic("should not be called without checking SupportFromEnabled")
}

// SupportFromEnabled implements the StoreLiveness interface.
func (Disabled) SupportFromEnabled() bool {
	return false // disabled
}

// SupportExpired implements the StoreLiveness interface.
func (Disabled) SupportExpired(hlc.Timestamp) bool {
	return true
}
