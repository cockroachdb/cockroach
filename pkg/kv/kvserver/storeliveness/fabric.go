// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storeliveness

import (
	"context"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Fabric is a representation of the Store Liveness fabric. It provides
// information about uninterrupted periods of "support" between stores.
type Fabric interface {
	SupportStatus
	InspectFabric

	// SupportFor returns the epoch of the current uninterrupted period of Store
	// Liveness support from the local store (S_local) for the store (S_remote)
	// corresponding to the specified id, and a boolean indicating whether S_local
	// supports S_remote. Epochs returned by successive SupportFor calls are
	// guaranteed to be monotonically increasing.
	//
	// S_remote may not be aware of the full extent of support from S_local, as
	// Store Liveness heartbeat response messages may be lost or delayed. However,
	// S_local will never be unaware of support it is providing.
	//
	// If S_local is unaware of the remote store S_remote, false will be returned.
	SupportFor(id slpb.StoreIdent) (slpb.Epoch, bool)

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
	// S_remote. It is the caller's responsibility to infer whether support has
	// expired, by comparing the returned timestamp to its local clock.
	//
	// S_local may not be aware of the full extent of support from S_remote, as
	// Store Liveness heartbeat response messages may be lost or delayed. However,
	// S_remote will never be unaware of support it is providing.
	//
	// If S_local is unaware of the remote store S_remote, it will initiate a
	// heartbeat loop to S_remote in order to request support.
	SupportFrom(id slpb.StoreIdent) (slpb.Epoch, hlc.Timestamp)

	// SupportFromEnabled determines if Store Liveness requests support from
	// other stores. If it returns true, then Store Liveness is sending
	// heartbeats and responding to heartbeats. If it returns false, Store
	// Liveness is not sending heartbeats but is still responding to heartbeats
	// to ensure any promise by the local store to provide support is still kept.
	SupportFromEnabled(ctx context.Context) bool

	// RegisterSupportWithdrawalCallback allows the local store to register a
	// callback which will be invoked every time the local store withdraws support
	// from any remote stores. The callback is used to wake up a potentially
	// asleep replica when it no longer considers its leader alive (in store
	// liveness). The replica may need to help elect a new leader.
	//
	// Returns the set of stores for which support was withdrawn.
	RegisterSupportWithdrawalCallback(func(storeIDs map[roachpb.StoreID]struct{}))
}

// InspectFabric is an interface that exposes all in-memory support state for a
// given store. It is used to power the Store Liveness /inspectz functionality.
type InspectFabric interface {
	InspectSupportFrom() slpb.InspectSupportFromStatesPerStore
	InspectSupportFor() slpb.InspectSupportForStatesPerStore
}

// SupportState represents the support state of a remote store.
type SupportState int

const (
	// StateUnknown indicates that there has been no interaction with the remote
	// store. Support was never provided, and therefore never withdrawn.
	StateUnknown SupportState = iota
	// StateSupporting indicates that support is currently being provided to the
	// remote store.
	StateSupporting
	// StateNotSupporting indicates that support has been withdrawn from the
	// remote store.
	StateNotSupporting
)

// combine combines two SupportState values, updating the receiver based on the
// precedence order:
//   - StateUnknown doesn't change the combined state.
//   - StateSupporting takes precedence over StateUnknown, that's all.
//   - StateNotSupporting takes precedence over all other states.
//
// This may be used to aggregate the support state across all SupportManagers
// running on a single node.
func (s *SupportState) combine(o SupportState) {
	switch o {
	case StateUnknown:
		// StateUnknown doesn't change the combined state.
	case StateNotSupporting:
		// StateNotSupporting takes precedence over all other states.
		*s = StateNotSupporting
	case StateSupporting:
		// StateSupporting takes precedence over StateUnknown, that's all.
		if *s == StateUnknown {
			*s = StateSupporting
		}
	default:
		panic("invalid state")
	}
}

// SupportStatus is an interface that can be used to query the support state of
// a remote node/store pair in the StoreLiveness fabric.
//
// Crucially, it may (but not necessarily) be used to collapse support state
// across all per-store StoreLiveness instances running on a single node. This
// is done by taking the most conservative view of things across them.
type SupportStatus interface {
	// SupportState returns the support state[1] for the supplied remote store.
	// Additionally, the timestamp[2] at which support was last withdrawn for
	// the store is also returned, if applicable[3].
	//
	// When aggregating across all per-store StoreLiveness instances, we have to
	// contend with the fact that different StoreLiveness instances may withdraw
	// support for remote stores independently. Moreover, some StoreLiveness
	// intances may have never interacted with the remote store. As such:
	//
	// [1] The returned SupportState corresponds to the most conservative view
	// of things. If any of the local stores do not support the remote store,
	// StateNotSupporting is returned. If all local stores support the remote
	// store, StateSupporting is returned.
	//
	// [2] If multiple instances have withdrawn support for the remote store,
	// the returned timestamp corresponds to the most recent withdrawal across
	// all of them.
	//
	// [3] If a store never provided support for a remote store, it is excluded
	// from both the calculations above.
	SupportState(id slpb.StoreIdent) (SupportState, hlc.ClockTimestamp)
}
