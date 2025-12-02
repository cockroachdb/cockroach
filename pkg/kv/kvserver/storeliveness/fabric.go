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

// SupportStatus is a representation of a node's support for remote
// nodes/stores in the StoreLiveness fabrics. Conceptually, if you consider the
// StoreLiveness Fabric to be a 2D matrix of all stores in a cluster,
// SupportStatus collapses this into a 1D vector by taking the most
// conservative view of things.


// SupportStatus is an interface that can be used to query the support status of
// a remote node/store pair in the StoreLiveness fabric.
//
// Crucially, it may (but not necessarily) be used to collapse support status
// across all per-store StoreLiveness instances running on a single node. This
// is done by taking the most conservative view of things across them.
type SupportStatus interface {
	// IsSupporting returns whether the node is supporting[1] the supplied
	// remote store. Additionally, the timestamp[2] at which support was last
	// withdrawn for the store is also returned.
	//
	
	// 
	// When aggregating across all per-store StoreLiveness instances, we
	// have to contend with the fact that different StoreLiveness instances may
	// withdraw support for remote stores independently. As such:
	//
	// [1] The returned boolean corresponds to the most conservative view of
	// things, so, if any of the local stores do not support the remote store,
	// false is returned.
	//
	// [2] If multiple instances have withdrawn support for the remote store,
	// the returned timestamp corresponds to the most recent withdrawal across
	// all of them.
	IsSupporting(id slpb.StoreIdent) (bool, hlc.Timestamp)
}
