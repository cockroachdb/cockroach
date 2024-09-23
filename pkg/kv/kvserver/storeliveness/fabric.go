// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storeliveness

import (
	"context"

	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Fabric is a representation of the Store Liveness fabric. It provides
// information about uninterrupted periods of "support" between stores.
type Fabric interface {
	// SupportFor returns the epoch of the current uninterrupted period of Store
	// Liveness support from the local store (S_local) for the store (S_remote)
	// corresponding to the specified id, and a boolean indicating whether S_local
	// supports S_remote.
	//
	// If S_local does not support S_remote, the epoch will be 0 and the boolean
	// will be false.
	//
	// S_remote may not be aware of the full extent of support from S_local, as
	// Store Liveness heartbeat response messages may be lost or delayed. However,
	// S_local will never be unaware of support it is providing.
	//
	// If S_local is unaware of the remote store S_remote, false will be returned.
	SupportFor(id slpb.StoreIdent) (slpb.Epoch, bool)

	// SupportFrom returns the epoch of the current uninterrupted period of Store
	// Liveness support for the local store (S_local) from the store (S_remote)
	// corresponding to the specified id, the timestamp until which the support is
	// provided (an expiration), and a boolean indicating whether S_local is
	// supported by S_remote.
	//
	// Whether support is provided or not is evaluated at the time of calling
	// SupportFrom by comparing the support expiration timestamp with S_local's
	// clock. If S_local is not supported by S_remote, the epoch will be 0, the
	// timestamp will be the zero timestamp, and the boolean will be false.
	//
	// S_local may not be aware of the full extent of support from S_remote, as
	// Store Liveness heartbeat response messages may be lost or delayed. However,
	// S_remote will never be unaware of support it is providing.
	//
	// If S_local is unaware of the remote store S_remote, false will be returned,
	// and S_local will initiate a heartbeat loop to S_remote in order to
	// request support so that future calls to SupportFrom may succeed.
	SupportFrom(id slpb.StoreIdent) (slpb.Epoch, hlc.Timestamp, bool)

	// SupportFromEnabled determines if Store Liveness requests support from
	// other stores. If it returns true, then Store Liveness is sending
	// heartbeats and responding to heartbeats. If it returns false, Store
	// Liveness is not sending heartbeats but is still responding to heartbeats
	// to ensure any promise by the local store to provide support is still kept.
	SupportFromEnabled(ctx context.Context) bool
}
