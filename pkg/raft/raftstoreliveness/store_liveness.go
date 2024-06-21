// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raftstoreliveness

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

// StoreLivenessEpoch is an epoch in the Store Liveness fabric, referencing an
// uninterrupted period of support from one store to another.
type StoreLivenessEpoch int64

// StoreLiveness is a representation of the Store Liveness fabric. It provides
// information about uninterrupted periods of "support" between stores.
type StoreLiveness interface {

	// SupportFor returns the epoch of the current uninterrupted period of Store
	// Liveness support for the specified replica's remote store (S_remote) from
	// the local replica's store (S_local), and a boolean indicating whether
	// S_local is currently supporting S_remote.
	//
	// If S_local is not currently supporting S_remote, the epoch will be 0 and
	// the boolean will be false.
	//
	// S_remote may not be aware of the full extent of support provided by
	// S_local, as Store Liveness heartbeat acknowledgement messages may be lost
	// or delayed. However, S_local will never be unaware of support it is
	// providing.
	//
	// If S_local cannot map the replica ID to a store ID, false will be returned.
	// It is therefore important to ensure that the replica ID to store ID mapping
	// is not lost during periods of support.
	SupportFor(id uint64) (StoreLivenessEpoch, bool)

	// SupportFrom returns the epoch of the current uninterrupted period of Store
	// Liveness support from the specified replica's remote store (S_remote) for
	// the local replica's store (S_local), the timestamp which the support is
	// provided until (an expiration), and a boolean indicating whether S_local is
	// currently supported by S_remote.
	//
	// If S_local is not currently supported by S_remote, the epoch will be 0, the
	// timestamp will be the zero timestamp, and the boolean will be false.
	//
	// S_local may not be aware of the full extent of support provided by
	// S_remote, as Store Liveness heartbeat acknowledgement messages may be lost
	// or delayed. However, S_remote will never be unaware of support it is
	// providing.
	//
	// If S_local cannot map the replica ID to a store ID, false will be returned.
	SupportFrom(id uint64) (StoreLivenessEpoch, hlc.Timestamp, bool)

	// SupportFromEnabled returns whether StoreLiveness is currently active, and
	// callers can rely on getting support from peers by calling SupportFrom.
	//
	// Note that even if StoreLiveness isn't currently active, replicas should not
	// break any previously made promises back when it was. As such, calls to
	// SupportFor should be made regardless of whether StoreLiveness is currently
	// active or not, which is what prompts the "SupportFrom" prefix.
	SupportFromEnabled() bool

	// SupportInPast returns whether the supplied timestamp is before the current
	// time.
	SupportInPast(ts hlc.Timestamp) bool
}
