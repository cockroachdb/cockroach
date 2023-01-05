// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilities

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// CapabilitiesWatcher presents a consistent snapshot of the global tenant
// capabilities state. It incrementally, and transparently, maintains this state
// by watching for changes.
type CapabilitiesWatcher interface {
	Authorizer

	// Start asynchronously begins watching over the global tenant capability
	// state.
	Start(ctx context.Context) error
}

// Authorizer stores an in-memory snapshot of the global tenant capabilities
// state and consults it to authorize operations.
type Authorizer interface {
	// HasCapabilityForBatch returns whether a tenant, referenced by its ID, is
	// allowed to execute the supplied batch request given the capabilities it
	// possesses.
	HasCapabilityForBatch(context.Context, roachpb.TenantID, *roachpb.BatchRequest) bool
}

// Entry ties together a tenantID with its capabilities.
type Entry struct {
	TenantID           roachpb.TenantID
	TenantCapabilities tenantcapabilitiespb.TenantCapabilities
}

// Update represents an update to the global tenant capability state.
type Update struct {
	Entry
	Deleted bool // whether the entry was deleted or not
}
