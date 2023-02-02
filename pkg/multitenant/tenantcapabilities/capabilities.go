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

// Watcher presents a consistent snapshot of the global tenant capabilities
// state. It incrementally, and transparently, maintains this state by watching
// for changes to system.tenants.
type Watcher interface {
	Reader

	// Start asynchronously begins watching over the global tenant capability
	// state.
	Start(ctx context.Context) error
}

// Reader provides access to the global tenant capability state. The global
// tenant capability state may be arbitrarily stale.
type Reader interface {
	GetCapabilities(id roachpb.TenantID) (_ tenantcapabilitiespb.TenantCapabilities, found bool)
}

// Authorizer performs various kinds of capability checks for requests issued
// by tenants. It does so by consulting the global tenant capability state.
//
// In the future, we may want to expand the Authorizer to take into account
// signals other than just the tenant capability state. For example, request
// usage pattern over a timespan.
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
