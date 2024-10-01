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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/mtinfopb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Reader provides access to the global tenant capability state. The global
// tenant capability state may be arbitrarily stale.
type Reader interface {
	// GetInfo returns the tenant information for the specified tenant.
	GetInfo(id roachpb.TenantID) (_ Entry, _ <-chan struct{}, found bool)

	// GetCapabilities returns the tenant capabilities for the specified tenant.
	GetCapabilities(id roachpb.TenantID) (_ *tenantcapabilitiespb.TenantCapabilities, found bool)

	// GetGlobalCapabilityState returns the capability state for all tenants.
	GetGlobalCapabilityState() map[roachpb.TenantID]*tenantcapabilitiespb.TenantCapabilities
}

// Authorizer performs various kinds of capability checks for requests issued
// by tenants. It does so by consulting the global tenant capability state.
//
// In the future, we may want to expand the Authorizer to take into account
// signals other than just the tenant capability state. For example, request
// usage pattern over a timespan.
type Authorizer interface {
	// HasCrossTenantRead returns true if a tenant can read other tenant spans.
	HasCrossTenantRead(ctx context.Context, tenID roachpb.TenantID, key roachpb.RKey) bool

	// HasCapabilityForBatch returns an error if a tenant, referenced by its ID,
	// is not allowed to execute the supplied batch request given the capabilities
	// it possesses.
	HasCapabilityForBatch(context.Context, roachpb.TenantID, *kvpb.BatchRequest) error

	// BindReader is a mechanism by which the caller can bind a Reader[1] to the
	// Authorizer post-creation. The Authorizer uses the Reader to consult the
	// global tenant capability state to authorize incoming requests. This
	// function cannot be used to update the Reader.
	//
	//
	// [1] The canonical implementation of the Authorizer lives on GRPC
	// interceptors, and as such, must be instantiated before the GRPC Server is
	// created. However, the GRPC server is created very early on during Server
	// startup and serves as a dependency for the canonical Reader's
	// implementation. Binding the Reader late allows us to break this dependency
	// cycle.
	BindReader(reader Reader)

	// HasNodeStatusCapability returns an error if a tenant, referenced by its ID,
	// is not allowed to access cluster-level node metadata and liveness.
	HasNodeStatusCapability(ctx context.Context, tenID roachpb.TenantID) error

	// HasNodelocalStorageCapability returns an error if a tenant,
	// referenced by its ID, is not allowed to use the nodelocal
	// storage service.
	HasNodelocalStorageCapability(ctx context.Context, tenID roachpb.TenantID) error

	// HasTSDBQueryCapability returns an error if a tenant, referenced by its ID,
	// is not allowed to query the TSDB for metrics.
	HasTSDBQueryCapability(ctx context.Context, tenID roachpb.TenantID) error

	// IsExemptFromRateLimiting returns true of the tenant should
	// not be subject to rate limiting.
	IsExemptFromRateLimiting(ctx context.Context, tenID roachpb.TenantID) bool

	// HasProcessDebugCapability returns an error if a tenant, referenced by its ID,
	// is not allowed to debug the running process.
	HasProcessDebugCapability(ctx context.Context, tenID roachpb.TenantID) error

	// HasTSDBAllMetricsCapability returns an error if a tenant, referenced by its ID,
	// is not allowed to query all metrics from the host.
	HasTSDBAllMetricsCapability(ctx context.Context, tenID roachpb.TenantID) error
}

// Entry ties together a tenantID with its capabilities.
type Entry struct {
	TenantID           roachpb.TenantID
	TenantCapabilities *tenantcapabilitiespb.TenantCapabilities
	Name               roachpb.TenantName
	DataState          mtinfopb.TenantDataState
	ServiceMode        mtinfopb.TenantServiceMode
}

// Ready indicates whether the metadata record is populated.
func (e Entry) Ready() bool {
	return e.TenantCapabilities != nil
}

// Update represents an update to the global tenant capability state.
type Update struct {
	Entry
	Deleted   bool // whether the entry was deleted or not
	Timestamp hlc.Timestamp
}

func (u Update) String() string {
	if u.Deleted {
		return fmt.Sprintf("delete: ten=%v", u.Entry.TenantID)
	}
	return fmt.Sprintf("update: %v", u.Entry)
}

func (u Entry) String() string {
	return fmt.Sprintf("ten=%v cap=%v", u.TenantID, u.TenantCapabilities)
}
