// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitiesauthorizer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// AllowNothingAuthorizer is a tenantcapabilities.Authorizer that
// allows all operations
type AllowNothingAuthorizer struct{}

var _ tenantcapabilities.Authorizer = &AllowNothingAuthorizer{}

// NewAllowNothingAuthorizer constructs and returns a AllowNothingAuthorizer.
func NewAllowNothingAuthorizer() *AllowNothingAuthorizer {
	return &AllowNothingAuthorizer{}
}

// HasCrossTenantRead returns true if a tenant can read from other tenants.
func (n *AllowNothingAuthorizer) HasCrossTenantRead(
	ctx context.Context, tenID roachpb.TenantID, key roachpb.RKey,
) bool {
	return false
}

// HasCapabilityForBatch implements the tenantcapabilities.Authorizer interface.
func (n *AllowNothingAuthorizer) HasCapabilityForBatch(
	context.Context, roachpb.TenantID, *kvpb.BatchRequest,
) error {
	return errors.New("operation blocked")
}

// BindReader implements the tenantcapabilities.Authorizer interface.
func (n *AllowNothingAuthorizer) BindReader(tenantcapabilities.Reader) {}

// HasNodeStatusCapability implements the tenantcapabilities.Authorizer interface.
func (n *AllowNothingAuthorizer) HasNodeStatusCapability(
	ctx context.Context, tenID roachpb.TenantID,
) error {
	return errors.New("operation blocked")
}

// HasTSDBQueryCapability implements the tenantcapabilities.Authorizer interface.
func (n *AllowNothingAuthorizer) HasTSDBQueryCapability(
	ctx context.Context, tenID roachpb.TenantID,
) error {
	return errors.New("operation blocked")
}

// HasNodelocalStorageCapability implements the tenantcapabilities.Authorizer interface.
func (n *AllowNothingAuthorizer) HasNodelocalStorageCapability(
	ctx context.Context, tenID roachpb.TenantID,
) error {
	return errors.New("operation blocked")
}

// IsExemptFromRateLimiting implements the tenantcapabilities.Authorizer interface.
func (n *AllowNothingAuthorizer) IsExemptFromRateLimiting(context.Context, roachpb.TenantID) bool {
	return false
}

// HasProcessDebugCapability implements the tenantcapabilities.Authorizer interface.
func (n *AllowNothingAuthorizer) HasProcessDebugCapability(
	ctx context.Context, tenID roachpb.TenantID,
) error {
	return errors.New("operation blocked")
}

// HasTSDBAllMetricsCapability implements the tenantcapabilities.Authorizer interface.
func (n *AllowNothingAuthorizer) HasTSDBAllMetricsCapability(
	ctx context.Context, tenID roachpb.TenantID,
) error {
	return errors.New("operation blocked")
}
