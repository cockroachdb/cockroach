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
)

// NoopAuthorizer is a tenantcapabilities.Authorizer that simply no-ops.
type NoopAuthorizer struct{}

var _ tenantcapabilities.Authorizer = &NoopAuthorizer{}

// NewNoopAuthorizer constructs and returns a NoopAuthorizer.
func NewNoopAuthorizer() *NoopAuthorizer {
	return &NoopAuthorizer{}
}

// HasCapabilityForBatch implements the tenantcapabilities.Authorizer interface.
func (n *NoopAuthorizer) HasCapabilityForBatch(
	context.Context, roachpb.TenantID, *kvpb.BatchRequest,
) error {
	return nil
}

// BindReader implements the tenantcapabilities.Authorizer interface.
func (n *NoopAuthorizer) BindReader(tenantcapabilities.Reader) {
}

// HasNodeStatusCapability implements the tenantcapabilities.Authorizer interface
func (n *NoopAuthorizer) HasNodeStatusCapability(
	ctx context.Context, tenID roachpb.TenantID,
) error {
	return nil
}

// HasTSDBQueryCapability implements the tenantcapabilities.Authorizer interface
func (n *NoopAuthorizer) HasTSDBQueryCapability(ctx context.Context, tenID roachpb.TenantID) error {
	return nil
}
