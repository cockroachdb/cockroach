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

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Authorizer is a concrete implementation of the tenantcapabilities.Authorizer
// interface. It's safe for concurrent use.
type Authorizer struct {
	capabilitiesReader tenantcapabilities.Reader
}

var _ tenantcapabilities.Authorizer = &Authorizer{}

// New constructs a new tenantcapabilities.Authorizer.
func New() *Authorizer {
	a := &Authorizer{
		// capabilitiesReader is set post construction, using BindReader.
	}
	return a
}

// HasCapabilityForBatch implements the tenantcapabilities.Authorizer interface.
func (a *Authorizer) HasCapabilityForBatch(
	ctx context.Context, tenID roachpb.TenantID, ba *roachpb.BatchRequest,
) error {
	if tenID.IsSystem() {
		return nil // the system tenant is allowed to do as it pleases
	}
	if a.capabilitiesReader == nil {
		log.Fatal(ctx, "trying to perform capability check when no Reader exists")
	}
	cp, found := a.capabilitiesReader.GetCapabilities(tenID)
	if !found {
		log.VInfof(
			ctx,
			3,
			"no capability information for tenant %s; requests that require capabilities may be denied",
			tenID,
		)
	}

	for _, ru := range ba.Requests {
		switch ru.GetInner().(type) {
		case *roachpb.AdminSplitRequest:
			if !cp.CanAdminSplit {
				return errors.Newf("tenant %s does not have admin split capability", tenID)
			}
		default:
			// No capability checks for other types of requests.
		}
	}
	return nil
}

// BindReader implements the tenantcapabilities.Authorizer interface.
func (a *Authorizer) BindReader(ctx context.Context, reader tenantcapabilities.Reader) {
	if a.capabilitiesReader != nil {
		log.Fatal(ctx, "cannot bind a tenant capabilities reader more than once")
	}
	a.capabilitiesReader = reader
}

func (a *Authorizer) HasNodeStatusCapability(ctx context.Context, tenID roachpb.TenantID) error {
	if tenID.IsSystem() {
		return nil // the system tenant is allowed to do as it pleases
	}
	cp, found := a.capabilitiesReader.GetCapabilities(tenID)
	if !found {
		log.Infof(
			ctx,
			"no capability information for tenant %s; requests that require capabilities may be denied",
			tenID,
		)
	}
	if !cp.CanViewNodeInfo {
		return errors.Newf("tenant %s does not have capability to query cluster node metadata", tenID)
	}
	return nil
}

func (a *Authorizer) HasTSDBQueryCapability(ctx context.Context, tenID roachpb.TenantID) error {
	if tenID.IsSystem() {
		return nil // the system tenant is allowed to do as it pleases
	}
	cp, found := a.capabilitiesReader.GetCapabilities(tenID)
	if !found {
		log.Infof(
			ctx,
			"no capability information for tenant %s; requests that require capabilities may be denied",
			tenID,
		)
	}
	if !cp.CanViewTsdbMetrics {
		return errors.Newf("tenant %s does not have capability to query timseries data", tenID)
	}
	return nil
}
