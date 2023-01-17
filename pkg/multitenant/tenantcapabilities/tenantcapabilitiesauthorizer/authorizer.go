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
)

// Authorizer is a concrete implementation of the tenantcapabilities.Authorizer
// interface. It's safe for concurrent use.
type Authorizer struct {
	capabilitiesReader tenantcapabilities.Reader
}

var _ tenantcapabilities.Authorizer = &Authorizer{}

// New constructs a new tenantcapabilities.Authorizer.
func New(reader tenantcapabilities.Reader) *Authorizer {
	a := &Authorizer{
		capabilitiesReader: reader,
	}
	return a
}

// HasCapabilityForBatch implements the tenantcapabilities.Authorizer interface.
func (a *Authorizer) HasCapabilityForBatch(
	ctx context.Context, tenID roachpb.TenantID, ba *roachpb.BatchRequest,
) bool {
	if tenID.IsSystem() {
		return true // the system tenant is allowed to do as it pleases
	}
	cp, found := a.capabilitiesReader.GetCapabilities(tenID)
	if !found {
		log.Infof(
			ctx,
			"no capability information for tenant %s; requests that require capabilities may be denied",
			tenID,
		)
	}

	for _, ru := range ba.Requests {
		switch ru.GetInner().(type) {
		case *roachpb.AdminSplitRequest:
			if !cp.CanAdminSplit {
				return false
			}
		default:
			// No capability checks for other types of requests.
		}
	}
	return true
}
