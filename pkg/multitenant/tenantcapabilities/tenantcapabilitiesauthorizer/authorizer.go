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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/errors"
)

// Authorizer is a concrete implementation of the tenantcapabilities.Authorizer
// interface. It's safe for concurrent use.
type Authorizer struct {
	capabilitiesReader tenantcapabilities.Reader
	settings           *cluster.Settings

	knobs tenantcapabilities.TestingKnobs
}

var _ tenantcapabilities.Authorizer = &Authorizer{}

// New constructs a new tenantcapabilities.Authorizer.
func New(settings *cluster.Settings, knobs *tenantcapabilities.TestingKnobs) *Authorizer {
	var testingKnobs tenantcapabilities.TestingKnobs
	if knobs != nil {
		testingKnobs = *knobs
	}
	a := &Authorizer{
		settings: settings,
		knobs:    testingKnobs,
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
		logcrash.ReportOrPanic(
			ctx, &a.settings.SV, "trying to perform capability check when no Reader exists",
		)
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
			if !cp.CanAdminSplit && !a.knobs.AuthorizerSkipAdminSplitCapabilityChecks {
				return errors.Newf("tenant %s does not have admin split capability", tenID)
			}
		default:
			return errors.Newf("request [%s] not permitted", ba.Summary())
		}
	}
	return nil
}

// BindReader implements the tenantcapabilities.Authorizer interface.
func (a *Authorizer) BindReader(reader tenantcapabilities.Reader) {
	a.capabilitiesReader = reader
}
