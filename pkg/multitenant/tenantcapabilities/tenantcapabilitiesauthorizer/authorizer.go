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
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Authorizer is a concrete implementation of the tenantcapabilities.Authorizer
// interface. It's safe for concurrent use.
type Authorizer struct {
	mu struct {
		syncutil.RWMutex
		capabilities map[roachpb.TenantID]tenantcapabilitiespb.TenantCapabilities
	}
}

var _ tenantcapabilities.Authorizer = &Authorizer{}

// New constructs a new tenantcapabilities.Authorizer.
func New() *Authorizer {
	a := &Authorizer{}
	a.mu.capabilities = make(map[roachpb.TenantID]tenantcapabilitiespb.TenantCapabilities)
	return a
}

// HasCapabilityForBatch implements the tenantcapabilities.Authorizer interface.
func (a *Authorizer) HasCapabilityForBatch(
	ctx context.Context, tenID roachpb.TenantID, ba *roachpb.BatchRequest,
) bool {
	if tenID.IsSystem() {
		return true // The system tenant is allowed to do as they please
	}
	a.mu.RLock()
	defer a.mu.RUnlock()

	for _, ru := range ba.Requests {
		switch ru.GetInner().(type) {
		case *roachpb.AdminSplitRequest:
			cp, ok := a.mu.capabilities[tenID]
			if !ok {
				log.Infof(
					ctx, "no capability information for tenant %s; denying request for AdminSplit", tenID,
				)
				return false
			}
			if !cp.CanAdminSplit {
				return false
			}
		}
	}
	return true
}

// Apply atomically applies a set of updates to the authorizer state.
func (a *Authorizer) Apply(updates []tenantcapabilities.Update) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, update := range updates {
		if update.TenantID.IsSystem() {
			return errors.AssertionFailedf("cannot update capabilities for the system tenant")
		}

		if update.Deleted {
			delete(a.mu.capabilities, update.TenantID)
		} else {
			a.mu.capabilities[update.TenantID] = update.TenantCapabilities
		}
	}
	return nil
}

// TestingFlushCapabilitiesState flushes the underlying global tenant capability
// state for testing purposes.
func (a *Authorizer) TestingFlushCapabilitiesState() (entries []tenantcapabilities.Entry) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for id, capability := range a.mu.capabilities {
		entries = append(entries, tenantcapabilities.Entry{
			TenantID:           id,
			TenantCapabilities: capability,
		})
	}
	return entries
}
