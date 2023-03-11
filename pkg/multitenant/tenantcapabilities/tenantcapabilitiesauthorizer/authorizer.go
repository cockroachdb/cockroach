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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	ctx context.Context, tenID roachpb.TenantID, ba *kvpb.BatchRequest,
) error {
	if tenID.IsSystem() {
		return nil // the system tenant is allowed to do as it pleases.
	}
	if a.capabilitiesReader == nil {
		return errors.AssertionFailedf("programming error: trying to perform capability check when no reader exists")
	}
	cp, found := a.capabilitiesReader.GetCapabilities(tenID)
	if !found {
		log.VInfof(ctx, 2,
			"no capability information for tenant %s; requests that require capabilities may be denied",
			tenID)
	}

	for _, ru := range ba.Requests {
		request := ru.GetInner()
		requiredCap, hasCap := reqMethodToCap[request.Method()]
		if requiredCap == noCapCheckNeeded {
			continue
		}
		if !hasCap || requiredCap == onlySystemTenant || !found || !cp.GetBool(requiredCap) {
			if requiredCap == tenantcapabilities.CanAdminSplit && a.knobs.AuthorizerSkipAdminSplitCapabilityChecks {
				continue
			}
			// All allowable request types must be explicitly opted into the
			// reqMethodToCap map. If a request type is missing from the map
			// (!hasCap), we must be conservative and assume it is
			// disallowed. This prevents accidents where someone adds a new
			// sensitive request type in KV and forgets to add an explicit
			// authorization rule for it here.
			return errors.Newf("client tenant does not have capability %q (%T)", requiredCap, request)
		}
	}
	return nil
}

var reqMethodToCap = map[kvpb.Method]tenantcapabilities.CapabilityID{
	// The following requests are authorized for all workloads.
	kvpb.AddSSTable:         noCapCheckNeeded,
	kvpb.Barrier:            noCapCheckNeeded,
	kvpb.ClearRange:         noCapCheckNeeded,
	kvpb.ConditionalPut:     noCapCheckNeeded,
	kvpb.DeleteRange:        noCapCheckNeeded,
	kvpb.Delete:             noCapCheckNeeded,
	kvpb.EndTxn:             noCapCheckNeeded,
	kvpb.Export:             noCapCheckNeeded,
	kvpb.Get:                noCapCheckNeeded,
	kvpb.HeartbeatTxn:       noCapCheckNeeded,
	kvpb.Increment:          noCapCheckNeeded,
	kvpb.InitPut:            noCapCheckNeeded,
	kvpb.IsSpanEmpty:        noCapCheckNeeded,
	kvpb.LeaseInfo:          noCapCheckNeeded,
	kvpb.PushTxn:            noCapCheckNeeded,
	kvpb.Put:                noCapCheckNeeded,
	kvpb.QueryIntent:        noCapCheckNeeded,
	kvpb.QueryLocks:         noCapCheckNeeded,
	kvpb.QueryTxn:           noCapCheckNeeded,
	kvpb.RangeStats:         noCapCheckNeeded,
	kvpb.RecoverTxn:         noCapCheckNeeded,
	kvpb.Refresh:            noCapCheckNeeded,
	kvpb.RefreshRange:       noCapCheckNeeded,
	kvpb.ResolveIntentRange: noCapCheckNeeded,
	kvpb.ResolveIntent:      noCapCheckNeeded,
	kvpb.ReverseScan:        noCapCheckNeeded,
	kvpb.RevertRange:        noCapCheckNeeded,
	kvpb.Scan:               noCapCheckNeeded,

	// The following are authorized via specific capabilities.
	kvpb.AdminSplit:   tenantcapabilities.CanAdminSplit,
	kvpb.AdminUnsplit: tenantcapabilities.CanAdminUnsplit,

	// TODO(ecwall): The following should also be authorized via specific capabilities.
	kvpb.AdminChangeReplicas: noCapCheckNeeded,
	kvpb.AdminMerge:          noCapCheckNeeded,
	kvpb.AdminRelocateRange:  noCapCheckNeeded,
	kvpb.AdminScatter:        noCapCheckNeeded,
	kvpb.AdminTransferLease:  noCapCheckNeeded,

	// TODO(knz,arul): Verify with the relevant teams whether secondary
	// tenants have legitimate access to any of those.
	kvpb.TruncateLog:                   onlySystemTenant,
	kvpb.Merge:                         onlySystemTenant,
	kvpb.RequestLease:                  onlySystemTenant,
	kvpb.TransferLease:                 onlySystemTenant,
	kvpb.Probe:                         onlySystemTenant,
	kvpb.RecomputeStats:                onlySystemTenant,
	kvpb.ComputeChecksum:               onlySystemTenant,
	kvpb.CheckConsistency:              onlySystemTenant,
	kvpb.AdminVerifyProtectedTimestamp: onlySystemTenant,
	kvpb.Migrate:                       onlySystemTenant,
	kvpb.Subsume:                       onlySystemTenant,
	kvpb.QueryResolvedTimestamp:        onlySystemTenant,
	kvpb.GC:                            onlySystemTenant,
}

const (
	noCapCheckNeeded = iota + tenantcapabilities.MaxCapabilityID + 1
	onlySystemTenant
)

// BindReader implements the tenantcapabilities.Authorizer interface.
func (a *Authorizer) BindReader(reader tenantcapabilities.Reader) {
	a.capabilitiesReader = reader
}

func (a *Authorizer) HasNodeStatusCapability(ctx context.Context, tenID roachpb.TenantID) error {
	if tenID.IsSystem() {
		return nil // the system tenant is allowed to do as it pleases.
	}
	cp, found := a.capabilitiesReader.GetCapabilities(tenID)
	if !found {
		log.Infof(ctx,
			"no capability information for tenant %s; requests that require capabilities may be denied",
			tenID,
		)
	}
	if !found || !cp.GetBool(tenantcapabilities.CanViewNodeInfo) {
		return errors.Newf("client tenant does not have capability to query cluster node metadata")
	}
	return nil
}

func (a *Authorizer) HasTSDBQueryCapability(ctx context.Context, tenID roachpb.TenantID) error {
	if tenID.IsSystem() {
		return nil // the system tenant is allowed to do as it pleases.
	}
	cp, found := a.capabilitiesReader.GetCapabilities(tenID)
	if !found {
		log.Infof(ctx,
			"no capability information for tenant %s; requests that require capabilities may be denied",
			tenID,
		)
	}
	if !found || !cp.GetBool(tenantcapabilities.CanViewTSDBMetrics) {
		return errors.Newf("client tenant does not have capability to query timeseries data")
	}
	return nil
}
