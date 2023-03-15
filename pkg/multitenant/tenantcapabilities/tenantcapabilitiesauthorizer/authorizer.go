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
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// authorizerEnabled dictates whether the Authorizer performs any capability
// checks or not. It is intended as an escape hatch to turn off the tenant
// capabilities infrastructure; as such, it is intended to be a sort of hammer
// of last resort, and isn't expected to be used during normal cluster
// operation.
var authorizerEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"tenant_capabilities.authorizer.enabled",
	"enables authorization based on capability checks for incoming (secondary tenant) requests",
	true,
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
	if a.elideCapabilityChecks(ctx, tenID) {
		return nil
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
		if !found {
			switch request.Method() {
			case kvpb.AdminSplit, kvpb.AdminScatter:
				// Secondary tenants are allowed to run AdminSplit and AdminScatter
				// requests by default, as they're integral to the performance of IMPORT
				// and RESTORE. If no entry is found in the capabilities map, we
				// fallback to this default behavior. Note that this isn't expected to
				// be the case during normal operation, as tenants that exist should
				// always have an entry in this map. It does help for some tests,
				// however.
				continue
			default:
				// For all other requests we conservatively return an error if no entry
				// is to be found for the tenant.
				return newTenantDoesNotHaveCapabilityError(requiredCap, request)
			}
		}
		if !hasCap || requiredCap == onlySystemTenant || !cp.GetBool(requiredCap) {
			// All allowable request types must be explicitly opted into the
			// reqMethodToCap map. If a request type is missing from the map
			// (!hasCap), we must be conservative and assume it is
			// disallowed. This prevents accidents where someone adds a new
			// sensitive request type in KV and forgets to add an explicit
			// authorization rule for it here.
			//
			// TODO(arul): This should be caught by a linter instead. Add a test that
			// goes over all request types and ensures there's an entry in this map
			// instead.
			return newTenantDoesNotHaveCapabilityError(requiredCap, request)
		}
	}
	return nil
}

func newTenantDoesNotHaveCapabilityError(
	cap tenantcapabilities.CapabilityID, req kvpb.Request,
) error {
	return errors.Newf("client tenant does not have capability %q (%T)", cap, req)
}

var reqMethodToCap = map[kvpb.Method]tenantcapabilities.CapabilityID{
	// The following requests are authorized for all workloads.
	kvpb.AddSSTable:         noCapCheckNeeded,
	kvpb.Barrier:            noCapCheckNeeded,
	kvpb.ClearRange:         noCapCheckNeeded,
	kvpb.ConditionalPut:     noCapCheckNeeded,
	kvpb.Delete:             noCapCheckNeeded,
	kvpb.DeleteRange:        noCapCheckNeeded,
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
	kvpb.ResolveIntent:      noCapCheckNeeded,
	kvpb.ResolveIntentRange: noCapCheckNeeded,
	kvpb.ReverseScan:        noCapCheckNeeded,
	kvpb.RevertRange:        noCapCheckNeeded,
	kvpb.Scan:               noCapCheckNeeded,

	// The following are authorized via specific capabilities.
	kvpb.AdminChangeReplicas: tenantcapabilities.CanAdminRelocateRange,
	kvpb.AdminScatter:        tenantcapabilities.CanAdminScatter,
	kvpb.AdminSplit:          tenantcapabilities.CanAdminSplit,
	kvpb.AdminUnsplit:        tenantcapabilities.CanAdminUnsplit,
	kvpb.AdminRelocateRange:  tenantcapabilities.CanAdminRelocateRange,
	kvpb.AdminTransferLease:  tenantcapabilities.CanAdminRelocateRange,

	// TODO(ecwall): The following should also be authorized via specific capabilities.
	kvpb.AdminMerge: noCapCheckNeeded,

	// TODO(knz,arul): Verify with the relevant teams whether secondary
	// tenants have legitimate access to any of those.
	kvpb.AdminVerifyProtectedTimestamp: onlySystemTenant,
	kvpb.CheckConsistency:              onlySystemTenant,
	kvpb.ComputeChecksum:               onlySystemTenant,
	kvpb.GC:                            onlySystemTenant,
	kvpb.Merge:                         onlySystemTenant,
	kvpb.Migrate:                       onlySystemTenant,
	kvpb.Probe:                         onlySystemTenant,
	kvpb.QueryResolvedTimestamp:        onlySystemTenant,
	kvpb.RecomputeStats:                onlySystemTenant,
	kvpb.RequestLease:                  onlySystemTenant,
	kvpb.Subsume:                       onlySystemTenant,
	kvpb.TransferLease:                 onlySystemTenant,
	kvpb.TruncateLog:                   onlySystemTenant,
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
	if a.elideCapabilityChecks(ctx, tenID) {
		return nil
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
	if a.elideCapabilityChecks(ctx, tenID) {
		return nil
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

// elideCapabilityChecks returns true if capability checks should be skipped for
// the supplied tenant.
func (a *Authorizer) elideCapabilityChecks(ctx context.Context, tenID roachpb.TenantID) bool {
	if tenID.IsSystem() {
		return true // the system tenant is allowed to do as it pleases
	}
	if !authorizerEnabled.Get(&a.settings.SV) {
		log.VInfof(ctx, 3, "authorizer turned off; eliding capability checks")
		return true
	}
	return false
}
