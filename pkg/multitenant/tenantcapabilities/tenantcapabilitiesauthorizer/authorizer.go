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
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesapi"
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
		log.Infof(ctx,
			"no capability information for tenant %s; requests that require capabilities may be denied",
			tenID,
		)
	}

	for _, ru := range ba.Requests {
		requiredCap, hasCap := reqTypeToCap[reflect.TypeOf(ru.GetInner())]
		if requiredCap == noCapCheckNeeded {
			continue
		}
		if !hasCap || requiredCap == onlySystemTenant || !found || !cp.GetBool(requiredCap) {
			// All allowable request types must be explicitly opted into the
			// reqTypeToCap map. If a request type is missing from the map
			// (!hasCap), we must be conservative and assume it is
			// disallowed. This prevents accidents where someone adds a new
			// sensitive request type in KV and forgets to add an explicit
			// authorization rule for it here.
			return errors.Newf("client tenant does not have capability %q (%T)", requiredCap, ru.GetInner())
		}
	}
	return nil
}

var reqTypeToCap map[reflect.Type]tenantcapabilitiesapi.CapabilityID = map[reflect.Type]tenantcapabilitiesapi.CapabilityID{
	// The following requests are authorized for all workloads.
	reflect.TypeOf((*kvpb.AddSSTableRequest)(nil)):         noCapCheckNeeded,
	reflect.TypeOf((*kvpb.BarrierRequest)(nil)):            noCapCheckNeeded,
	reflect.TypeOf((*kvpb.ClearRangeRequest)(nil)):         noCapCheckNeeded,
	reflect.TypeOf((*kvpb.ConditionalPutRequest)(nil)):     noCapCheckNeeded,
	reflect.TypeOf((*kvpb.DeleteRangeRequest)(nil)):        noCapCheckNeeded,
	reflect.TypeOf((*kvpb.DeleteRequest)(nil)):             noCapCheckNeeded,
	reflect.TypeOf((*kvpb.EndTxnRequest)(nil)):             noCapCheckNeeded,
	reflect.TypeOf((*kvpb.ExportRequest)(nil)):             noCapCheckNeeded,
	reflect.TypeOf((*kvpb.GetRequest)(nil)):                noCapCheckNeeded,
	reflect.TypeOf((*kvpb.HeartbeatTxnRequest)(nil)):       noCapCheckNeeded,
	reflect.TypeOf((*kvpb.IncrementRequest)(nil)):          noCapCheckNeeded,
	reflect.TypeOf((*kvpb.InitPutRequest)(nil)):            noCapCheckNeeded,
	reflect.TypeOf((*kvpb.IsSpanEmptyRequest)(nil)):        noCapCheckNeeded,
	reflect.TypeOf((*kvpb.PushTxnRequest)(nil)):            noCapCheckNeeded,
	reflect.TypeOf((*kvpb.PutRequest)(nil)):                noCapCheckNeeded,
	reflect.TypeOf((*kvpb.QueryIntentRequest)(nil)):        noCapCheckNeeded,
	reflect.TypeOf((*kvpb.QueryLocksRequest)(nil)):         noCapCheckNeeded,
	reflect.TypeOf((*kvpb.QueryTxnRequest)(nil)):           noCapCheckNeeded,
	reflect.TypeOf((*kvpb.RecoverTxnRequest)(nil)):         noCapCheckNeeded,
	reflect.TypeOf((*kvpb.RefreshRequest)(nil)):            noCapCheckNeeded,
	reflect.TypeOf((*kvpb.RefreshRangeRequest)(nil)):       noCapCheckNeeded,
	reflect.TypeOf((*kvpb.ResolveIntentRangeRequest)(nil)): noCapCheckNeeded,
	reflect.TypeOf((*kvpb.ResolveIntentRequest)(nil)):      noCapCheckNeeded,
	reflect.TypeOf((*kvpb.ReverseScanRequest)(nil)):        noCapCheckNeeded,
	reflect.TypeOf((*kvpb.RevertRangeRequest)(nil)):        noCapCheckNeeded,
	reflect.TypeOf((*kvpb.ScanRequest)(nil)):               noCapCheckNeeded,

	// The following are authorized via specific capabilities.
	reflect.TypeOf((*kvpb.AdminSplitRequest)(nil)): tenantcapabilitiesapi.CanAdminSplit,

	// TODO(ecwall): The following should also be authorized via specific capabilities.
	reflect.TypeOf((*kvpb.AdminChangeReplicasRequest)(nil)): onlySystemTenant,
	reflect.TypeOf((*kvpb.AdminMergeRequest)(nil)):          onlySystemTenant,
	reflect.TypeOf((*kvpb.AdminRelocateRangeRequest)(nil)):  onlySystemTenant,
	reflect.TypeOf((*kvpb.AdminScatterRequest)(nil)):        onlySystemTenant,
	reflect.TypeOf((*kvpb.AdminTransferLeaseRequest)(nil)):  onlySystemTenant,
	reflect.TypeOf((*kvpb.AdminUnsplitRequest)(nil)):        onlySystemTenant,

	// TODO(knz,arul): Verify with the relevant teams whether secondary
	// tenants have legitimate access to any of those.
	reflect.TypeOf((*kvpb.TruncateLogRequest)(nil)):                   onlySystemTenant,
	reflect.TypeOf((*kvpb.MergeRequest)(nil)):                         onlySystemTenant,
	reflect.TypeOf((*kvpb.RequestLeaseRequest)(nil)):                  onlySystemTenant,
	reflect.TypeOf((*kvpb.LeaseInfoRequest)(nil)):                     onlySystemTenant,
	reflect.TypeOf((*kvpb.TransferLeaseRequest)(nil)):                 onlySystemTenant,
	reflect.TypeOf((*kvpb.ProbeRequest)(nil)):                         onlySystemTenant,
	reflect.TypeOf((*kvpb.RecomputeStatsRequest)(nil)):                onlySystemTenant,
	reflect.TypeOf((*kvpb.ComputeChecksumRequest)(nil)):               onlySystemTenant,
	reflect.TypeOf((*kvpb.CheckConsistencyRequest)(nil)):              onlySystemTenant,
	reflect.TypeOf((*kvpb.AdminVerifyProtectedTimestampRequest)(nil)): onlySystemTenant,
	reflect.TypeOf((*kvpb.MigrateRequest)(nil)):                       onlySystemTenant,
	reflect.TypeOf((*kvpb.SubsumeRequest)(nil)):                       onlySystemTenant,
	reflect.TypeOf((*kvpb.RangeStatsRequest)(nil)):                    onlySystemTenant,
	reflect.TypeOf((*kvpb.QueryResolvedTimestampRequest)(nil)):        onlySystemTenant,
	reflect.TypeOf((*kvpb.GCRequest)(nil)):                            onlySystemTenant,
}

const noCapCheckNeeded = tenantcapabilitiesapi.LastCapabilityID + 1
const onlySystemTenant = tenantcapabilitiesapi.LastCapabilityID + 2

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
	if !found || !cp.GetBool(tenantcapabilitiesapi.CanViewNodeInfo) {
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
	if !found || !cp.GetBool(tenantcapabilitiesapi.CanViewTSDBMetrics) {
		return errors.Newf("client tenant does not have capability to query timeseries data")
	}
	return nil
}
