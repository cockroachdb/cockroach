// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantcapabilitiesauthorizer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// authorizerMode configures how tenant capabilities are checked.
//   - on: requests are accepted according to the tenant's capabilities.
//   - allow-all: all requests are accepted.
//   - v222: requests not subject to capabilities are accepted; other
//     requests are only accepted for the system tenant.
//     This is also adequate for CC serverless.
var authorizerMode = settings.RegisterEnumSetting(
	settings.SystemOnly,
	"server.secondary_tenants.authorization.mode",
	"configures how requests are authorized for secondary tenants",
	"on",
	map[int64]string{
		int64(authorizerModeOn):       "on",
		int64(authorizerModeAllowAll): "allow-all",
		int64(authorizerModeV222):     "v222",
	},
	settings.WithName("server.virtual_cluster_authorization.mode"),
)

type authorizerModeType int64

const (
	// authorizerModeOn is the default mode. It checks tenant capabilities.
	authorizerModeOn authorizerModeType = iota
	// authorizerModeAllowAll allows all requests.
	// This can be used for troubleshooting when secondary tenants
	// are expected to be granted all capabilities.
	authorizerModeAllowAll
	// authorizerModeV222 is the pre-v23.1 authorization behavior.
	// Requests that are not subject to capabilities are allowed, and other
	// requests are only allowed for the system tenant.
	authorizerModeV222
)

// Authorizer is a concrete implementation of the tenantcapabilities.Authorizer
// interface. It's safe for concurrent use.
type Authorizer struct {
	settings *cluster.Settings
	knobs    tenantcapabilities.TestingKnobs

	// We protect capabilitiesReader by a mutex because it is assigned
	// asynchronously during server startup, after the RPC service may
	// have started accepting requests.
	syncutil.Mutex
	capabilitiesReader tenantcapabilities.Reader
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
		// The system tenant has access to all request types.
		return nil
	}

	cp, mode := a.getMode(ctx, tenID)
	switch mode {
	case authorizerModeOn:
		return a.capCheckForBatch(ctx, tenID, ba, cp)
	case authorizerModeAllowAll:
		return nil
	case authorizerModeV222:
		return a.authBatchNoCap(ctx, tenID, ba)
	default:
		err := errors.AssertionFailedf("unknown authorizer mode: %d", mode)
		logcrash.ReportOrPanic(ctx, &a.settings.SV, "%v", err)
		return err
	}
}

// authBatchNoCap implements the pre-v23.1 authorization behavior, where
// requests that are not subject to capabilities are allowed, and other
// requests are only allowed for the system tenant.
func (a *Authorizer) authBatchNoCap(
	ctx context.Context, tenID roachpb.TenantID, ba *kvpb.BatchRequest,
) error {
	for _, ru := range ba.Requests {
		request := ru.GetInner()
		requiredCap := reqMethodToCap[request.Method()]
		if requiredCap == noCapCheckNeeded {
			continue
		}
		switch request.Method() {
		case kvpb.AdminSplit, kvpb.AdminScatter:
			// Secondary tenants are allowed to run AdminSplit and
			// AdminScatter requests by default, as they're integral to the
			// performance of IMPORT and RESTORE.
			continue
		}
		return newTenantDoesNotHaveCapabilityError(requiredCap, request)
	}
	return nil
}

func (a *Authorizer) capCheckForBatch(
	ctx context.Context,
	tenID roachpb.TenantID,
	ba *kvpb.BatchRequest,
	cp *tenantcapabilitiespb.TenantCapabilities,
) error {
	for _, ru := range ba.Requests {
		request := ru.GetInner()
		requiredCap, hasCap := reqMethodToCap[request.Method()]
		if requiredCap == noCapCheckNeeded {
			continue
		}
		if !hasCap || requiredCap == onlySystemTenant ||
			!tenantcapabilities.MustGetBoolByID(cp, requiredCap) {
			// All allowable request types must be explicitly opted into the
			// reqMethodToCap map. If a request type is missing from the map
			// (!hasCap), we must be conservative and assume it is
			// disallowed. This prevents accidents where someone adds a new
			// sensitive request type in KV and forgets to add an explicit
			// authorization rule for it here.
			return newTenantDoesNotHaveCapabilityError(requiredCap, request)
		}
	}
	return nil
}

func newTenantDoesNotHaveCapabilityError(cap tenantcapabilities.ID, req kvpb.Request) error {
	return errors.Newf("client tenant does not have capability %q (%T)", cap, req)
}

var reqMethodToCap = map[kvpb.Method]tenantcapabilities.ID{
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
	kvpb.CheckConsistency:    tenantcapabilities.CanCheckConsistency,

	// TODO(knz,arul): Verify with the relevant teams whether secondary
	// tenants have legitimate access to any of those.
	kvpb.AdminMerge:                    onlySystemTenant,
	kvpb.AdminVerifyProtectedTimestamp: onlySystemTenant,
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
	kvpb.WriteBatch:                    onlySystemTenant,
}

const (
	noCapCheckNeeded = iota + tenantcapabilities.MaxCapabilityID + 1
	onlySystemTenant
)

// BindReader implements the tenantcapabilities.Authorizer interface.
func (a *Authorizer) BindReader(reader tenantcapabilities.Reader) {
	a.Lock()
	defer a.Unlock()
	a.capabilitiesReader = reader
}

func (a *Authorizer) HasNodeStatusCapability(ctx context.Context, tenID roachpb.TenantID) error {
	if tenID.IsSystem() {
		return nil
	}
	errFn := func() error {
		return errors.New("client tenant does not have capability to query cluster node metadata")
	}
	cp, mode := a.getMode(ctx, tenID)
	switch mode {
	case authorizerModeOn:
		break // fallthrough to the next check.
	case authorizerModeAllowAll:
		return nil
	case authorizerModeV222:
		return errFn()
	default:
		err := errors.AssertionFailedf("unknown authorizer mode: %d", mode)
		logcrash.ReportOrPanic(ctx, &a.settings.SV, "%v", err)
		return err
	}

	if !tenantcapabilities.MustGetBoolByID(
		cp, tenantcapabilities.CanViewNodeInfo,
	) {
		return errFn()
	}
	return nil
}

func (a *Authorizer) HasTSDBQueryCapability(ctx context.Context, tenID roachpb.TenantID) error {
	if tenID.IsSystem() {
		return nil
	}
	errFn := func() error {
		return errors.Newf("client tenant does not have capability to query timeseries data")
	}

	cp, mode := a.getMode(ctx, tenID)
	switch mode {
	case authorizerModeOn:
		break // fallthrough to the next check.
	case authorizerModeAllowAll:
		return nil
	case authorizerModeV222:
		return errFn()
	default:
		err := errors.AssertionFailedf("unknown authorizer mode: %d", mode)
		logcrash.ReportOrPanic(ctx, &a.settings.SV, "%v", err)
		return err
	}

	if !tenantcapabilities.MustGetBoolByID(
		cp, tenantcapabilities.CanViewTSDBMetrics,
	) {
		return errFn()
	}
	return nil
}

func (a *Authorizer) HasNodelocalStorageCapability(
	ctx context.Context, tenID roachpb.TenantID,
) error {
	if tenID.IsSystem() {
		return nil
	}
	errFn := func() error {
		return errors.Newf("client tenant does not have capability to use nodelocal storage")
	}
	cp, mode := a.getMode(ctx, tenID)
	switch mode {
	case authorizerModeOn:
		break // fallthrough to the next check.
	case authorizerModeAllowAll:
		return nil
	case authorizerModeV222:
		return errFn()
	default:
		err := errors.AssertionFailedf("unknown authorizer mode: %d", mode)
		logcrash.ReportOrPanic(ctx, &a.settings.SV, "%v", err)
		return err
	}

	if !tenantcapabilities.MustGetBoolByID(
		cp, tenantcapabilities.CanUseNodelocalStorage,
	) {
		return errFn()
	}
	return nil
}

// IsExemptFromRateLimiting returns true if the tenant is not subject to rate limiting.
func (a *Authorizer) IsExemptFromRateLimiting(ctx context.Context, tenID roachpb.TenantID) bool {
	if tenID.IsSystem() {
		return true
	}
	cp, mode := a.getMode(ctx, tenID)
	switch mode {
	case authorizerModeOn:
		break // fallthrough to the next check.
	case authorizerModeAllowAll:
		return true
	case authorizerModeV222:
		return false
	default:
		err := errors.AssertionFailedf("unknown authorizer mode: %d", mode)
		logcrash.ReportOrPanic(ctx, &a.settings.SV, "%v", err)
		return false
	}

	return tenantcapabilities.MustGetBoolByID(cp, tenantcapabilities.ExemptFromRateLimiting)
}

// getMode retrieves the authorization mode.
func (a *Authorizer) getMode(
	ctx context.Context, tid roachpb.TenantID,
) (cp *tenantcapabilitiespb.TenantCapabilities, selectedMode authorizerModeType) {
	// We prioritize what the cluster setting tells us.
	selectedMode = authorizerModeType(authorizerMode.Get(&a.settings.SV))
	if selectedMode == authorizerModeOn {
		if !a.settings.Version.IsActive(ctx, clusterversion.V23_1TenantCapabilities) {
			// If the cluster hasn't been upgraded to v23.1 with
			// capabilities yet, the capabilities won't be ready for use. In
			// that case, fall back to the previous behavior.
			selectedMode = authorizerModeV222
		}
	}

	// If the mode is "on", we need to check the capabilities. Are they
	// available?
	if selectedMode == authorizerModeOn {
		a.Lock()
		reader := a.capabilitiesReader
		a.Unlock()
		if reader == nil {
			// The server has started but the reader hasn't started/bound
			// yet. Block requests that would need specific capabilities.
			log.Warningf(ctx, "capability check for tenant %s before capability reader exists, assuming capability is unavailable", tid)
			selectedMode = authorizerModeV222
		} else {
			// We have a reader. Did we get data from the rangefeed yet?
			var found bool
			cp, found = reader.GetCapabilities(tid)
			if !found {
				// No data from the rangefeed yet. Assume caps are still
				// unavailable.
				log.VInfof(ctx, 2,
					"no capability information for tenant %s; requests that require capabilities may be denied",
					tid)
				selectedMode = authorizerModeV222
			}
		}
	}
	return cp, selectedMode
}

func (a *Authorizer) HasProcessDebugCapability(ctx context.Context, tenID roachpb.TenantID) error {
	if tenID.IsSystem() {
		return nil
	}
	errFn := func() error {
		return errors.New("client tenant does not have capability to debug the process")
	}
	cp, mode := a.getMode(ctx, tenID)
	switch mode {
	case authorizerModeOn:
		break // fallthrough to the next check.
	case authorizerModeAllowAll:
		return nil
	case authorizerModeV222:
		return errFn()
	default:
		err := errors.AssertionFailedf("unknown authorizer mode: %d", mode)
		logcrash.ReportOrPanic(ctx, &a.settings.SV, "%v", err)
		return err
	}

	if !tenantcapabilities.MustGetBoolByID(
		cp, tenantcapabilities.CanDebugProcess,
	) {
		return errFn()
	}
	return nil
}
