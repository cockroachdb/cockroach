// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"google.golang.org/grpc"
)

// tenantAuthorizer authorizes RPCs sent by tenants to a node's tenant RPC
// server, that is, it ensures that the request only accesses resources
// available to the tenant.
type tenantAuthorizer struct {
	// tenantID is the tenant ID for the current node.
	// Equals SystemTenantID when running a KV node.
	tenantID roachpb.TenantID
	// capabilitiesAuthorizer is used to perform capability checks for incoming
	// tenant requests. Capability checks are only performed when running on a KV
	// node; the TenantRPCAuthorizer no-ops on secondary tenants.
	capabilitiesAuthorizer tenantcapabilities.Authorizer
}

func tenantIDFromString(commonName, field string) (roachpb.TenantID, error) {
	tenID, err := strconv.ParseUint(commonName, 10, 64)
	if err != nil {
		return roachpb.TenantID{}, authErrorf("could not parse tenant ID from %s: %s", field, err)
	}
	if tenID < roachpb.MinTenantID.ToUint64() || tenID > roachpb.MaxTenantID.ToUint64() {
		return roachpb.TenantID{}, authErrorf("invalid tenant ID %d in %s", tenID, field)
	}
	return roachpb.MustMakeTenantID(tenID), nil
}

// authorize enforces a security boundary around endpoints that tenants
// request from the host KV node or other tenant SQL pod.
func (a tenantAuthorizer) authorize(
	ctx context.Context,
	sv *settings.Values,
	tenID roachpb.TenantID,
	fullMethod string,
	req interface{},
) error {
	switch fullMethod {
	case "/cockroach.roachpb.Internal/Batch":
		return a.authBatch(ctx, sv, tenID, req.(*kvpb.BatchRequest))

	case "/cockroach.roachpb.Internal/RangeLookup":
		return a.authRangeLookup(ctx, tenID, req.(*kvpb.RangeLookupRequest))

	case "/cockroach.roachpb.Internal/RangeFeed", "/cockroach.roachpb.Internal/MuxRangeFeed":
		return a.authRangeFeed(tenID, req.(*kvpb.RangeFeedRequest))
	case "/cockroach.roachpb.Internal/GossipSubscription":
		return a.authGossipSubscription(tenID, req.(*kvpb.GossipSubscriptionRequest))

	case "/cockroach.roachpb.Internal/TokenBucket":
		return a.authTokenBucket(tenID, req.(*kvpb.TokenBucketRequest))

	case "/cockroach.roachpb.Internal/TenantSettings":
		return a.authTenantSettings(tenID, req.(*kvpb.TenantSettingsRequest))

	case "/cockroach.rpc.Heartbeat/Ping":
		return nil // no restriction to usage of this endpoint by tenants

	case "/cockroach.server.serverpb.Status/Regions":
		return nil // no restriction to usage of this endpoint by tenants

	case "/cockroach.server.serverpb.Status/NodeLocality":
		return nil // no restriction to usage of this endpoint by tenants

	case "/cockroach.server.serverpb.Status/Statements":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/CombinedStatementStats":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/ResetSQLStats":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/ListContentionEvents":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/ListLocalContentionEvents":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/ListSessions":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/ListLocalSessions":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/IndexUsageStatistics":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/CancelSession":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/CancelQuery":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/TenantRanges":
		return a.authTenantRanges(tenID)

	case "/cockroach.server.serverpb.Status/TransactionContentionEvents":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/SpanStats":
		return a.authSpanStats(ctx, tenID, req.(*roachpb.SpanStatsRequest))

	case "/cockroach.roachpb.Internal/GetSpanConfigs":
		return a.authGetSpanConfigs(ctx, tenID, req.(*roachpb.GetSpanConfigsRequest))

	case "/cockroach.roachpb.Internal/SpanConfigConformance":
		return a.authSpanConfigConformance(ctx, tenID, req.(*roachpb.SpanConfigConformanceRequest))

	case "/cockroach.roachpb.Internal/GetAllSystemSpanConfigsThatApply":
		return a.authGetAllSystemSpanConfigsThatApply(tenID, req.(*roachpb.GetAllSystemSpanConfigsThatApplyRequest))

	case "/cockroach.roachpb.Internal/UpdateSpanConfigs":
		return a.authUpdateSpanConfigs(ctx, tenID, req.(*roachpb.UpdateSpanConfigsRequest))

	case "/cockroach.roachpb.Internal/GetRangeDescriptors":
		return a.authGetRangeDescriptors(ctx, tenID, req.(*kvpb.GetRangeDescriptorsRequest))

	case "/cockroach.server.serverpb.Status/HotRangesV2":
		return a.authHotRangesV2(tenID)

	case "/cockroach.server.serverpb.Status/Nodes":
		return a.capabilitiesAuthorizer.HasNodeStatusCapability(ctx, tenID)

	case "/cockroach.server.serverpb.Admin/Liveness":
		return a.capabilitiesAuthorizer.HasNodeStatusCapability(ctx, tenID)

	case "/cockroach.ts.tspb.TimeSeries/Query":
		return a.authTSDBQuery(ctx, tenID, req.(*tspb.TimeSeriesQueryRequest))

	case "/cockroach.blobs.Blob/List",
		"/cockroach.blobs.Blob/Delete",
		"/cockroach.blobs.Blob/Stat",
		"/cockroach.blobs.Blob/GetStream",
		"/cockroach.blobs.Blob/PutStream":
		return a.capabilitiesAuthorizer.HasNodelocalStorageCapability(ctx, tenID)

	case "/cockroach.server.serverpb.Admin/ReadFromTenantInfo":
		// NB: we don't check anything here as every tenant, even those who do not
		// have HasCrossTenantRead, will call this even if only to learn that they
		// are not a reader tenant.
		return nil

	default:
		return authErrorf("unknown method %q", fullMethod)
	}
}

func checkSpanBounds(rSpan, tenSpan roachpb.RSpan) error {
	if outsideTenant(rSpan, tenSpan) {
		return spanErr(rSpan, tenSpan)
	}
	return nil
}

func outsideTenant(rSpan, tenSpan roachpb.RSpan) bool {
	return !tenSpan.ContainsKeyRange(rSpan.Key, rSpan.EndKey)
}

func spanErr(rSpan, tenSpan roachpb.RSpan) error {
	return authErrorf("requested key span %s not fully contained in tenant keyspace %s", rSpan, tenSpan)
}

// authBatch authorizes the provided tenant to invoke the Batch RPC with the
// provided args.
func (a tenantAuthorizer) authBatch(
	ctx context.Context, sv *settings.Values, tenID roachpb.TenantID, args *kvpb.BatchRequest,
) error {
	if err := a.capabilitiesAuthorizer.HasCapabilityForBatch(ctx, tenID, args); err != nil {
		if errors.HasAssertionFailure(err) {
			logcrash.ReportOrPanic(ctx, sv, "%v", err)
		}
		return authError(err.Error())
	}

	// All keys in the request must reside within the tenant's keyspace.
	rSpan, err := keys.Range(args.Requests)
	if err != nil {
		return authError(err.Error())
	}
	tenSpan := tenantPrefix(tenID)

	if outsideTenant(rSpan, tenSpan) {
		if args.IsReadOnly() && a.capabilitiesAuthorizer.HasCrossTenantRead(ctx, tenID, rSpan.Key) {
			return nil
		}
		return spanErr(rSpan, tenSpan)
	}
	return nil
}

func (a tenantAuthorizer) authGetRangeDescriptors(
	ctx context.Context, tenID roachpb.TenantID, args *kvpb.GetRangeDescriptorsRequest,
) error {
	return validateSpan(ctx, tenID, args.Span, true, a)
}

func (a tenantAuthorizer) authSpanStats(
	ctx context.Context, tenID roachpb.TenantID, args *roachpb.SpanStatsRequest,
) error {
	for _, span := range args.Spans {
		err := validateSpan(ctx, tenID, span, true, a)
		if err != nil {
			return err
		}
	}
	return nil
}

// authRangeLookup authorizes the provided tenant to invoke the RangeLookup RPC
// with the provided args.
func (a tenantAuthorizer) authRangeLookup(
	ctx context.Context, tenID roachpb.TenantID, args *kvpb.RangeLookupRequest,
) error {
	tenSpan := tenantPrefix(tenID)
	if !tenSpan.ContainsKey(args.Key) {
		// Allow it anyway if the tenant can read other tenants.
		if a.capabilitiesAuthorizer.HasCrossTenantRead(ctx, tenID, args.Key) {
			return nil
		}
		return authErrorf("requested key %s not fully contained in tenant keyspace %s", args.Key, tenSpan)
	}
	return nil
}

// authRangeFeed authorizes the provided tenant to invoke the RangeFeed RPC with
// the provided args.
func (a tenantAuthorizer) authRangeFeed(tenID roachpb.TenantID, args *kvpb.RangeFeedRequest) error {
	rSpan, err := keys.SpanAddr(args.Span)
	if err != nil {
		return authError(err.Error())
	}
	tenSpan := tenantPrefix(tenID)
	return checkSpanBounds(rSpan, tenSpan)
}

// authGossipSubscription authorizes the provided tenant to invoke the
// GossipSubscription RPC with the provided args.
func (a tenantAuthorizer) authGossipSubscription(
	tenID roachpb.TenantID, args *kvpb.GossipSubscriptionRequest,
) error {
	for _, pat := range args.Patterns {
		allowed := false
		for _, allow := range gossipSubscriptionPatternAllowlist {
			if pat == allow {
				allowed = true
				break
			}
		}
		if !allowed {
			return authErrorf("requested pattern %q not permitted", pat)
		}
	}
	return nil
}

// authTenant checks if the given tenantID matches the one the
// authorizer was initialized with. This authorizer is used for
// endpoints that should remain within a single tenant's pods.
func (a tenantAuthorizer) authTenant(id roachpb.TenantID) error {
	if a.tenantID != id {
		return authErrorf("request from tenant %s not permitted on tenant %s", id, a.tenantID)
	}
	return nil
}

// gossipSubscriptionPatternAllowlist contains keys outside of a tenant's
// keyspace that GossipSubscription RPC invocations are allowed to touch.
// WIP: can't import gossip directly.
var gossipSubscriptionPatternAllowlist = []string{
	"cluster-id",
	"node:.*",
	"store:.*",
	"system-db",
}

// authTenantRanges authorizes the provided tenant to invoke the
// TenantRanges RPC with the provided args. It requires that an authorized
// tenantID has been set.
func (a tenantAuthorizer) authTenantRanges(tenID roachpb.TenantID) error {
	if !tenID.IsSet() {
		return authErrorf("tenant ranges request with unspecified tenant not permitted.")
	}
	return nil
}

// authTokenBucket authorizes the provided tenant to invoke the
// TokenBucket RPC with the provided args.
func (a tenantAuthorizer) authTokenBucket(
	tenID roachpb.TenantID, args *kvpb.TokenBucketRequest,
) error {
	if args.TenantID == 0 {
		return authErrorf("token bucket request with unspecified tenant not permitted")
	}
	if argTenant := roachpb.MustMakeTenantID(args.TenantID); argTenant != tenID {
		return authErrorf("token bucket request for tenant %s not permitted", argTenant)
	}
	return nil
}

// authTenantSettings authorizes the provided tenant to invoke the
// TenantSettings RPC with the provided args.
func (a tenantAuthorizer) authTenantSettings(
	tenID roachpb.TenantID, args *kvpb.TenantSettingsRequest,
) error {
	if !args.TenantID.IsSet() {
		return authErrorf("tenant settings request with unspecified tenant not permitted")
	}
	if args.TenantID != tenID {
		return authErrorf("tenant settings request for tenant %s not permitted", args.TenantID)
	}
	return nil
}

// authGetAllSystemSpanConfigsThatApply authorizes the provided tenant to invoke
// GetAllSystemSpanConfigs RPC with the provided args.
func (a tenantAuthorizer) authGetAllSystemSpanConfigsThatApply(
	tenID roachpb.TenantID, args *roachpb.GetAllSystemSpanConfigsThatApplyRequest,
) error {
	if !args.TenantID.IsSet() {
		return authErrorf(
			"GetAllSystemSpanConfigsThatApply request with unspecified tenant not permitted",
		)
	}
	if args.TenantID != tenID {
		return authErrorf(
			"GetAllSystemSpanConfigsThatApply request for tenant %s not permitted", args.TenantID,
		)
	}
	return nil
}

// authGetSpanConfigs authorizes the provided tenant to invoke the
// GetSpanConfigs RPC with the provided args.
func (a tenantAuthorizer) authGetSpanConfigs(
	ctx context.Context, tenID roachpb.TenantID, args *roachpb.GetSpanConfigsRequest,
) error {
	for _, target := range args.Targets {
		if err := validateSpanConfigTarget(ctx, tenID, target, true, a); err != nil {
			return err
		}
	}
	return nil
}

// authUpdateSpanConfigs authorizes the provided tenant to invoke the
// UpdateSpanConfigs RPC with the provided args.
func (a tenantAuthorizer) authUpdateSpanConfigs(
	ctx context.Context, tenID roachpb.TenantID, args *roachpb.UpdateSpanConfigsRequest,
) error {
	for _, entry := range args.ToUpsert {
		if err := validateSpanConfigTarget(ctx, tenID, entry.Target, false, a); err != nil {
			return err
		}
	}
	for _, target := range args.ToDelete {
		if err := validateSpanConfigTarget(ctx, tenID, target, false, a); err != nil {
			return err
		}
	}

	return nil
}

// authHotRangesV2 authorizes the provided tenant to invoke the
// HotRangesV2 RPC with the provided args. It requires that an authorized
// tenantID has been set.
func (a tenantAuthorizer) authHotRangesV2(tenID roachpb.TenantID) error {
	if !tenID.IsSet() {
		return authErrorf("hot ranges request with unspecified tenant not permitted")
	}
	return nil
}

// authSpanConfigConformance authorizes the provided tenant to invoke the
// SpanConfigConformance RPC with the provided args.
func (a tenantAuthorizer) authSpanConfigConformance(
	ctx context.Context, tenID roachpb.TenantID, args *roachpb.SpanConfigConformanceRequest,
) error {
	for _, sp := range args.Spans {
		if err := validateSpan(ctx, tenID, sp, false, a); err != nil {
			return err
		}
	}
	return nil
}

// authTSDBQuery authorizes the provided tenant to invoke the TSDB
// Query RPC with the provided args. A non-system tenant is allowed to
// query metric names in its registry as long as the TenantID is set on
// the query. System-level queries are permitted without a TenantID.
func (a tenantAuthorizer) authTSDBQuery(
	ctx context.Context, id roachpb.TenantID, request *tspb.TimeSeriesQueryRequest,
) error {
	for _, query := range request.Queries {
		if !query.TenantID.IsSet() {
			// If tenantID is unset, we make sure the tenant has permission
			// to query all metrics.
			if err := a.capabilitiesAuthorizer.HasTSDBAllMetricsCapability(ctx, id); err != nil {
				return authError(err.Error())
			}
		} else if !query.TenantID.Equal(id) {
			// Regardless of permissions, if you set a tenantID it should still
			// only be this one.
			return authErrorf("tsdb query with invalid tenant not permitted")
		}
	}
	if err := a.capabilitiesAuthorizer.HasTSDBQueryCapability(ctx, id); err != nil {
		return authError(err.Error())
	}
	return nil
}

// validateSpanConfigTarget validates that the tenant is authorized to interact
// with the supplied span config target. In particular, span targets must be
// wholly contained within the tenant keyspace and system span config targets
// must be well-formed.
func validateSpanConfigTarget(
	ctx context.Context,
	tenID roachpb.TenantID,
	spanConfigTarget roachpb.SpanConfigTarget,
	read bool,
	a tenantAuthorizer,
) error {
	validateSystemTarget := func(target roachpb.SystemSpanConfigTarget) error {
		if target.SourceTenantID != tenID {
			return authErrorf("malformed source tenant field")
		}

		if tenID == roachpb.SystemTenantID {
			// Nothing to validate, the system tenant is allowed to set system span
			// configurations over secondary tenants, itself, and the entire cluster.
			return nil
		}

		if target.IsEntireKeyspaceTarget() {
			return authErrorf("secondary tenants cannot target the entire keyspace")
		}

		if target.IsSpecificTenantKeyspaceTarget() &&
			target.Type.GetSpecificTenantKeyspace().TenantID != target.SourceTenantID {
			return authErrorf(
				"secondary tenants cannot interact with system span configurations of other tenants",
			)
		}

		return nil
	}

	switch spanConfigTarget.Union.(type) {
	case *roachpb.SpanConfigTarget_Span:
		return validateSpan(ctx, tenID, *spanConfigTarget.GetSpan(), read, a)
	case *roachpb.SpanConfigTarget_SystemSpanConfigTarget:
		return validateSystemTarget(*spanConfigTarget.GetSystemSpanConfigTarget())
	default:
		return errors.AssertionFailedf("unknown span config target type")
	}
}

func validateSpan(
	ctx context.Context, tenID roachpb.TenantID, sp roachpb.Span, isRead bool, a tenantAuthorizer,
) error {
	tenSpan := tenantPrefix(tenID)
	rSpan, err := keys.SpanAddr(sp)
	if err != nil {
		return authError(err.Error())
	}
	if outsideTenant(rSpan, tenSpan) {
		// Allow it anyway if the tenant can read other tenants.
		if isRead && a.capabilitiesAuthorizer.HasCrossTenantRead(ctx, tenID, rSpan.Key) {
			return nil
		}
		return spanErr(rSpan, tenSpan)
	}
	return nil
}

const tenantLoggingTag = "client-tenant"

// contextWithClientTenant inserts a tenant identifier in the context,
// identifying the tenant that's the client for an RPC. The identifier can be
// retrieved later through roachpb.ClientTenantFromContext(ctx). The tenant
// information is used both as a log tag, and also for purposes like rate
// limiting tenant calls.
func contextWithClientTenant(ctx context.Context, tenID roachpb.TenantID) context.Context {
	ctx = roachpb.ContextWithClientTenant(ctx, tenID)
	ctx = logtags.AddTag(ctx, tenantLoggingTag, tenID.String())
	return ctx
}

// contextWithoutClientTenant removes a tenant identifier in the context.
func contextWithoutClientTenant(ctx context.Context) context.Context {
	ctx = roachpb.ContextWithoutClientTenant(ctx)
	return logtags.RemoveTag(ctx, tenantLoggingTag)
}

func tenantPrefix(tenID roachpb.TenantID) roachpb.RSpan {
	// TODO(nvanbenschoten): consider caching this span.
	sp := keys.MakeTenantSpan(tenID)
	return roachpb.RSpan{
		Key:    roachpb.RKey(sp.Key),
		EndKey: roachpb.RKey(sp.EndKey),
	}
}

// wrappedServerStream is a thin wrapper around grpc.ServerStream that allows
// modifying its context and overriding its RecvMsg method. It can be used to
// intercept each message and inject custom validation logic.
type wrappedServerStream struct {
	grpc.ServerStream
	ctx  context.Context
	recv func(interface{}) error
}

// Context overrides the nested grpc.ServerStream.Context().
func (ss *wrappedServerStream) Context() context.Context {
	return ss.ctx
}

// RecvMsg overrides the nested grpc.ServerStream.RecvMsg().
func (ss *wrappedServerStream) RecvMsg(m interface{}) error {
	return ss.recv(m)
}
