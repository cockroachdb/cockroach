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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	tenID roachpb.TenantID, fullMethod string, req interface{},
) error {
	switch fullMethod {
	case "/cockroach.roachpb.Internal/Batch":
		return a.authBatch(tenID, req.(*roachpb.BatchRequest))

	case "/cockroach.roachpb.Internal/RangeLookup":
		return a.authRangeLookup(tenID, req.(*roachpb.RangeLookupRequest))

	case "/cockroach.roachpb.Internal/RangeFeed", "/cockroach.roachpb.Internal/MuxRangeFeed":
		return a.authRangeFeed(tenID, req.(*roachpb.RangeFeedRequest))
	case "/cockroach.roachpb.Internal/GossipSubscription":
		return a.authGossipSubscription(tenID, req.(*roachpb.GossipSubscriptionRequest))

	case "/cockroach.roachpb.Internal/TokenBucket":
		return a.authTokenBucket(tenID, req.(*roachpb.TokenBucketRequest))

	case "/cockroach.roachpb.Internal/TenantSettings":
		return a.authTenantSettings(tenID, req.(*roachpb.TenantSettingsRequest))

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

	case "/cockroach.server.serverpb.Status/CancelLocalSession":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/CancelQuery":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/TenantRanges":
		return a.authTenantRanges(tenID)

	case "/cockroach.server.serverpb.Status/CancelLocalQuery":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/TransactionContentionEvents":
		return a.authTenant(tenID)

	case "/cockroach.roachpb.Internal/GetSpanConfigs":
		return a.authGetSpanConfigs(tenID, req.(*roachpb.GetSpanConfigsRequest))

	case "/cockroach.roachpb.Internal/SpanConfigConformance":
		return a.authSpanConfigConformance(tenID, req.(*roachpb.SpanConfigConformanceRequest))

	case "/cockroach.roachpb.Internal/GetAllSystemSpanConfigsThatApply":
		return a.authGetAllSystemSpanConfigsThatApply(tenID, req.(*roachpb.GetAllSystemSpanConfigsThatApplyRequest))

	case "/cockroach.roachpb.Internal/UpdateSpanConfigs":
		return a.authUpdateSpanConfigs(tenID, req.(*roachpb.UpdateSpanConfigsRequest))

	case "/cockroach.roachpb.Internal/GetRangeDescriptors":
		return a.authGetRangeDescriptors(tenID, req.(*roachpb.GetRangeDescriptorsRequest))

	case "/cockroach.server.serverpb.Status/HotRangesV2":
		return a.authHotRangesV2(tenID)

	case "/cockroach.server.serverpb.Status/NodesUI":
		return a.authCapability(tenID)

	case "/cockroach.server.serverpb.Admin/Liveness":
		return a.authCapability(tenID)

	case "/cockroach.ts.tspb.TimeSeries/Query":
		return a.authCapability(tenID)

	default:
		return authErrorf("unknown method %q", fullMethod)
	}
}

func checkSpanBounds(rSpan, tenSpan roachpb.RSpan) error {
	if !tenSpan.ContainsKeyRange(rSpan.Key, rSpan.EndKey) {
		return authErrorf("requested key span %s not fully contained in tenant keyspace %s", rSpan, tenSpan)
	}
	return nil
}

// authBatch authorizes the provided tenant to invoke the Batch RPC with the
// provided args.
func (a tenantAuthorizer) authBatch(tenID roachpb.TenantID, args *roachpb.BatchRequest) error {
	// Consult reqAllowed to determine whether each request in the batch
	// is permitted. If not, reject the entire batch.
	for _, ru := range args.Requests {
		switch ru.GetInner().(type) {
		case
			*roachpb.AddSSTableRequest,
			*roachpb.AdminChangeReplicasRequest,
			*roachpb.AdminRelocateRangeRequest,
			*roachpb.AdminScatterRequest,
			*roachpb.AdminSplitRequest,
			*roachpb.AdminTransferLeaseRequest,
			*roachpb.AdminUnsplitRequest,
			*roachpb.ClearRangeRequest,
			*roachpb.ConditionalPutRequest,
			*roachpb.DeleteRangeRequest,
			*roachpb.DeleteRequest,
			*roachpb.EndTxnRequest,
			*roachpb.ExportRequest,
			*roachpb.GetRequest,
			*roachpb.HeartbeatTxnRequest,
			*roachpb.IncrementRequest,
			*roachpb.InitPutRequest,
			*roachpb.IsSpanEmptyRequest,
			*roachpb.LeaseInfoRequest,
			*roachpb.PutRequest,
			*roachpb.QueryIntentRequest,
			*roachpb.QueryLocksRequest,
			*roachpb.QueryTxnRequest,
			*roachpb.RangeStatsRequest,
			*roachpb.RefreshRangeRequest,
			*roachpb.RefreshRequest,
			*roachpb.ReverseScanRequest,
			*roachpb.RevertRangeRequest,
			*roachpb.ScanRequest:
			continue
		}
		return authErrorf("request [%s] not permitted", args.Summary())
	}

	// All keys in the request must reside within the tenant's keyspace.
	rSpan, err := keys.Range(args.Requests)
	if err != nil {
		return authError(err.Error())
	}
	tenSpan := tenantPrefix(tenID)
	return checkSpanBounds(rSpan, tenSpan)
}

func (a tenantAuthorizer) authGetRangeDescriptors(
	tenID roachpb.TenantID, args *roachpb.GetRangeDescriptorsRequest,
) error {
	return validateSpan(tenID, args.Span)
}

// authRangeLookup authorizes the provided tenant to invoke the RangeLookup RPC
// with the provided args.
func (a tenantAuthorizer) authRangeLookup(
	tenID roachpb.TenantID, args *roachpb.RangeLookupRequest,
) error {
	tenSpan := tenantPrefix(tenID)
	if !tenSpan.ContainsKey(args.Key) {
		return authErrorf("requested key %s not fully contained in tenant keyspace %s", args.Key, tenSpan)
	}
	return nil
}

// authRangeFeed authorizes the provided tenant to invoke the RangeFeed RPC with
// the provided args.
func (a tenantAuthorizer) authRangeFeed(
	tenID roachpb.TenantID, args *roachpb.RangeFeedRequest,
) error {
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
	tenID roachpb.TenantID, args *roachpb.GossipSubscriptionRequest,
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

// authCapability checks if the current tenant has the requested capability.
func (a tenantAuthorizer) authCapability(id roachpb.TenantID) error {
	// TODO(davidh): add capability-specific checks here that correspond to specific requests.
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
	tenID roachpb.TenantID, args *roachpb.TokenBucketRequest,
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
	tenID roachpb.TenantID, args *roachpb.TenantSettingsRequest,
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
	tenID roachpb.TenantID, args *roachpb.GetSpanConfigsRequest,
) error {
	for _, target := range args.Targets {
		if err := validateSpanConfigTarget(tenID, target); err != nil {
			return err
		}
	}
	return nil
}

// authUpdateSpanConfigs authorizes the provided tenant to invoke the
// UpdateSpanConfigs RPC with the provided args.
func (a tenantAuthorizer) authUpdateSpanConfigs(
	tenID roachpb.TenantID, args *roachpb.UpdateSpanConfigsRequest,
) error {
	for _, entry := range args.ToUpsert {
		if err := validateSpanConfigTarget(tenID, entry.Target); err != nil {
			return err
		}
	}
	for _, target := range args.ToDelete {
		if err := validateSpanConfigTarget(tenID, target); err != nil {
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
	tenID roachpb.TenantID, args *roachpb.SpanConfigConformanceRequest,
) error {
	for _, sp := range args.Spans {
		if err := validateSpan(tenID, sp); err != nil {
			return err
		}
	}
	return nil
}

// validateSpanConfigTarget validates that the tenant is authorized to interact
// with the supplied span config target. In particular, span targets must be
// wholly contained within the tenant keyspace and system span config targets
// must be well-formed.
func validateSpanConfigTarget(
	tenID roachpb.TenantID, spanConfigTarget roachpb.SpanConfigTarget,
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
		return validateSpan(tenID, *spanConfigTarget.GetSpan())
	case *roachpb.SpanConfigTarget_SystemSpanConfigTarget:
		return validateSystemTarget(*spanConfigTarget.GetSystemSpanConfigTarget())
	default:
		return errors.AssertionFailedf("unknown span config target type")
	}
}

func validateSpan(tenID roachpb.TenantID, sp roachpb.Span) error {
	tenSpan := tenantPrefix(tenID)
	rSpan, err := keys.SpanAddr(sp)
	if err != nil {
		return authError(err.Error())
	}
	return checkSpanBounds(rSpan, tenSpan)
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
	prefix := roachpb.RKey(keys.MakeTenantPrefix(tenID))
	return roachpb.RSpan{
		Key:    prefix,
		EndKey: prefix.PrefixEnd(),
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
