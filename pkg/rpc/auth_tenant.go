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
	"github.com/cockroachdb/logtags"
	"google.golang.org/grpc"
)

// tenantAuthorizer authorizes RPCs sent by tenants to a node's tenant RPC
// server, that is, it ensures that the request only accesses resources
// available to the tenant.
type tenantAuthorizer struct {
	tenantID roachpb.TenantID
}

func tenantFromCommonName(commonName string) (roachpb.TenantID, error) {
	tenID, err := strconv.ParseUint(commonName, 10, 64)
	if err != nil {
		return roachpb.TenantID{}, authErrorf("could not parse tenant ID from Common Name (CN): %s", err)
	}
	if tenID < roachpb.MinTenantID.ToUint64() || tenID > roachpb.MaxTenantID.ToUint64() {
		return roachpb.TenantID{}, authErrorf("invalid tenant ID %d in Common Name (CN)", tenID)
	}
	return roachpb.MakeTenantID(tenID), nil
}

// authorize enforces a security boundary around endpoints that tenants
// request from the host KV node.
func (a tenantAuthorizer) authorize(
	tenID roachpb.TenantID, fullMethod string, req interface{},
) error {
	switch fullMethod {
	case "/cockroach.roachpb.Internal/Batch":
		return a.authBatch(tenID, req.(*roachpb.BatchRequest))

	case "/cockroach.roachpb.Internal/RangeLookup":
		return a.authRangeLookup(tenID, req.(*roachpb.RangeLookupRequest))

	case "/cockroach.roachpb.Internal/RangeFeed":
		return a.authRangeFeed(tenID, req.(*roachpb.RangeFeedRequest))

	case "/cockroach.roachpb.Internal/GossipSubscription":
		return a.authGossipSubscription(tenID, req.(*roachpb.GossipSubscriptionRequest))

	case "/cockroach.roachpb.Internal/TokenBucket":
		return a.authTokenBucket(tenID, req.(*roachpb.TokenBucketRequest))

	case "/cockroach.rpc.Heartbeat/Ping":
		return nil // no restriction to usage of this endpoint by tenants

	case "/cockroach.server.serverpb.Status/Regions":
		return nil // no restriction to usage of this endpoint by tenants

	case "/cockroach.server.serverpb.Status/Statements":
		return a.authTenant(tenID)

	case "/cockroach.server.serverpb.Status/ListSessions":
		return a.authTenant(tenID)

	case "/cockroach.roachpb.Internal/GetSpanConfigs":
		return a.authGetSpanConfigs(tenID, req.(*roachpb.GetSpanConfigsRequest))

	case "/cockroach.roachpb.Internal/UpdateSpanConfigs":
		return a.authUpdateSpanConfigs(tenID, req.(*roachpb.UpdateSpanConfigsRequest))

	default:
		return authErrorf("unknown method %q", fullMethod)
	}
}

// authBatch authorizes the provided tenant to invoke the Batch RPC with the
// provided args.
func (a tenantAuthorizer) authBatch(tenID roachpb.TenantID, args *roachpb.BatchRequest) error {
	// Consult reqMethodAllowlist to determine whether each request in the batch
	// is permitted. If not, reject the entire batch.
	for _, ru := range args.Requests {
		if !reqAllowed(ru.GetInner()) {
			return authErrorf("request [%s] not permitted", args.Summary())
		}
	}

	// All keys in the request must reside within the tenant's keyspace.
	rSpan, err := keys.Range(args.Requests)
	if err != nil {
		return authError(err.Error())
	}
	tenSpan := tenantPrefix(tenID)
	if !tenSpan.ContainsKeyRange(rSpan.Key, rSpan.EndKey) {
		return authErrorf("requested key span %s not fully contained in tenant keyspace %s", rSpan, tenSpan)
	}
	return nil
}

var reqMethodAllowlist = [...]bool{
	roachpb.Get:            true,
	roachpb.Put:            true,
	roachpb.ConditionalPut: true,
	roachpb.Increment:      true,
	roachpb.Delete:         true,
	roachpb.DeleteRange:    true,
	roachpb.ClearRange:     true,
	roachpb.Scan:           true,
	roachpb.ReverseScan:    true,
	roachpb.EndTxn:         true,
	roachpb.HeartbeatTxn:   true,
	roachpb.QueryTxn:       true,
	roachpb.QueryIntent:    true,
	roachpb.InitPut:        true,
	roachpb.AddSSTable:     true,
	roachpb.Export:         true,
	roachpb.Refresh:        true,
	roachpb.RefreshRange:   true,
}

func reqAllowed(r roachpb.Request) bool {
	m := int(r.Method())
	return m < len(reqMethodAllowlist) && reqMethodAllowlist[m]
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
	if !tenSpan.ContainsKeyRange(rSpan.Key, rSpan.EndKey) {
		return authErrorf("requested key span %s not fully contained in tenant keyspace %s", rSpan, tenSpan)
	}
	return nil
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

// gossipSubscriptionPatternAllowlist contains keys outside of a tenant's
// keyspace that GossipSubscription RPC invocations are allowed to touch.
// WIP: can't import gossip directly.
var gossipSubscriptionPatternAllowlist = []string{
	"cluster-id",
	"node:.*",
	"system-db",
}

// authTokenBucket authorizes the provided tenant to invoke the
// TokenBucket RPC with the provided args.
func (a tenantAuthorizer) authTokenBucket(
	tenID roachpb.TenantID, args *roachpb.TokenBucketRequest,
) error {
	if args.TenantID == 0 {
		return authErrorf("token bucket request with unspecified tenant not permitted")
	}
	if argTenant := roachpb.MakeTenantID(args.TenantID); argTenant != tenID {
		return authErrorf("token bucket request for tenant %s not permitted", argTenant)
	}
	return nil
}

// authGetSpanConfigs authorizes the provided tenant to invoke the
// GetSpanConfigs RPC with the provided args.
func (a tenantAuthorizer) authGetSpanConfigs(
	tenID roachpb.TenantID, args *roachpb.GetSpanConfigsRequest,
) error {
	tenSpan := tenantPrefix(tenID)
	for _, sp := range args.Spans {
		rSpan, err := keys.SpanAddr(sp)
		if err != nil {
			return authError(err.Error())
		}
		if !tenSpan.ContainsKeyRange(rSpan.Key, rSpan.EndKey) {
			return authErrorf("requested key span %s not fully contained in tenant keyspace %s", rSpan, tenSpan)
		}
	}
	return nil
}

// authUpdateSpanConfigs authorizes the provided tenant to invoke the
// UpdateSpanConfigs RPC with the provided args.
func (a tenantAuthorizer) authUpdateSpanConfigs(
	tenID roachpb.TenantID, args *roachpb.UpdateSpanConfigsRequest,
) error {
	tenSpan := tenantPrefix(tenID)
	validate := func(sp roachpb.Span) error {
		rSpan, err := keys.SpanAddr(sp)
		if err != nil {
			return authError(err.Error())
		}
		if !tenSpan.ContainsKeyRange(rSpan.Key, rSpan.EndKey) {
			return authErrorf("requested key span %s not fully contained in tenant keyspace %s", rSpan, tenSpan)
		}
		return nil
	}

	for _, entry := range args.ToUpsert {
		if err := validate(entry.Span); err != nil {
			return err
		}
	}
	for _, span := range args.ToDelete {
		if err := validate(span); err != nil {
			return err
		}
	}

	return nil
}

func contextWithTenant(ctx context.Context, tenID roachpb.TenantID) context.Context {
	ctx = roachpb.NewContextForTenant(ctx, tenID)
	ctx = logtags.AddTag(ctx, "tenant", tenID.String())
	return ctx
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
// intercept each messsage and inject custom validation logic.
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
