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
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// tenantAuth is an auth policy used for RPCs sent to a node's tenant RPC
// server. It validates that client TLS certificates provide a tenant ID and
// that the RPC being invoked is compatable with that tenant.
type tenantAuth struct{}

// kvAuth implements the auth interface.
func (a tenantAuth) AuthUnary() grpc.UnaryServerInterceptor   { return a.unaryInterceptor }
func (a tenantAuth) AuthStream() grpc.StreamServerInterceptor { return a.streamInterceptor }

func (a tenantAuth) unaryInterceptor(
	ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (interface{}, error) {
	tenID, err := a.tenantFromCert(ctx)
	if err != nil {
		return nil, err
	}
	ctx = contextWithTenant(ctx, tenID)
	if err := a.authRequest(ctx, tenID, info.FullMethod, req); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (a tenantAuth) streamInterceptor(
	srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	ctx := ss.Context()
	tenID, err := a.tenantFromCert(ctx)
	if err != nil {
		return err
	}
	ctx = contextWithTenant(ctx, tenID)
	return handler(srv, &wrappedServerStream{
		ServerStream: ss,
		ctx:          ctx,
		recv: func(m interface{}) error {
			if err := ss.RecvMsg(m); err != nil {
				return err
			}
			// 'm' is now populated and contains the request from the client.
			return a.authRequest(ctx, tenID, info.FullMethod, m)
		},
	})
}

func (a tenantAuth) tenantFromCert(ctx context.Context) (roachpb.TenantID, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return roachpb.TenantID{}, errTLSInfoMissing
	}

	tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return roachpb.TenantID{}, errTLSInfoMissing
	}

	commonName := tlsInfo.State.PeerCertificates[0].Subject.CommonName
	tenID, err := strconv.ParseUint(commonName, 10, 64)
	if err != nil {
		return roachpb.TenantID{}, authErrorf("could not parse tenant ID from Common Name (CN): %s", err)
	}
	if tenID < roachpb.MinTenantID.ToUint64() || tenID > roachpb.MaxTenantID.ToUint64() {
		return roachpb.TenantID{}, authErrorf("invalid tenant ID %d in Common Name (CN)", tenID)
	}
	return roachpb.MakeTenantID(tenID), nil
}

func (a tenantAuth) authRequest(
	ctx context.Context, tenID roachpb.TenantID, fullMethod string, req interface{},
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

	case "/cockroach.rpc.Heartbeat/Ping":
		return nil // no authorization

	default:
		return authErrorf("unknown method %q", fullMethod)
	}
}

// authBatch authorizes the provided tenant to invoke the Batch RPC with the
// provided args.
func (a tenantAuth) authBatch(tenID roachpb.TenantID, args *roachpb.BatchRequest) error {
	rSpan, err := keys.Range(args.Requests)
	if err != nil {
		return authError(err.Error())
	}
	tenSpan := tenantPrefix(tenID)
	if tenSpan.ContainsKeyRange(rSpan.Key, rSpan.EndKey) {
		return nil
	}
	for _, allow := range batchSpanAllowlist {
		if rSpan.Equal(allow) {
			return nil
		}
	}
	return authErrorf("requested key span %s not fully contained in tenant keyspace %s", rSpan, tenSpan)
}

// batchSpanAllowlist contains spans outside of a tenant's keyspace that Batch
// RPC invocations are allowed to touch.
var batchSpanAllowlist = []roachpb.RSpan{
	// TODO(nvanbenschoten): Explore whether we can get rid of this by no longer
	// reading this key in sqlServer.start.
	{
		Key:    roachpb.RKey(keys.BootstrapVersionKey),
		EndKey: roachpb.RKey(keys.BootstrapVersionKey.Next()),
	},
}

// authRangeLookup authorizes the provided tenant to invoke the RangeLookup RPC
// with the provided args.
func (a tenantAuth) authRangeLookup(
	tenID roachpb.TenantID, args *roachpb.RangeLookupRequest,
) error {
	tenSpan := tenantPrefix(tenID)
	if tenSpan.ContainsKey(args.Key) {
		return nil
	}
	for _, allow := range rangeLookupKeyAllowlist {
		if args.Key.Equal(allow) {
			return nil
		}
	}
	return authErrorf("requested key %s not fully contained in tenant keyspace %s", args.Key, tenSpan)
}

// rangeLookupKeyAllowlist contains keys outside of a tenant's keyspace that
// RangeLookup RPC invocations are allowed to touch.
var rangeLookupKeyAllowlist = []roachpb.Key{
	// TODO(nvanbenschoten): Explore whether we can get rid of this by no longer
	// reading this key in sqlServer.start.
	keys.BootstrapVersionKey,
}

// authRangeFeed authorizes the provided tenant to invoke the RangeFeed RPC with
// the provided args.
func (a tenantAuth) authRangeFeed(tenID roachpb.TenantID, args *roachpb.RangeFeedRequest) error {
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
func (a tenantAuth) authGossipSubscription(
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

// gossipSubscriptionPatternAllowlist contains keys outside of a tenant's
// keyspace that GossipSubscription RPC invocations are allowed to touch.
// WIP: can't import gossip directly.
var gossipSubscriptionPatternAllowlist = []string{
	"node:.*",
	"system-db",
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
