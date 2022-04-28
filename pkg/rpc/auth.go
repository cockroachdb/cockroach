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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

var errTLSInfoMissing = authError("TLSInfo is not available in request context")

func authError(msg string) error {
	return status.Error(codes.Unauthenticated, msg)
}

func authErrorf(format string, a ...interface{}) error {
	return status.Errorf(codes.Unauthenticated, format, a...)
}

// kvAuth is the standard auth policy used for RPCs sent to an RPC server. It
// validates that client TLS certificate provided by the incoming connection
// contains a sufficiently privileged user.
type kvAuth struct {
	tenant tenantAuthorizer
}

// kvAuth implements the auth interface.
func (a kvAuth) AuthUnary() grpc.UnaryServerInterceptor   { return a.unaryInterceptor }
func (a kvAuth) AuthStream() grpc.StreamServerInterceptor { return a.streamInterceptor }

func (a kvAuth) unaryInterceptor(
	ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (interface{}, error) {
	// Allow unauthenticated requests for the inter-node CA public key as part
	// of the Add/Join protocol. RFC: https://github.com/cockroachdb/cockroach/pull/51991
	if info.FullMethod == "/cockroach.server.serverpb.Admin/RequestCA" {
		return handler(ctx, req)
	}
	// Allow unauthenticated requests for the inter-node CA bundle as part
	// of the Add/Join protocol. RFC: https://github.com/cockroachdb/cockroach/pull/51991
	if info.FullMethod == "/cockroach.server.serverpb.Admin/RequestCertBundle" {
		return handler(ctx, req)
	}

	tenID, err := a.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	if tenID != (roachpb.TenantID{}) {
		ctx = contextWithTenant(ctx, tenID)
		if err := a.tenant.authorize(tenID, info.FullMethod, req); err != nil {
			return nil, err
		}
	}
	return handler(ctx, req)
}

func (a kvAuth) streamInterceptor(
	srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	ctx := ss.Context()
	tenID, err := a.authenticate(ctx)
	if err != nil {
		return err
	}
	if tenID != (roachpb.TenantID{}) {
		ctx = contextWithTenant(ctx, tenID)
		origSS := ss
		ss = &wrappedServerStream{
			ServerStream: origSS,
			ctx:          ctx,
			recv: func(m interface{}) error {
				if err := origSS.RecvMsg(m); err != nil {
					return err
				}
				// 'm' is now populated and contains the request from the client.
				return a.tenant.authorize(tenID, info.FullMethod, m)
			},
		}
	}
	return handler(srv, ss)
}

func (a kvAuth) authenticate(ctx context.Context) (roachpb.TenantID, error) {
	if grpcutil.IsLocalRequestContext(ctx) {
		// This is an in-process request. Bypass authentication check.
		//
		// TODO(tbg): I don't understand when this is hit. Internal requests are routed
		// directly to a `*Node` and should never pass through this code path.
		return roachpb.TenantID{}, nil
	}

	p, ok := peer.FromContext(ctx)
	if !ok {
		return roachpb.TenantID{}, errTLSInfoMissing
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return roachpb.TenantID{}, errTLSInfoMissing
	}

	certUsers, err := security.GetCertificateUsers(&tlsInfo.State)
	if err != nil {
		return roachpb.TenantID{}, err
	}

	clientCert := tlsInfo.State.PeerCertificates[0]
	if a.tenant.tenantID == roachpb.SystemTenantID {
		// This node is a KV node.
		//
		// Is this a connection from a SQL tenant server?
		if security.IsTenantCertificate(clientCert) {
			// Incoming connection originating from a tenant SQL server,
			// into a KV node.
			// We extract the tenant ID to perform authorization
			// of the RPC for this particular tenant.
			return tenantFromCommonName(clientCert.Subject.CommonName)
		}
	} else {
		// This node is a SQL tenant server.
		//
		// Is this a connection from another SQL tenant server?
		if security.IsTenantCertificate(clientCert) {
			// Incoming connection originating from a tenant SQL server,
			// into a KV node. Let through. The other server
			// is able to use any of this server's RPCs.
			return roachpb.TenantID{}, nil
		}
	}

	// Here we handle the following cases:
	//
	// - incoming connection from a RPC admin client into either a KV
	//   node or a SQL server, using a valid root or node client cert.
	// - incoming connections from another KV node into a KV node, using
	//   a node client cert.
	// - calls coming through the gRPC gateway, from an HTTP client. The gRPC
	//   gateway uses a connection dialed as the node user.
	//
	// In both cases, we must check that the client cert is either root
	// or node.

	// TODO(benesch): the vast majority of RPCs should be limited to just
	// NodeUser. This is not a security concern, as RootUser has access to
	// read and write all data, merely good hygiene. For example, there is
	// no reason to permit the root user to send raw Raft RPCs.
	if !security.Contains(certUsers, username.NodeUser) &&
		!security.Contains(certUsers, username.RootUser) {
		return roachpb.TenantID{}, authErrorf("user %s is not allowed to perform this RPC", certUsers)
	}

	return roachpb.TenantID{}, nil
}
