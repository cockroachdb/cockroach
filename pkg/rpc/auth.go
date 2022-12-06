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
	"github.com/cockroachdb/errors"
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
	ctx = contextWithClientTenant(ctx, tenID)
	if tenID.IsSet() {
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
	ctx = contextWithClientTenant(ctx, tenID)
	if tenID != (roachpb.TenantID{}) {
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

// authenticate returns the client tenant ID of an RPC. An empty TenantID is
// returned if authorization should not be performed because the caller is
// allowed to perform any RPC.
func (a kvAuth) authenticate(ctx context.Context) (roachpb.TenantID, error) {
	// Deal with local requests done through the internalClientAdapter. There's no
	// TLS for these calls, so the regular authentication code path doesn't apply.
	{
		clientTenantID, localRequest := grpcutil.IsLocalRequestContext(ctx)
		if localRequest {
			if clientTenantID == roachpb.SystemTenantID {
				// Bypass authentication check.
				return roachpb.TenantID{}, nil
			}
			return clientTenantID, nil
		}
	}

	p, ok := peer.FromContext(ctx)
	if !ok {
		return roachpb.TenantID{}, errTLSInfoMissing
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return roachpb.TenantID{}, errTLSInfoMissing
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
	// or node. We also need to check that the tenant scope for the cert
	// is either the system tenant ID or matches the tenant ID of the server.

	// TODO(benesch): the vast majority of RPCs should be limited to just
	// NodeUser. This is not a security concern, as RootUser has access to
	// read and write all data, merely good hygiene. For example, there is
	// no reason to permit the root user to send raw Raft RPCs.
	certUserScope, err := security.GetCertificateUserScope(&tlsInfo.State)
	if err != nil {
		return roachpb.TenantID{}, err
	}

	// Confirm that the user scope is node/root. Otherwise, return an authentication error.
	_, err = getActiveNodeOrUserScope(certUserScope, username.RootUser, a.tenant.tenantID)
	if err != nil {
		return roachpb.TenantID{}, authErrorf("client certificate %s cannot be used to perform RPC on tenant %d", clientCert.Subject, a.tenant.tenantID)
	}

	// User is node/root user authorized for this tenant, return success.
	return roachpb.TenantID{}, nil
}

// getActiveNodeOrUserScope returns a node user scope if one is present in the set of certificate user scopes. If node
// user scope is not present, it returns the user scope corresponding to the username parameter. The node user scope
// will always override the user scope for authentication.
func getActiveNodeOrUserScope(
	certUserScope []security.CertificateUserScope, user string, serverTenantID roachpb.TenantID,
) (security.CertificateUserScope, error) {
	var userScope security.CertificateUserScope
	for _, scope := range certUserScope {
		// If we get a scope that matches the Node user, immediately return.
		if scope.Username == username.NodeUser {
			if scope.Global {
				return scope, nil
			}
			// Confirm that the certificate scope and serverTenantID are the system tenant.
			if scope.TenantID == roachpb.SystemTenantID && serverTenantID == roachpb.SystemTenantID {
				return scope, nil
			}
		}
		if scope.Username == user && (scope.TenantID == serverTenantID || scope.Global) {
			userScope = scope
		}
	}
	// Double check we're not returning an empty scope
	if userScope.Username == "" {
		return userScope, errors.New("could not find active user scope for client certificate")
	}

	// Only return userScope if we haven't found a NodeUser.
	return userScope, nil
}
