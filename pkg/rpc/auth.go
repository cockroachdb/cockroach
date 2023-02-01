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
	"crypto/x509"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	// Perform authentication and authz selection.
	authnRes, authz, err := a.authenticateAndSelectAuthzRule(ctx)
	if err != nil {
		return nil, err
	}

	// Enhance the context if the peer is a tenant server.
	switch ar := authnRes.(type) {
	case authnSuccessPeerIsTenantServer:
		ctx = contextWithClientTenant(ctx, roachpb.TenantID(ar))
	default:
		ctx = contextWithoutClientTenant(ctx)
	}

	// Handle authorization according to the selected authz method.
	switch ar := authz.(type) {
	case authzTenantServerToKVServer:
		if err := a.tenant.authorize(ctx, roachpb.TenantID(ar), info.FullMethod, req); err != nil {
			return nil, err
		}
	case authzTenantServerToTenantServer:
	// Tenant servers can see all of each other's RPCs.
	case authzPrivilegedPeerToServer:
		// Privileged clients (root/node) can see all RPCs.
	default:
		return nil, errors.AssertionFailedf("unhandled case: %T", err)
	}
	return handler(ctx, req)
}

func (a kvAuth) streamInterceptor(
	srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	ctx := ss.Context()

	// Perform authentication and authz selection.
	authnRes, authz, err := a.authenticateAndSelectAuthzRule(ctx)
	if err != nil {
		return err
	}

	// Enhance the context if the peer is a tenant server.
	switch ar := authnRes.(type) {
	case authnSuccessPeerIsTenantServer:
		ctx = contextWithClientTenant(ctx, roachpb.TenantID(ar))
	default:
		ctx = contextWithoutClientTenant(ctx)
	}

	// Handle authorization according to the selected authz method.
	switch ar := authz.(type) {
	case authzTenantServerToKVServer:
		origSS := ss
		ss = &wrappedServerStream{
			ServerStream: origSS,
			ctx:          ctx,
			recv: func(m interface{}) error {
				if err := origSS.RecvMsg(m); err != nil {
					return err
				}
				// 'm' is now populated and contains the request from the client.
				return a.tenant.authorize(ctx, roachpb.TenantID(ar), info.FullMethod, m)
			},
		}
	case authzTenantServerToTenantServer:
	// Tenant servers can see all of each other's RPCs.
	case authzPrivilegedPeerToServer:
		// Privileged clients (root/node) can see all RPCs.
	default:
		return errors.AssertionFailedf("unhandled case: %T", err)
	}
	return handler(srv, ss)
}

func (a kvAuth) authenticateAndSelectAuthzRule(
	ctx context.Context,
) (authnResult, requiredAuthzMethod, error) {
	// Perform authentication.
	authnRes, err := a.authenticate(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Select authorization rules suitable for the peer.
	authz, err := a.selectAuthzMethod(ctx, authnRes)
	if err != nil {
		return nil, nil, err
	}

	return authnRes, authz, nil
}

// authnResult is a sum type that describes how RPC authentication has succeeded.
// This is used as input to selectAuthzMethod.
type authnResult interface {
	authnResult()
}

func getClientCert(ctx context.Context) (*x509.Certificate, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, errTLSInfoMissing
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(tlsInfo.State.PeerCertificates) == 0 {
		return nil, errTLSInfoMissing
	}

	clientCert := tlsInfo.State.PeerCertificates[0]
	return clientCert, nil
}

// authnSuccessPeerIsTenantServer indicates authentication has
// succeeded, and the peer wishes to identify itself as a tenant
// server with this tenant ID.
type authnSuccessPeerIsTenantServer roachpb.TenantID

// authnSuccessPeerIsPrivileged indicates authentication
// has succeeded, and the peer has used a root or node client cert.
type authnSuccessPeerIsPrivileged struct{}

func (authnSuccessPeerIsTenantServer) authnResult() {}
func (authnSuccessPeerIsPrivileged) authnResult()   {}

// authenticate verifies the credentials of the client and performs
// some consistency check with the information provided.
func (a kvAuth) authenticate(ctx context.Context) (authnResult, error) {
	var ar authnResult
	if clientTenantID, localRequest := grpcutil.IsLocalRequestContext(ctx); localRequest {
		var err error
		ar, err = a.authenticateLocalRequest(ctx, clientTenantID)
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		ar, err = a.authenticateNetworkRequest(ctx)
		if err != nil {
			return nil, err
		}
	}

	switch res := ar.(type) {
	case authnSuccessPeerIsTenantServer:
		if wantedTenantID := roachpb.TenantID(res); !a.tenant.tenantID.IsSystem() && wantedTenantID != a.tenant.tenantID {
			log.Ops.Infof(ctx, "rejected incoming request from tenant %d (misconfiguration?)", wantedTenantID)
			return nil, authErrorf("client tenant identity (%v) does not match server", wantedTenantID)
		}
	case authnSuccessPeerIsPrivileged:
	default:
		return nil, errors.AssertionFailedf("programming error: unhandled case %T", ar)
	}

	return ar, nil
}

// Deal with local requests done through the
// internalClientAdapter. There's no TLS for these calls, so the
// regular authentication code path doesn't apply. The clientTenantID
// should be the result of a call to grpcutil.IsLocalRequestContext.
func (a kvAuth) authenticateLocalRequest(
	ctx context.Context, clientTenantID roachpb.TenantID,
) (authnResult, error) {
	if !clientTenantID.IsSet() || clientTenantID.IsSystem() {
		return authnSuccessPeerIsPrivileged{}, nil
	}

	return authnSuccessPeerIsTenantServer(clientTenantID), nil
}

// authenticateNetworkRequest authenticates requests made over a TLS connection.
func (a kvAuth) authenticateNetworkRequest(ctx context.Context) (authnResult, error) {
	// We will need to look at the TLS cert in any case, so extract it
	// first.
	clientCert, err := getClientCert(ctx)
	if err != nil {
		return nil, err
	}

	// Did the client peer use a tenant client cert?
	if security.IsTenantCertificate(clientCert) {
		// If the peer is using a client tenant cert, in any case we
		// validate the tenant ID stored in the CN for correctness.
		tlsID, err := tenantIDFromString(clientCert.Subject.CommonName, "Common Name (CN)")
		if err != nil {
			return nil, err
		}

		return authnSuccessPeerIsTenantServer(tlsID), nil
	}

	// We are using TLS, but the peer is not using a client tenant cert.
	// In that case, we only allow RPCs if the principal is 'node' or
	// 'root' and the tenant scope in the cert matches this server
	// (either the cert has scope "global" or its scope tenant ID
	// matches our own).
	//
	// TODO(benesch): the vast majority of RPCs should be limited to
	// just NodeUser. This is not a security concern, as RootUser has
	// access to read and write all data, merely good hygiene. For
	// example, there is no reason to permit the root user to send raw
	// Raft RPCs.
	certUserScope, err := security.GetCertificateUserScope(clientCert)
	if err != nil {
		return nil, err
	}
	if err := checkRootOrNodeInScope(certUserScope, a.tenant.tenantID); err != nil {
		return nil, err
	}

	return authnSuccessPeerIsPrivileged{}, nil
}

// requiredAuthzMethod is a sum type that describes which authorization
// rules to use to determine whether a RPC is allowed or not.
type requiredAuthzMethod interface {
	rpcAuthzMethod()
}

// Tenant server connecting to KV node.
type authzTenantServerToKVServer roachpb.TenantID

// Tenant server connecting to another tenant server.
type authzTenantServerToTenantServer struct{}

// External client connecting to tenant server or KV node using 'root'
// or 'node' client cert; or KV node connecting to other KV node.
type authzPrivilegedPeerToServer struct{}

func (authzTenantServerToKVServer) rpcAuthzMethod()     {}
func (authzTenantServerToTenantServer) rpcAuthzMethod() {}
func (authzPrivilegedPeerToServer) rpcAuthzMethod()     {}

// selectAuthzMethod selects the authorization rule to use for the
// given authentication event.
func (a kvAuth) selectAuthzMethod(
	ctx context.Context, ar authnResult,
) (requiredAuthzMethod, error) {
	switch res := ar.(type) {
	case authnSuccessPeerIsTenantServer:
		// The client is a tenant server. We have two possible cases:
		// - tenant server to KV node.
		// - tenant server to another tenant server.
		if a.tenant.tenantID == roachpb.SystemTenantID {
			return authzTenantServerToKVServer(res), nil
		}
		return authzTenantServerToTenantServer{}, nil

	case authnSuccessPeerIsPrivileged:
		// Here are the remaining cases:
		//
		// - incoming connection from a RPC admin client into either a KV
		//   node or a SQL server, using a valid root or node client cert.
		// - incoming connections from another KV node into a KV node, using
		//   a node client cert.
		// - calls coming through the gRPC gateway, from an HTTP client. The gRPC
		//   gateway uses a connection dialed as the node user.
		//
		// In all these cases, the RPC request is authorized.
		return authzPrivilegedPeerToServer{}, nil

	default:
		return nil, errors.AssertionFailedf("programming error: unhandled case %T", ar)
	}
}

// checkRootOrNodeInScope checks that the root or node principals are
// present in the cert user scopes.
func checkRootOrNodeInScope(
	certUserScope []security.CertificateUserScope, serverTenantID roachpb.TenantID,
) error {
	for _, scope := range certUserScope {
		// Only consider global scopes or scopes that match this server.
		if !(scope.Global || scope.TenantID == serverTenantID) {
			continue
		}

		// If we get a scope that matches the Node user, immediately return.
		if scope.Username == username.NodeUser || scope.Username == username.RootUser {
			return nil
		}
	}

	return authErrorf(
		"need root or node client cert to perform RPCs on this server (this is tenant %v; cert is valid for %s)",
		serverTenantID, security.FormatUserScopes(certUserScope))
}
