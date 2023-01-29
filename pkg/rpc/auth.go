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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
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
	sv     *settings.Values
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
	authnRes, authz, ctx, err := a.authenticateAndSelectAuthzRule(ctx)
	if err != nil {
		return nil, err
	}

	// Enhance the context to ensure the API handler only sees a client tenant ID
	// via roachpb.ClientTenantFromContext when relevant.
	ctx = contextForRequest(ctx, authnRes)

	// Handle authorization according to the selected authz method.
	switch ar := authz.(type) {
	case authzTenantServerToKVServer:
		if err := a.tenant.authorize(roachpb.TenantID(ar), info.FullMethod, req); err != nil {
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
	authnRes, authz, ctx, err := a.authenticateAndSelectAuthzRule(ctx)
	if err != nil {
		return err
	}

	// Enhance the context to ensure the API handler only sees a client tenant ID
	// via roachpb.ClientTenantFromContext when relevant.
	ctx = contextForRequest(ctx, authnRes)

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
				return a.tenant.authorize(roachpb.TenantID(ar), info.FullMethod, m)
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
) (authnResult, requiredAuthzMethod, context.Context, error) {
	// Perform authentication.
	authnRes, ctx, err := a.authenticate(ctx)
	if err != nil {
		return nil, nil, ctx, err
	}

	// Select authorization rules suitable for the peer.
	authz, err := a.selectAuthzMethod(ctx, authnRes)
	if err != nil {
		return nil, nil, ctx, err
	}

	return authnRes, authz, ctx, nil
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
// some consistency check with the information provided. The caller
// should discard the original context.Context and use the new one;
// the function also consumes and strips some fields from the incoming
// gRPC metadata.MD to avoid confusion if/when the RPC gets forwarded.
func (a kvAuth) authenticate(ctx context.Context) (authnResult, context.Context, error) {
	var ar authnResult
	if clientTenantID, localRequest := grpcutil.IsLocalRequestContext(ctx); localRequest {
		var err error
		ar, ctx, err = a.authenticateLocalRequest(ctx, clientTenantID)
		if err != nil {
			return nil, ctx, err
		}
	} else {
		var err error
		ar, ctx, err = a.authenticateNetworkRequest(ctx)
		if err != nil {
			return nil, ctx, err
		}
	}

	switch res := ar.(type) {
	case authnSuccessPeerIsTenantServer:
		if wantedTenantID := roachpb.TenantID(res); !a.tenant.tenantID.IsSystem() && wantedTenantID != a.tenant.tenantID {
			log.Ops.Infof(ctx, "rejected incoming request from tenant %d (misconfiguration?)", wantedTenantID)
			return nil, ctx, authErrorf("client tenant identity (%v) does not match server", wantedTenantID)
		}
	case authnSuccessPeerIsPrivileged:
	default:
		return nil, ctx, errors.AssertionFailedf("programming error: unhandled case %T", ar)
	}

	return ar, ctx, nil
}

// Deal with local requests done through the internalClientAdapter.
// There's no TLS for these calls, so the regular authentication code
// path doesn't apply. The clientTenantID should be the result of a
// call to grpcutil.IsLocalRequestContext.
func (a kvAuth) authenticateLocalRequest(
	ctx context.Context, clientTenantID roachpb.TenantID,
) (authnResult, context.Context, error) {
	// Sanity check: verify that we do not also have gRPC network credentials
	// in the context. This would indicate that metadata was improperly propagated.
	maybeTid, ctx, err := consumeTenantIDFromRPCMetadata(ctx)
	if err != nil || maybeTid.IsSet() {
		logcrash.ReportOrPanic(ctx, a.sv, "programming error: network credentials in internal adapter request (%v, %v)", maybeTid, err)
		return nil, ctx, authErrorf("programming error")
	}

	if !clientTenantID.IsSet() {
		return authnSuccessPeerIsPrivileged{}, ctx, nil
	}

	if clientTenantID.IsSystem() {
		return authnSuccessPeerIsPrivileged{}, ctx, nil
	}

	return authnSuccessPeerIsTenantServer(clientTenantID), ctx, nil
}

// authenticateNetworkRequest authenticates requests made over a TLS connection.
func (a kvAuth) authenticateNetworkRequest(
	ctx context.Context,
) (authnResult, context.Context, error) {
	// We will need to look at the TLS cert in any case, so extract it
	// first.
	clientCert, err := getClientCert(ctx)
	if err != nil {
		return nil, ctx, err
	}

	tenantIDFromMetadata, ctx, err := consumeTenantIDFromRPCMetadata(ctx)
	if err != nil {
		return nil, ctx, authErrorf("client provided invalid tenant ID: %v", err)
	}

	// Did the client peer use a tenant client cert?
	if security.IsTenantCertificate(clientCert) {
		// If the peer is using a client tenant cert, in any case we
		// validate the tenant ID stored in the CN for correctness.
		tlsID, err := tenantIDFromString(clientCert.Subject.CommonName, "Common Name (CN)")
		if err != nil {
			return nil, ctx, err
		}
		// If the peer is using a TenantCertificate and also
		// provided a tenant ID via gRPC metadata, they must
		// match.
		if tenantIDFromMetadata.IsSet() && tenantIDFromMetadata != tlsID {
			return nil, ctx, authErrorf(
				"client wants to authenticate as tenant %v, but is using TLS cert for tenant %v",
				tenantIDFromMetadata, tlsID)
		}
		return authnSuccessPeerIsTenantServer(tlsID), ctx, nil
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
		return nil, ctx, err
	}
	if err := checkRootOrNodeInScope(certUserScope, a.tenant.tenantID); err != nil {
		return nil, ctx, err
	}

	if tenantIDFromMetadata.IsSet() {
		return authnSuccessPeerIsTenantServer(tenantIDFromMetadata), ctx, nil
	}
	return authnSuccessPeerIsPrivileged{}, ctx, nil
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

// contextForRequest sets up the context.Context for use by
// the API handler. It covers two cases:
//
//   - the request is coming from a secondary tenant.
//     Then it uses roachpb.ContextWithTenantClient() to
//     ensure that the API handler will find the tenant ID
//     with roachpb.TenantClientFromContext().
//   - the request is coming from the system tenant.
//     then it clears the tenant client information
//     to ensure that the API handler will _not_ find
//     a tenant ID with roachpb.TenantClientFromContext().
//
// This latter case is important e.g. in the following scenario:
//
//	SQL (a) -(network gRPC)-> KV (b) -(internal client adapter)-> KV (c)
//
// The authn in the call from (a) to (b) has added a tenant ID in the
// Go context for the handler at (b). This context.Context "pierces"
// the stack of calls in the internal client adapter, and thus the
// tenant ID is still present when the call is received at (c).
// However, we don't want the API handler at (c) to see it any more.
// So we need to remove it.
func contextForRequest(ctx context.Context, authnRes authnResult) context.Context {
	switch ar := authnRes.(type) {
	case authnSuccessPeerIsTenantServer:
		// The simple context key will be used in various places via
		// roachpb.ClientTenantFromContext(). This also adds a logging
		// tag.
		ctx = contextWithClientTenant(ctx, roachpb.TenantID(ar))
	default:
		// The caller is not a tenant server, but it may have been in the
		// process of handling an API call for a tenant server and so it
		// may have a client tenant ID in its context already. To ensure
		// none will be found, we need to clear it explicitly.
		ctx = contextWithoutClientTenant(ctx)
	}
	return ctx
}

// tenantClientCred is responsible for passing the tenant ID as
// medatada header to called RPCs. This makes it possible to pass the
// tenant ID even when using a different TLS cert than the "tenant
// client cert".
type tenantClientCred struct {
	md map[string]string
}

// clientTIDMetadataHeaderKey is the gRPC metadata key that indicates
// which tenant ID the client is intending to connect as (originating
// tenant identity).
//
// This is used instead of the cert CN field when connecting with a
// TLS client cert that is not marked as special "tenant client cert"
// via the "Tenants" string in the OU field.
//
// This metadata item is intended to be extremely short-lived: it is
// alive in the context only at the very edge of an outgoing call
// (it's injected by the final gRPC call machinery, just before the
// network request, via the PerRPCCredentials interface); and at the
// very edge of an incoming call (it's consumed, and removed,
// immediately during RPC authentication above).
//
// We care about this to cover the case where we have this chain
// of RPCs:
//
// SQL (a) -(network gRPC)-> KV (b) -(internal client adapter)-> KV (c)
//
// The MD coming from (a) to (b) must not be present in the call
// from (b) to (c), otherwise (c) gets the mistaken impression
// that its caller (b) is also a secondary tenant server.
//
// To access the client tenant ID inside RPC handlers or other code,
// use roachpb.ClientTenantFromContext() instead.
const clientTIDMetadataHeaderKey = "client-tid"

// newTenantClientCreds constructs a credentials.PerRPCCredentials
// which injects the client tenant ID as extra gRPC metadata in each
// RPC.
func newTenantClientCreds(tid roachpb.TenantID) credentials.PerRPCCredentials {
	return &tenantClientCred{
		md: map[string]string{
			clientTIDMetadataHeaderKey: fmt.Sprint(tid),
		},
	}
}

// consumeTenantIDFromRPCMetadata checks if there is a tenant ID in
// the incoming gRPC metadata. If there is, it is returned and also
// removed from the incoming context. This preserves the invariant
// that the clientTIDMetadataHeaderKey is always short-lived and not
// forwarded through Go calls.
func consumeTenantIDFromRPCMetadata(
	ctx context.Context,
) (roachpb.TenantID, context.Context, error) {
	found, val, ctx := grpcutil.FastGetAndDeleteValueFromIncomingContext(ctx, clientTIDMetadataHeaderKey)
	if !found {
		return roachpb.TenantID{}, ctx, nil
	}
	tid, err := tenantIDFromString(val, "gRPC metadata")
	return tid, ctx, err
}

// GetRequestMetadata implements the (grpc)
// credentials.PerRPCCredentials interface.
func (tcc *tenantClientCred) GetRequestMetadata(
	ctx context.Context, uri ...string,
) (map[string]string, error) {
	return tcc.md, nil
}

// RequireTransportSecurity implements the (grpc)
// credentials.PerRPCCredentials interface.
func (tcc *tenantClientCred) RequireTransportSecurity() bool { return false }
