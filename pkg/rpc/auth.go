// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"crypto/x509"
	"fmt"
	drpc "storj.io/drpc"
	"storj.io/drpc/drpcserver"

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
	grpcpeer "google.golang.org/grpc/peer"
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
func (a kvAuth) AuthDrpc() drpcserver.ServerInterceptor {
	return a.serverDrpcInterceptor
}

// NewDrpcAuthInterceptor creates a new DRPC server interceptor for authentication and authorization.
// It uses the provided rpc.Context to configure the underlying authentication and authorization mechanisms.
func NewDrpcAuthInterceptor(
	rpcCtx *Context, // Changed to accept *rpc.Context
) drpcserver.ServerInterceptor {
	auth := kvAuth{
		sv: &rpcCtx.Settings.SV, // Assumes Settings is an exported field of rpc.Context and SV is a field of Settings
		tenant: tenantAuthorizer{
			tenantID:               rpcCtx.tenID,                  // Accesses field from rpc.Context (can be unexported)
			capabilitiesAuthorizer: rpcCtx.capabilitiesAuthorizer, // Accesses field from rpc.Context (can be unexported)
		},
	}
	return auth.AuthDrpc()
}

// serverDrpcInterceptor implements server-side authentication and authorization for dRPC requests.
// dRPC typically carries the relevant request context on the stream itself (stream.Context()).
func (a kvAuth) serverDrpcInterceptor(
	_ context.Context, rpcName string, stream drpc.Stream, handler drpc.Handler,
) error {
	// Step 1: Authenticate the request and select an authorization rule.
	authnRes, authz, err := a.authenticateAndSelectAuthzRule(stream.Context())
	if err != nil {
		// Authentication or selection of authorization rule failed.
		return err // Return error to terminate RPC.
	}

	// Step 2: Enhance the context.
	// This creates a new context, potentially with client tenant information,
	// based on the authentication result. This enhanced context will be
	// made available to the RPC handler.
	enhancedCtx := contextForRequest(stream.Context(), authnRes)

	// Step 3: Prepare a wrapped stream.
	// This stream will carry the enhancedCtx. For certain authorization cases,
	// it will also intercept message receiving (MsgRecv) for per-message checks.
	wrappedStream := &wrappedDrpcStream{
		Stream: stream,      // The original dRPC stream from the transport.
		ctx:    enhancedCtx, // The context the handler will see via wrappedStream.Context().
	}

	// Step 4: Apply authorization logic based on the determined authorization method.
	switch ar := authz.(type) {
	case authzTenantServerToKVServer:
		// Handles requests from a tenant server to a KV server.
		// These requests require per-message authorization to ensure a tenant
		// only accesses its own data.

		// Clear any leftover gRPC incoming metadata from the context.
		// This prevents unintended metadata propagation if the dRPC call
		// is part of a chain (e.g., gRPC -> dRPC).
		finalCtxForHandler := enhancedCtx
		// rpcName is equivalent to gRPC's FullMethod (e.g., "/package.Service/Method").
		// Special handling for Blob/PutStream to preserve 'filename' metadata, similar to gRPC interceptor.
		if rpcName == "/cockroach.blobs.Blob/PutStream" { // Match dRPC service/method name.
			finalCtxForHandler = grpcutil.ClearIncomingContextExcept(finalCtxForHandler, "filename")
		} else {
			finalCtxForHandler = grpcutil.ClearIncomingContext(finalCtxForHandler)
		}
		wrappedStream.ctx = finalCtxForHandler // Update context on the wrapped stream.

		// Set up per-message authorization by providing the doAuthRecv function.
		wrappedStream.doAuthRecv = func(msg drpc.Message, enc drpc.Encoding) error {
			// First, receive the actual message using the underlying (original) stream.
			// wrappedStream.Stream is the original stream passed to the interceptor.
			if err := wrappedStream.Stream.MsgRecv(msg, enc); err != nil {
				return err // Error during message receive.
			}
			// After successfully receiving, 'msg' is populated. Authorize it.
			// The finalCtxForHandler contains authentication details (e.g., tenant ID).
			// roachpb.TenantID(ar) provides the client tenant ID for authorization.
			return a.tenant.authorize(finalCtxForHandler, a.sv, roachpb.TenantID(ar), rpcName, msg)
		}

	case authzTenantServerToTenantServer:
		// Handles requests from one tenant server to another.
		// After initial authentication, these are generally allowed.
		// No additional per-message authorization is typically needed.
		// The wrappedStream with its enhancedCtx is passed along.

	case authzPrivilegedPeerToServer:
		// Handles requests from privileged peers (e.g., root/node users, other KV nodes).
		// These are generally allowed full access after initial authentication.
		// No additional per-message authorization.
		// The wrappedStream with its enhancedCtx is passed along.

	default:
		// Should not happen. Indicates a programming error (unhandled authorization case).
		return errors.AssertionFailedf("unhandled authz case in dRPC interceptor: %T", ar)
	}

	// Step 5: Call the actual dRPC handler.
	// The handler receives the wrappedStream (which provides the enhanced context
	// and potentially per-message authorization via its MsgRecv method) and the rpcName.
	return handler.HandleRPC(wrappedStream, rpcName)
}

func (a kvAuth) unaryInterceptor(
	ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (interface{}, error) {

	// Perform authentication and authz selection.
	authnRes, authz, err := a.authenticateAndSelectAuthzRule(ctx)
	if err != nil {
		return nil, err
	}

	// Enhance the context to ensure the API handler only sees a client tenant ID
	// via roachpb.ClientTenantFromContext when relevant.
	ctx = contextForRequest(ctx, authnRes)

	// Handle authorization according to the selected authz method.
	switch ar := authz.(type) {
	case authzTenantServerToKVServer:
		// Clear any leftover gRPC incoming metadata, if this call
		// is originating from a RPC handler function called as
		// a result of a tenant call. This is this case:
		//
		//    tenant -(rpc)-> tenant -(rpc)-> KV
		//                            ^ YOU ARE HERE
		//
		// at this point, the left side RPC has left some incoming
		// metadata in the context, but we need to get rid of it
		// before we let the call go through KV. Any stray metadata
		// could influence the execution on the KV-level handlers.
		ctx = grpcutil.ClearIncomingContext(ctx)

		if err := a.tenant.authorize(ctx, a.sv, roachpb.TenantID(ar), info.FullMethod, req); err != nil {
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

	// Enhance the context to ensure the API handler only sees a client tenant ID
	// via roachpb.ClientTenantFromContext when relevant.
	ctx = contextForRequest(ctx, authnRes)

	// Handle authorization according to the selected authz method.
	switch ar := authz.(type) {
	case authzTenantServerToKVServer:
		// Clear any leftover gRPC incoming metadata, if this call
		// is originating from a RPC handler function called as
		// a result of a tenant call. This is this case:
		//
		//    tenant -(rpc)-> tenant -(rpc)-> KV
		//                            ^ YOU ARE HERE
		//
		// at this point, the left side RPC has left some incoming
		// metadata in the context, but we need to get rid of it
		// before we let the call go through KV. Any stray metadata
		// could influence the execution on the KV-level handlers.
		//
		// We have a single unfortunate quirk, the PutStream
		// method of the blob service. That RPC uses incoming
		// metadata to identify the filename of the file being
		// uploaded.
		if info.FullMethod == "/cockroach.blobs.Blob/PutStream" {
			ctx = grpcutil.ClearIncomingContextExcept(ctx, "filename")
		} else {
			ctx = grpcutil.ClearIncomingContext(ctx)
		}

		origSS := ss
		ss = &wrappedServerStream{
			ServerStream: origSS,
			ctx:          ctx,
			recv: func(m interface{}) error {
				if err := origSS.RecvMsg(m); err != nil {
					return err
				}
				// 'm' is now populated and contains the request from the client.
				return a.tenant.authorize(ctx, a.sv, roachpb.TenantID(ar), info.FullMethod, m)
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
	p, ok := grpcpeer.FromContext(ctx)
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

// Deal with local requests done through the internalClientAdapter.
// There's no TLS for these calls, so the regular authentication code
// path doesn't apply. The clientTenantID should be the result of a
// call to grpcutil.IsLocalRequestContext.
func (a kvAuth) authenticateLocalRequest(
	ctx context.Context, clientTenantID roachpb.TenantID,
) (authnResult, error) {
	// Sanity check: verify that we do not also have gRPC network credentials
	// in the context. This would indicate that metadata was improperly propagated.
	maybeTid, err := tenantIDFromRPCMetadata(ctx)
	if err != nil || maybeTid.IsSet() {
		logcrash.ReportOrPanic(ctx, a.sv, "programming error: network credentials in internal adapter request (%v, %v)", maybeTid, err)
		return nil, authErrorf("programming error")
	}

	if !clientTenantID.IsSet() {
		return authnSuccessPeerIsPrivileged{}, nil
	}

	if clientTenantID.IsSystem() {
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

	tenantIDFromMetadata, err := tenantIDFromRPCMetadata(ctx)
	if err != nil {
		return nil, authErrorf("client provided invalid tenant ID: %v", err)
	}

	// Did the client peer use a tenant client cert?
	if security.IsTenantCertificate(clientCert) {
		// If the peer is using a client tenant cert, in any case we
		// validate the tenant ID stored in the CN for correctness.
		tlsID, err := tenantIDFromString(clientCert.Subject.CommonName, "Common Name (CN)")
		if err != nil {
			return nil, err
		}
		// If the peer is using a TenantCertificate and also
		// provided a tenant ID via gRPC metadata, they must
		// match.
		if tenantIDFromMetadata.IsSet() && tenantIDFromMetadata != tlsID {
			return nil, authErrorf(
				"client wants to authenticate as tenant %v, but is using TLS cert for tenant %v",
				tenantIDFromMetadata, tlsID)
		}
		return authnSuccessPeerIsTenantServer(tlsID), nil
	}

	// We are using TLS, but the peer is not using a client tenant cert.
	// In that case, we only allow RPCs if the principal is 'node' or
	// 'root' and the tenant scope in the cert matches this server
	// (either the cert has scope "global" or its scope tenant ID
	// matches our own). The client could also present a certificate with subject
	// DN equalling rootSubject or nodeSubject set using
	// root-cert-distinguished-name and node-cert-distinguished-name cli flags
	// respectively. Additionally if subject_required cluster setting is set, both
	// root and node users must have a valid DN set.
	//
	// TODO(benesch): the vast majority of RPCs should be limited to
	// just NodeUser. This is not a security concern, as RootUser has
	// access to read and write all data, merely good hygiene. For
	// example, there is no reason to permit the root user to send raw
	// Raft RPCs.
	rootOrNodeDNSet, certDNMatchesRootOrNodeDN := security.CheckCertDNMatchesRootDNorNodeDN(clientCert)
	if rootOrNodeDNSet && !certDNMatchesRootOrNodeDN {
		return nil, authErrorf(
			"need root or node client cert to perform RPCs on this server: cert dn did not match set root or node dn",
		)
	}
	if !rootOrNodeDNSet {
		if security.ClientCertSubjectRequired.Get(a.sv) {
			return nil, authErrorf(
				"root and node roles do not have valid DNs set which subject_required cluster setting mandates",
			)
		}
		if err := checkRootOrNodeInScope(clientCert, a.tenant.tenantID); err != nil {
			return nil, err
		}
	}

	if tenantIDFromMetadata.IsSet() {
		return authnSuccessPeerIsTenantServer(tenantIDFromMetadata), nil
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
func checkRootOrNodeInScope(clientCert *x509.Certificate, serverTenantID roachpb.TenantID) error {
	containsFn := func(scope security.CertificateUserScope) bool {
		// Only consider global scopes or scopes that match this server.
		if !(scope.Global || scope.TenantID == serverTenantID) {
			return false
		}

		// If we get a scope that matches the Node user, immediately return.
		if scope.Username == username.NodeUser || scope.Username == username.RootUser {
			return true
		}

		return false
	}
	ok, err := security.CertificateUserScopeContainsFunc(clientCert, containsFn)
	if ok || err != nil {
		return err
	}
	certUserScope, err := security.GetCertificateUserScope(clientCert)
	if err != nil {
		return err
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
// This metadata item is not meant to be used beyond authentication;
// to access the client tenant ID inside RPC handlers or other code,
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

// tenantIDFromRPCMetadata checks if there is a tenant ID in
// the incoming gRPC metadata.
func tenantIDFromRPCMetadata(ctx context.Context) (roachpb.TenantID, error) {
	val, ok := grpcutil.FastFirstValueFromIncomingContext(ctx, clientTIDMetadataHeaderKey)
	if !ok {
		return roachpb.TenantID{}, nil
	}
	return tenantIDFromString(val, "gRPC metadata")
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
