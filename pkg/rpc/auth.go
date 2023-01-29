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
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
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

	// Do we have a wanted tenant ID from the request metadata?
	var wantedTenantID roachpb.TenantID
	if val := metadata.ValueFromIncomingContext(ctx, ClientTIDMetadataHeaderKey); len(val) > 0 {
		var err error
		wantedTenantID, err = tenantIDFromString(val[0], "gRPC metadata")
		if err != nil {
			return roachpb.TenantID{}, authErrorf("client provided invalid tenant ID: %v", err)
		}
	}

	if security.IsTenantCertificate(clientCert) {
		// If the peer is using a client tenant cert, in any case we
		// validate the tenant ID stored in the CN for correctness.
		tlsID, err := tenantIDFromString(clientCert.Subject.CommonName, "Common Name (CN)")
		if err != nil {
			return roachpb.TenantID{}, err
		}
		// If the peer is using a client tenant cert, either:
		// - there was a wanted tenant ID in the metadata,
		//   in which case we verify the metadata ID is the
		//   same as the ID in the cert; or
		// - there was no wanted ID in the metadata,
		//   in which case we use the tenant ID in the cert.
		if wantedTenantID.IsSet() {
			// Verify conformance.
			if tlsID != wantedTenantID {
				return roachpb.TenantID{}, authErrorf(
					"client wants to authenticate as tenant %v, but is using TLS cert for tenant %v",
					wantedTenantID, tlsID)
			}
		} else {
			// Use the ID in the cert as wanted tenant ID.
			wantedTenantID = tlsID
		}
	} else {
		// The peer is not using a client tenant cert.
		// In that case, we only allow RPCs if the principal
		// is 'node' or 'root' and the tenant scope
		// in the cert matches this server (either the cert has
		// scope "global" or its scope tenant ID matches our own).
		//
		// TODO(benesch): the vast majority of RPCs should be limited to just
		// NodeUser. This is not a security concern, as RootUser has access to
		// read and write all data, merely good hygiene. For example, there is
		// no reason to permit the root user to send raw Raft RPCs.
		certUserScope, err := security.GetCertificateUserScope(clientCert)
		if err != nil {
			return roachpb.TenantID{}, err
		}
		if err := checkRootOrNodeInScope(certUserScope, a.tenant.tenantID); err != nil {
			return roachpb.TenantID{}, err
		}
	}

	// After this point, authentication has succeeded. We now need
	// to determine how to _authorize_ the operation.

	if a.tenant.tenantID == roachpb.SystemTenantID {
		// This node is a KV node.
		//
		// If the client wants to authenticate as a tenant server,
		// continue with authorization using the tenant ID it wants.
		return wantedTenantID, nil
	}

	// If the peer wants to authenticate as another tenant server,
	// verify that the peer is a service for the same tenant
	// as ourselves (we don't want to allow tenant 123 to
	// serve requests for a client coming from tenant 456).
	if wantedTenantID.IsSet() {
		if wantedTenantID != a.tenant.tenantID {
			return roachpb.TenantID{}, authErrorf("this tenant (%v) cannot serve requests from a server for tenant %v", a.tenant.tenantID, wantedTenantID)
		}
	}

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
	return roachpb.TenantID{}, nil
}

// checkRootOrNodeInScope checks that the root or node principals are
// present in the cert user scopes.
func checkRootOrNodeInScope(
	certUserScope []security.CertificateUserScope, serverTenantID roachpb.TenantID,
) (err error) {
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
		"need root or node client cert to perform RPCs on tenant %v (cert valid for %s)",
		serverTenantID, formatScopesForError(certUserScope))
}

func formatScopesForError(certUserScope []security.CertificateUserScope) string {
	var buf strings.Builder

	comma := ""
	for _, scope := range certUserScope {
		fmt.Fprintf(&buf, "%s%q on ", comma, scope.Username)
		if scope.Global {
			buf.WriteString("all tenants")
		} else {
			fmt.Fprintf(&buf, "tenant %v", scope.TenantID)
		}
		comma = ", "
	}

	return buf.String()
}
