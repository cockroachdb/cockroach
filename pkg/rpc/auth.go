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

	"github.com/cockroachdb/cockroach/pkg/security"
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

// auth is a policy that performs authentication and authorization for unary and
// streaming RPC invocations. An auth enforces its policy through a pair of gRPC
// interceptors that it exports.
type auth interface {
	// AuthUnary returns an interceptor to validate unary RPCs.
	AuthUnary() grpc.UnaryServerInterceptor

	// AuthUnary returns an interceptor to validate streaming RPCs.
	AuthStream() grpc.StreamServerInterceptor
}

// kvAuth is the standard auth policy used for RPCs sent to an RPC server. It
// validates that client TLS certificate provided by the incoming connection
// contains a sufficiently privileged user.
type kvAuth struct{}

// kvAuth implements the auth interface.
func (a kvAuth) AuthUnary() grpc.UnaryServerInterceptor   { return a.unaryInterceptor }
func (a kvAuth) AuthStream() grpc.StreamServerInterceptor { return a.streamInterceptor }

func (a kvAuth) unaryInterceptor(
	ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (interface{}, error) {
	if err := a.requireSuperUser(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (a kvAuth) streamInterceptor(
	srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	if err := a.requireSuperUser(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}

func (a kvAuth) requireSuperUser(ctx context.Context) error {
	// TODO(marc): grpc's authentication model (which gives credential access in
	// the request handler) doesn't really fit with the current design of the
	// security package (which assumes that TLS state is only given at connection
	// time) - that should be fixed.
	if grpcutil.IsLocalRequestContext(ctx) {
		// This is an in-process request. Bypass authentication check.
		return nil
	}

	peer, ok := peer.FromContext(ctx)
	if !ok {
		return errTLSInfoMissing
	}

	tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return errTLSInfoMissing
	}

	certUsers, err := security.GetCertificateUsers(&tlsInfo.State)
	if err != nil {
		return err
	}
	// TODO(benesch): the vast majority of RPCs should be limited to just
	// NodeUser. This is not a security concern, as RootUser has access to
	// read and write all data, merely good hygiene. For example, there is
	// no reason to permit the root user to send raw Raft RPCs.
	if !security.ContainsUser(security.NodeUser, certUsers) &&
		!security.ContainsUser(security.RootUser, certUsers) {
		return authErrorf("user %s is not allowed to perform this RPC", certUsers)
	}
	return nil
}
