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

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

type tenantAuthInterceptor struct{}

// authTenantRPCRequest performs authorization of an incoming request at the
// tenant kv server.
func (ic *tenantAuthInterceptor) authTenantRPCRequest(
	ctx context.Context, fullMethod string, req interface{},
) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return nil, errors.New("no peer info found")
	}

	tlsInfo, ok := peer.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return nil, errors.New("no TLSInfo found")
	}

	// NB: cert expiration is in tlsInfo.State.PeerCertificates[0].NotAfter, but
	// TLS handshake already verified it.
	tenantID := tlsInfo.State.PeerCertificates[0].Subject.CommonName
	ctx = logtags.AddTag(ctx, "tenant", tenantID)

	if log.V(3) {
		log.Warningf(ctx, "RPC %s: %v", fullMethod, req)
	}

	// TODO(tbg): add an allowlist of RPCs that the tenant is allowed to access
	// and a request/response verifier for each.

	return ctx, nil
}

func (ic *tenantAuthInterceptor) UnaryInterceptor(
	ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (interface{}, error) {
	var err error
	ctx, err = ic.authTenantRPCRequest(ctx, info.FullMethod, req)
	if err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (ic *tenantAuthInterceptor) StreamServerInterceptor(
	srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	ctx := ss.Context()
	var err error
	ctx, err = ic.authTenantRPCRequest(ctx, info.FullMethod, nil)
	if err != nil {
		return err
	}
	return handler(srv, &interceptedServerStream{
		ctx:          ctx,
		ServerStream: ss,
		recv: func(msg interface{}) error {
			if err := ss.RecvMsg(msg); err != nil {
				return err
			}
			// 'msg' is now populated and contains the request from the client.
			_, err := ic.authTenantRPCRequest(ctx, info.FullMethod, msg)
			return err
		}})
}

type interceptedServerStream struct {
	grpc.ServerStream
	ctx  context.Context
	recv func(interface{}) error
}

func (ss *interceptedServerStream) Context() context.Context {
	return ss.ctx
}

func (ss *interceptedServerStream) RecvMsg(m interface{}) error {
	return ss.recv(m)
}
