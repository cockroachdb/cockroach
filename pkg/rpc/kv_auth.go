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

	"google.golang.org/grpc"
)

type kvAuthInterceptor struct{}

func (ic *kvAuthInterceptor) UnaryInterceptor(
	ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (interface{}, error) {
	if err := requireSuperUser(ctx); err != nil {
		return nil, err
	}
	return handler(ctx, req)
}

func (ic *kvAuthInterceptor) StreamServerInterceptor(
	srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	if err := requireSuperUser(ss.Context()); err != nil {
		return err
	}
	return handler(srv, ss)
}
