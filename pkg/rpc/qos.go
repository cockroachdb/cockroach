// Copyright 2019 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/qos"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func qosClientInterceptor(
	prevUnaryInterceptor grpc.UnaryClientInterceptor,
) grpc.UnaryClientInterceptor {
	return func(
		goCtx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker, opts ...grpc.CallOption,
	) error {
		// Add a qos level header if the goCtx contains a qos level.
		if l, haveLevel := qos.LevelFromContext(goCtx); haveLevel {
			goCtx = metadata.AppendToOutgoingContext(goCtx, clientQosLevelKey, l.EncodeString())
		}
		// Chain the previous interceptor if there is one.
		if prevUnaryInterceptor != nil {
			return prevUnaryInterceptor(goCtx, method, req, reply, cc, invoker, opts...)
		}
		return invoker(goCtx, method, req, reply, cc, opts...)
	}
}

func qosServerInterceptor(
	prevUnaryInterceptor grpc.UnaryServerInterceptor,
) grpc.UnaryServerInterceptor {
	warnTooManyEvery := log.Every(time.Second)
	errMalformedQosLevelEvery := log.Every(time.Second)
	return func(
		goCtx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
	) (interface{}, error) {
		if md, ok := metadata.FromIncomingContext(goCtx); ok {
			if v := md.Get(clientQosLevelKey); len(v) > 0 {
				// We don't expect more than one item; gRPC does not copy metadata
				// from one incoming RPC to an outgoing RPC, so there should be a
				// single qos level in the context put there by the interceptor on the
				// client before calling this RPC. Nevertheless, having two is only
				// logged and is not treated as an error.
				if len(v) > 1 && warnTooManyEvery.ShouldLog() {
					log.Warningf(goCtx, "unexpected multiple qos levels in client metadata: %s", v)
				}
				// If a qos level header exists but is malformed it is ignored but a
				// message is logged with the corresponding error.
				// TODO(ajwerner): consider if this behavior should be less lenient for
				// malformed headers.
				if l, err := qos.DecodeString(v[0]); err == nil {
					goCtx = qos.ContextWithLevel(goCtx, l)
				} else if errMalformedQosLevelEvery.ShouldLog() {
					log.Errorf(goCtx, "malformed qos level %s header: %v", clientQosLevelKey, err)
				}
			}
		}
		if prevUnaryInterceptor != nil {
			return prevUnaryInterceptor(goCtx, req, info, handler)
		}
		return handler(goCtx, req)
	}
}
