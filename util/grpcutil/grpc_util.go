// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tamir Duberstein (tamird@gmail.com)

package grpcutil

import (
	"net/http"
	"strings"

	"golang.org/x/net/context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/transport"

	"github.com/cockroachdb/cockroach/util"
)

// GRPCHandlerFunc returns an http.Handler that delegates to grpcServer on incoming gRPC
// connections or otherHandler otherwise.
func GRPCHandlerFunc(grpcServer *grpc.Server, otherHandler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if IsGRPCRequest(r) {
			grpcServer.ServeHTTP(w, r)
		} else {
			otherHandler.ServeHTTP(w, r)
		}
	})
}

// IsGRPCRequest returns true if r came from a grpc client.
//
// Its logic is a partial recreation of gRPC's internal checks, see
// https://github.com/grpc/grpc-go/blob/01de3de/transport/handler_server.go#L61:L69
func IsGRPCRequest(r *http.Request) bool {
	return r.ProtoMajor == 2 && strings.Contains(r.Header.Get(util.ContentTypeHeader), "application/grpc")
}

// IsClosedConnection returns true if err is an error produced by gRPC on closed connections.
func IsClosedConnection(err error) bool {
	if err == context.Canceled || grpc.ErrorDesc(err) == grpc.ErrClientConnClosing.Error() || err == transport.ErrConnClosing || grpc.Code(err) == codes.Canceled {
		return true
	}
	if streamErr, ok := err.(transport.StreamError); ok && streamErr.Code == codes.Canceled {
		return true
	}
	return util.IsClosedConnection(err)
}
