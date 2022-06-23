// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpc

import (
	"context"
)

// ServerInfo contains server rpc info.
type ServerInfo struct {
	// Path is the rpc method path.
	//
	// This is RPC-system dependent, but will be unique for a given RPC method.
	// For both Twirp and gRPC, this is /package.Service/Method.
	//
	// May be empty, although almost never is in current practice.
	Path string
}

// ServerInterceptor is a server interceptor.
type ServerInterceptor interface {
	Intercept(
		ctx context.Context,
		request interface{},
		serverInfo *ServerInfo,
		serverHandler ServerHandler,
	) (interface{}, error)
}

// ServerInterceptorFunc is a function that implements ServerInterceptor.
type ServerInterceptorFunc func(
	ctx context.Context,
	request interface{},
	serverInfo *ServerInfo,
	serverHandler ServerHandler,
) (interface{}, error)

// Intercept implements ServerInterceptor.
func (i ServerInterceptorFunc) Intercept(
	ctx context.Context,
	request interface{},
	serverInfo *ServerInfo,
	serverHandler ServerHandler,
) (interface{}, error) {
	return i(ctx, request, serverInfo, serverHandler)
}

// ServerHandler is a server handler.
type ServerHandler interface {
	Handle(ctx context.Context, request interface{}) (interface{}, error)
}

// ServerHandlerFunc is a function that implements ServerHandler.
type ServerHandlerFunc func(ctx context.Context, request interface{}) (interface{}, error)

// Handle implements ServerHandler.
func (h ServerHandlerFunc) Handle(ctx context.Context, request interface{}) (interface{}, error) {
	return h(ctx, request)
}

// NewChainedServerInterceptor returns a new chained ServerInterceptor.
//
// Returns nil if interceptors is empty.
func NewChainedServerInterceptor(interceptors ...ServerInterceptor) ServerInterceptor {
	switch n := len(interceptors); n {
	case 0:
		return nil
	case 1:
		return interceptors[0]
	default:
		return ServerInterceptorFunc(
			func(
				ctx context.Context,
				request interface{},
				serverInfo *ServerInfo,
				serverHandler ServerHandler,
			) (_ interface{}, retErr error) {
				chainer := func(
					currentInterceptor ServerInterceptor,
					currentHandler ServerHandler,
				) ServerHandler {
					return ServerHandlerFunc(
						func(
							currentCtx context.Context,
							currentRequest interface{},
						) (interface{}, error) {
							return currentInterceptor.Intercept(currentCtx, currentRequest, serverInfo, currentHandler)
						},
					)
				}
				chainedHandler := serverHandler
				for i := n - 1; i >= 0; i-- {
					chainedHandler = chainer(interceptors[i], chainedHandler)
				}
				return chainedHandler.Handle(ctx, request)
			},
		)
	}
}
