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

package grpcclient

import (
	"context"
	"crypto/tls"

	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// ClientConnProvider provides client connections.
type ClientConnProvider interface {
	// NewClientConn gets a client conn for the given address. It may reuse a pooled client conn.
	//
	// The given Context is only used for dialing if the connection is not already cached.
	NewClientConn(ctx context.Context, address string) (grpc.ClientConnInterface, error)
}

// NewClientConnProvider returns a new ClientConnProvider. It handles the closing of the
// clientConns created through it.
func NewClientConnProvider(
	ctx context.Context,
	logger *zap.Logger,
	options ...ClientConnProviderOption,
) (ClientConnProvider, error) {
	return newClientConnProvider(ctx, logger, options...)
}

// ClientConnProviderOption is an option for a new ClientConn.
type ClientConnProviderOption func(*clientConnProvider)

// ClientConnProviderWithTLSConfig returns a new ClientConnProviderOption to use the tls.Config.
//
// The default is to use no TLS.
func ClientConnProviderWithTLSConfig(tlsConfig *tls.Config) ClientConnProviderOption {
	return func(clientConnProvider *clientConnProvider) {
		clientConnProvider.tlsConfig = tlsConfig
	}
}

// ClientConnProviderWithObservability returns a new ClientConnProviderOption to use
// OpenCensus tracing and metrics.
//
// The default is to use no observability.
func ClientConnProviderWithObservability() ClientConnProviderOption {
	return func(clientConnProvider *clientConnProvider) {
		clientConnProvider.observability = true
	}
}

// ClientConnProviderWithGZIPCompression returns a new ClientConnProviderOption that
// enables gzip compression.
//
// The default is to not use compression.
func ClientConnProviderWithGZIPCompression() ClientConnProviderOption {
	return func(clientConnProvider *clientConnProvider) {
		clientConnProvider.gzipCompression = true
	}
}
