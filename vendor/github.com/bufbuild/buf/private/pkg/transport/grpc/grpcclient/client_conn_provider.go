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
	"errors"
	"fmt"
	"sync"

	"github.com/bufbuild/buf/private/pkg/rpc/rpcgrpc"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
)

const maxMessageSizeBytes = 256 << 20

type clientConnProvider struct {
	ctx             context.Context
	logger          *zap.Logger
	tlsConfig       *tls.Config
	observability   bool
	gzipCompression bool

	cachedDialOptions []grpc.DialOption
	cache             map[string]*grpc.ClientConn
	lock              sync.RWMutex // protects cache
}

func newClientConnProvider(
	ctx context.Context,
	logger *zap.Logger,
	options ...ClientConnProviderOption,
) (*clientConnProvider, error) {
	clientConnProvider := &clientConnProvider{
		ctx:    ctx,
		logger: logger.Named("grpcclient"),
		cache:  make(map[string]*grpc.ClientConn),
	}
	for _, option := range options {
		option(clientConnProvider)
	}
	// cache dial options as they are static
	dialOptions, err := clientConnProvider.getDialOptions()
	if err != nil {
		return nil, err
	}
	clientConnProvider.cachedDialOptions = dialOptions
	return clientConnProvider, nil
}

func (c *clientConnProvider) NewClientConn(ctx context.Context, address string) (grpc.ClientConnInterface, error) {
	if address == "" {
		return nil, errors.New("address is required")
	}
	parsedAddress, err := parseAddress(address)
	if err != nil {
		return nil, err
	}
	c.lock.RLock()
	clientConn, ok := c.cache[parsedAddress]
	c.lock.RUnlock()
	if !ok {
		c.lock.Lock()
		defer c.lock.Unlock()
		clientConn, ok = c.cache[parsedAddress]
		if !ok {
			clientConn, err = c.newClientConnUncached(ctx, parsedAddress)
			if err != nil {
				return nil, err
			}
			// automatically close clientConn on shutdown
			go c.monitorClientConnStatus(parsedAddress, clientConn)
			c.cache[parsedAddress] = clientConn
		}
	}
	return clientConn, nil
}

func (c *clientConnProvider) newClientConnUncached(ctx context.Context, parsedAddress string) (*grpc.ClientConn, error) {
	c.logger.Debug("dial", zap.String("address", parsedAddress))
	clientConn, err := grpc.DialContext(ctx, parsedAddress, c.cachedDialOptions...)
	if err != nil {
		return nil, fmt.Errorf("Could not reach address %s: %w", parsedAddress, err)
	}
	return clientConn, nil
}

func (c *clientConnProvider) monitorClientConnStatus(parsedAddress string, clientConn *grpc.ClientConn) {
	// Wait until context is cancelled
	// Note: in the future we may want to do something more
	// clever here like discard client conns that have spent
	// some amount of time in a specific state (TransientFailure or Shutdown),
	// but for now we're entrusting reconnection strategies to
	// the grpc-go connection manager.
	<-c.ctx.Done()
	c.lock.Lock()
	delete(c.cache, parsedAddress)
	c.lock.Unlock()
	// Always returns an error
	_ = clientConn.Close()
}

func (c *clientConnProvider) getDialOptions() ([]grpc.DialOption, error) {
	defaultCallOptions := []grpc.CallOption{
		// NOT Recv, this is the client
		grpc.MaxCallSendMsgSize(maxMessageSizeBytes),
		// NOT Send, this is the client
		grpc.MaxCallRecvMsgSize(maxMessageSizeBytes),
	}
	if !c.gzipCompression {
		defaultCallOptions = append(
			defaultCallOptions,
			// By using gzip.Name, gzip is imported, which registers the
			// gzip Compressor.
			//
			// https://pkg.go.dev/google.golang.org/grpc@v1.39.0/encoding/gzip
			grpc.UseCompressor(gzip.Name),
		)
	}
	dialOptions := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			defaultCallOptions...,
		),
		grpc.WithUnaryInterceptor(
			rpcgrpc.NewUnaryClientInterceptor(),
		),
		grpc.WithStreamInterceptor(
			rpcgrpc.NewStreamClientInterceptor(),
		),
	}
	if c.tlsConfig != nil {
		dialOptions = append(
			dialOptions,
			grpc.WithTransportCredentials(
				credentials.NewTLS(c.tlsConfig),
			),
		)
	} else {
		dialOptions = append(
			dialOptions,
			grpc.WithInsecure(),
		)
	}
	if c.observability {
		dialOptions = append(
			dialOptions,
			grpc.WithStatsHandler(
				&ocgrpc.ClientHandler{
					StartOptions: trace.StartOptions{
						Sampler: trace.AlwaysSample(),
					},
				},
			),
		)
	}
	return dialOptions, nil
}
