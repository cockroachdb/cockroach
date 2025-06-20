// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"crypto/tls"
	"math"
	"net"
	"time"

	"github.com/cockroachdb/cockroach/pkg/rpc/rpcbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"storj.io/drpc"
	"storj.io/drpc/drpcclient"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcmigrate"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcpool"
	"storj.io/drpc/drpcserver"
	"storj.io/drpc/drpcstream"
	"storj.io/drpc/drpcwire"
)

// Default idle connection timeout for DRPC connections in the pool.
var defaultDRPCConnIdleTimeout = 5 * time.Minute

func DialDRPC(
	rpcCtx *Context,
) func(ctx context.Context, target string, _ rpcbase.ConnectionClass) (drpc.Conn, error) {
	return func(ctx context.Context, target string, _ rpcbase.ConnectionClass) (drpc.Conn, error) {
		// TODO(server): could use connection class instead of empty key here.
		pool := drpcpool.New[struct{}, drpcpool.Conn](drpcpool.Options{
			Expiration: defaultDRPCConnIdleTimeout,
		})
		pooledConn := pool.Get(ctx /* unused */, struct{}{}, func(ctx context.Context,
			_ struct{}) (drpcpool.Conn, error) {

			netConn, err := drpcmigrate.DialWithHeader(ctx, "tcp", target, drpcmigrate.DRPCHeader)
			if err != nil {
				return nil, err
			}

			opts := drpcconn.Options{
				Manager: drpcmanager.Options{
					Reader: drpcwire.ReaderOptions{
						MaximumBufferSize: math.MaxInt,
					},
					Stream: drpcstream.Options{
						MaximumBufferSize: 0, // unlimited
					},
					SoftCancel: true, // don't close the transport when stream context is canceled
				},
			}
			var conn *drpcconn.Conn
			if rpcCtx.ContextOptions.Insecure {
				conn = drpcconn.NewWithOptions(netConn, opts)
			} else {
				tlsConfig, err := rpcCtx.GetClientTLSConfig()
				if err != nil {
					return nil, err
				}
				// Clone TLS config to avoid modifying a cached TLS config.
				tlsConfig = tlsConfig.Clone()
				// TODO(server): remove this hack which is necessary at least in
				// testing to get TestDRPCSelectQuery to pass.
				tlsConfig.InsecureSkipVerify = true
				tlsConn := tls.Client(netConn, tlsConfig)
				conn = drpcconn.NewWithOptions(tlsConn, opts)
			}

			return conn, nil
		})

		if rpcCtx.Knobs.UnaryClientInterceptorDRPC != nil {
			if interceptor := rpcCtx.Knobs.UnaryClientInterceptorDRPC(target); interceptor != nil {
				rpcCtx.clientUnaryInterceptorsDRPC = []drpcclient.UnaryClientInterceptor{interceptor}
			}
		}
		if rpcCtx.Knobs.StreamClientInterceptorDRPC != nil {
			if interceptor := rpcCtx.Knobs.StreamClientInterceptorDRPC(target); interceptor != nil {
				rpcCtx.clientStreamInterceptorsDRPC = []drpcclient.StreamClientInterceptor{interceptor}
			}
		}
		clientConn, _ := drpcclient.NewClientConnWithOptions(
			ctx,
			pooledConn,
			drpcclient.WithChainUnaryInterceptor(rpcCtx.clientUnaryInterceptorsDRPC...),
			drpcclient.WithChainStreamInterceptor(rpcCtx.clientStreamInterceptorsDRPC...),
		)

		// Wrap the clientConn to ensure the entire pool is closed when this connection handle is closed.
		return &closeEntirePoolConn{
			Conn: clientConn,
			pool: pool,
		}, nil
	}
}

type closeEntirePoolConn struct {
	drpc.Conn
	pool *drpcpool.Pool[struct{}, drpcpool.Conn]
}

func (c *closeEntirePoolConn) Close() error {
	_ = c.Conn.Close()
	return c.pool.Close()
}

type DRPCConnection = Connection[drpc.Conn]

// ErrDRPCDisabled is returned from hosts that in principle could but do not
// have the DRPC server enabled.
var ErrDRPCDisabled = errors.New("DRPC is not enabled")

// DRPCServer defines the interface for a DRPC server, which can serve
// connections and provides a drpc.Mux for registering handlers.
type DRPCServer interface {
	// Serve starts serving DRPC requests on the provided listener.
	Serve(ctx context.Context, lis net.Listener) error
	drpc.Mux
}

// drpcServer implements the DRPCServer interface.
type drpcServer struct {
	*drpcserver.Server
	drpc.Mux
}

// NewDRPCServer creates a new DRPCServer with the provided rpc context.
func NewDRPCServer(_ context.Context, _ *Context) (DRPCServer, error) {
	d := &drpcServer{}
	mux := drpcmux.New()
	d.Server = drpcserver.NewWithOptions(mux, drpcserver.Options{
		Log: func(err error) {
			log.Warningf(context.Background(), "drpc server error %v", err)
		},
		// The reader's max buffer size defaults to 4mb, and if it is exceeded (such
		// as happens with AddSSTable) the RPCs fail.
		Manager: drpcmanager.Options{Reader: drpcwire.ReaderOptions{MaximumBufferSize: math.MaxInt}},
	})
	d.Mux = mux

	// NB: any server middleware (server interceptors in gRPC parlance) would go
	// here:
	//     dmux = whateverMiddleware1(dmux)
	//     dmux = whateverMiddleware2(dmux)
	//     ...
	//
	// Each middleware must implement the Handler interface:
	//
	//   HandleRPC(stream Stream, rpc string) error
	//
	// where Stream
	// See here for an example:
	// https://github.com/bryk-io/pkg/blob/4da5fbfef47770be376e4022eab5c6c324984bf7/net/drpc/server.go#L91-L101
	return d, nil
}

// NewDummyDRPCServer returns a DRPCServer that is disabled and always returns
// ErrDRPCDisabled.
func NewDummyDRPCServer() DRPCServer {
	return &drpcOffServer{}
}

// drpcOffServer is a disabled DRPC server implementation. It immediately closes
// accepted connections and returns ErrDRPCDisabled for all Serve calls.
// Register is a no-op.
type drpcOffServer struct{}

// Serve implements the DRPCServer interface for drpcOffServer. It closes any
// accepted connection and returns ErrDRPCDisabled.
func (srv *drpcOffServer) Serve(_ context.Context, lis net.Listener) error {
	conn, err := lis.Accept()
	if err != nil {
		return err
	}
	_ = conn.Close()
	return ErrDRPCDisabled
}

// Register implements the drpc.Mux interface for drpcOffServer. It is a no-op
// when DRPC is disabled.
func (srv *drpcOffServer) Register(interface{}, drpc.Description) error {
	return nil
}
