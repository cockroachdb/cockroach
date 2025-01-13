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

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"storj.io/drpc"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcmigrate"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcpool"
	"storj.io/drpc/drpcserver"
	"storj.io/drpc/drpcstats"
	"storj.io/drpc/drpcstream"
	"storj.io/drpc/drpcwire"
)

type DRPCServer struct {
	Srv    *drpcServerWrapper
	Mux    *drpcMuxWrapper
	TLSCfg *tls.Config
}

func newDRPCServer(_ context.Context, rpcCtx *Context) (*DRPCServer, error) {
	var dmuxw *drpcMuxWrapper = &drpcMuxWrapper{}
	var dsrvw *drpcServerWrapper = &drpcServerWrapper{}
	var tlsCfg *tls.Config

	if ExperimentalDRPCEnabled.Get(&rpcCtx.Settings.SV) {
		dmuxw.dmux = drpcmux.New()
		dsrvw.Dsrv = drpcserver.NewWithOptions(dmuxw.dmux, drpcserver.Options{
			Log: func(err error) {
				log.Warningf(context.Background(), "drpc server error %v", err)
			},
			// The reader's max buffer size defaults to 4mb, and if it is exceeded (such
			// as happens with AddSSTable) the RPCs fail.
			Manager: drpcmanager.Options{Reader: drpcwire.ReaderOptions{MaximumBufferSize: math.MaxInt}},
		})

		var err error
		tlsCfg, err = rpcCtx.GetServerTLSConfig()
		if err != nil {
			return nil, err
		}

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
	}

	return &DRPCServer{
		Srv:    dsrvw,
		Mux:    dmuxw,
		TLSCfg: tlsCfg,
	}, nil
}

// drpcServerWrapper allows us to use a mock server implementation when DRPC is
// not enabled.
type drpcServerWrapper struct {
	Dsrv *drpcserver.Server
}

// Serve listens for connections on the provided listener and handles DRPC
// requests on new connections when DRPC is enabled. If DRPC is not enabled,
// it returns an unsupported operation error.
func (s *drpcServerWrapper) Serve(ctx context.Context, lis net.Listener) (err error) {
	if s.Dsrv != nil {
		return s.Dsrv.Serve(ctx, lis)
	}

	// Accept() always return an error
	_, err = lis.Accept()
	if err != nil {
		return err
	}
	return nil
}

// ServeOne serves a single set of rpcs on the provided transport when DRPC
// is enabled. If DRPC is not enabled, it returns an unsupported opertion
// error.
func (s *drpcServerWrapper) ServeOne(ctx context.Context, tr drpc.Transport) (err error) {
	if s.Dsrv != nil {
		return s.Dsrv.ServeOne(ctx, tr)
	}
	return nil
}

// Stats returns the collected stats grouped by rpc when DRPC is enabled. If
// DRPC is not enabled, it returns empty stats.
func (s *drpcServerWrapper) Stats() map[string]drpcstats.Stats {
	if s.Dsrv != nil {
		return s.Dsrv.Stats()
	}
	return map[string]drpcstats.Stats{}
}

// drpcMuxWrapper allows us to use a mock mux implementation when DRPC is
// not enabled.
type drpcMuxWrapper struct {
	dmux *drpcmux.Mux
}

func (d *drpcMuxWrapper) Register(srv interface{}, desc drpc.Description) error {
	if d.dmux != nil {
		return d.dmux.Register(srv, desc)
	}
	return nil
}

func dialDRPC(rpcCtx *Context) func(ctx context.Context, target string) (drpcpool.Conn, error) {
	return func(ctx context.Context, target string) (drpcpool.Conn, error) {
		// TODO(server): could use connection class instead of empty key here.
		pool := drpcpool.New[struct{}, drpcpool.Conn](drpcpool.Options{})
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
				tlsConn := tls.Client(netConn, tlsConfig)
				conn = drpcconn.NewWithOptions(tlsConn, opts)
			}

			return conn, nil
		})
		// `pooledConn.Close` doesn't tear down any of the underlying TCP
		// connections but simply marks the pooledConn handle as returning
		// errors. When we "close" this conn, we want to tear down all of
		// the connections in the pool (in effect mirroring the behavior of
		// gRPC where a single conn is shared).
		return &closeEntirePoolConn{
			Conn: pooledConn,
			pool: pool,
		}, nil
	}
}

type closeEntirePoolConn struct {
	drpcpool.Conn
	pool *drpcpool.Pool[struct{}, drpcpool.Conn]
}

func (c *closeEntirePoolConn) Close() error {
	_ = c.Conn.Close()
	return c.pool.Close()
}
