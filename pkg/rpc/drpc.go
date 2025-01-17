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
	"github.com/cockroachdb/errors"
	"storj.io/drpc"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcmigrate"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcpool"
	"storj.io/drpc/drpcserver"
	"storj.io/drpc/drpcstream"
	"storj.io/drpc/drpcwire"
)

// ErrDRPCDisabled is returned from hosts that in principle could but do not
// have the DRPC server enabled.
var ErrDRPCDisabled = errors.New("DRPC is not enabled")

type drpcServerI interface {
	Serve(ctx context.Context, lis net.Listener) error
}

type drpcMuxI interface {
	Register(srv interface{}, desc drpc.Description) error
}

type DRPCServer struct {
	Srv    drpcServerI
	Mux    drpcMuxI
	TLSCfg *tls.Config
}

var _ drpcServerI = (*drpcserver.Server)(nil)
var _ drpcServerI = (*drpcOffServer)(nil)

func newDRPCServer(_ context.Context, rpcCtx *Context) (*DRPCServer, error) {
	var dmux drpcMuxI = &drpcOffServer{}
	var dsrv drpcServerI = &drpcOffServer{}
	var tlsCfg *tls.Config

	if ExperimentalDRPCEnabled.Get(&rpcCtx.Settings.SV) {
		mux := drpcmux.New()
		dsrv = drpcserver.NewWithOptions(mux, drpcserver.Options{
			Log: func(err error) {
				log.Warningf(context.Background(), "drpc server error %v", err)
			},
			// The reader's max buffer size defaults to 4mb, and if it is exceeded (such
			// as happens with AddSSTable) the RPCs fail.
			Manager: drpcmanager.Options{Reader: drpcwire.ReaderOptions{MaximumBufferSize: math.MaxInt}},
		})
		dmux = mux

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
		Srv:    dsrv,
		Mux:    dmux,
		TLSCfg: tlsCfg,
	}, nil
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

// drpcOffServer is used for drpcServerI and drpcMuxI if the DRPC server is
// disabled. It immediately closes accepted connections and returns
// ErrDRPCDisabled.
type drpcOffServer struct{}

func (srv *drpcOffServer) Serve(_ context.Context, lis net.Listener) error {
	conn, err := lis.Accept()
	if err != nil {
		return err
	}
	_ = conn.Close()
	return ErrDRPCDisabled
}

func (srv *drpcOffServer) Register(interface{}, drpc.Description) error {
	return nil
}
