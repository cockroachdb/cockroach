// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"crypto/tls"
	"math"
	"time"

	"storj.io/drpc"
	"storj.io/drpc/drpcconn"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcmigrate"
	"storj.io/drpc/drpcpool"
	"storj.io/drpc/drpcstream"
	"storj.io/drpc/drpcwire"
)

// Default idle connection timeout for DRPC connections in the pool.
var defaultDRPCConnIdleTimeout = 5 * time.Minute

func dialDRPC(
	rpcCtx *Context,
) func(ctx context.Context, target string, _ ConnectionClass) (drpc.Conn, error) {
	return func(ctx context.Context, target string, _ ConnectionClass) (drpc.Conn, error) {
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
	drpc.Conn
	pool *drpcpool.Pool[struct{}, drpcpool.Conn]
}

func (c *closeEntirePoolConn) Close() error {
	_ = c.Conn.Close()
	return c.pool.Close()
}

type DRPCConnection = Connection[drpc.Conn]
