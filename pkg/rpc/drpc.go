// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
	"storj.io/drpc/drpcwire"
)

func newDRPCServer(ctx context.Context, rpcCtx *Context) (*DRPCServer, error) {
	dmux := drpcmux.New()
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

	dsrv := drpcserver.NewWithOptions(dmux, drpcserver.Options{
		Log: func(err error) {
			log.Warningf(context.Background(), "drpc server error %v", err)
		},
		// The reader's max buffer size defaults to 4mb, and if it is exceeded (such
		// as happens with AddSSTable) the RPCs fail.
		Manager: drpcmanager.Options{Reader: drpcwire.ReaderOptions{MaximumBufferSize: math.MaxInt}},
	})

	tlsCfg, err := rpcCtx.GetServerTLSConfig()
	if err != nil {
		return nil, err
	}

	return &DRPCServer{
		Srv:    dsrv,
		Mux:    dmux,
		TLSCfg: tlsCfg,
	}, nil
}
