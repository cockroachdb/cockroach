// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"crypto/tls"
	"math"
	"net"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"storj.io/drpc"
	"storj.io/drpc/drpcerr"
	"storj.io/drpc/drpcmanager"
	"storj.io/drpc/drpcmux"
	"storj.io/drpc/drpcserver"
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

type drpcServer struct {
	serveModeHandler
	srv     drpcServerI
	mux     drpcMuxI
	tlsCfg  *tls.Config
	enabled bool
}

var _ drpcServerI = (*drpcserver.Server)(nil)
var _ drpcServerI = (*drpcOffServer)(nil)

// TODO: Register DRPC Heartbeat service
func newDRPCServer(_ context.Context, rpcCtx *rpc.Context) (*drpcServer, error) {
	var dmux drpcMuxI = &drpcOffServer{}
	var dsrv drpcServerI = &drpcOffServer{}
	var tlsCfg *tls.Config
	enabled := false

	if rpc.ExperimentalDRPCEnabled.Get(&rpcCtx.Settings.SV) {
		enabled = true
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

	d := &drpcServer{
		srv:     dsrv,
		mux:     dmux,
		tlsCfg:  tlsCfg,
		enabled: enabled,
	}

	d.setMode(modeInitializing)

	return d, nil
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

func (s *drpcServer) health(ctx context.Context) error {
	sm := s.mode.get()
	switch sm {
	case modeInitializing:
		return drpcerr.WithCode(errors.New("node is waiting for cluster initialization"), uint64(codes.Unavailable))
	case modeDraining:
		return drpcerr.WithCode(errors.New("node is shutting down"), uint64(codes.Unavailable))
	case modeOperational:
		return nil
	default:
		return srverrors.ServerError(ctx, errors.Newf("unknown mode: %v", sm))
	}
}
