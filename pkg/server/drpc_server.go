// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"crypto/tls"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"storj.io/drpc"
	"storj.io/drpc/drpcerr"
)

// drpcServer wraps a DRPCServer and manages its enabled state and TLS
// configuration. It also tracks the current serving mode for health checks.
type drpcServer struct {
	// Embeds logic for managing server mode (initializing, draining, operational).
	serveModeHandler
	// Underlying DRPC server implementation.
	rpc.DRPCServer
	// TLS configuration for secure connections.
	tlsCfg *tls.Config
}

// newDRPCServer creates and configures a new drpcServer instance. It enables
// DRPC if the experimental setting is on, otherwise returns a dummy server.
func newDRPCServer(
	ctx context.Context, rpcCtx *rpc.Context, requestMetrics *rpc.RequestMetrics,
) (*drpcServer, error) {
	d := &drpcServer{}
	d.setMode(modeInitializing)

	dsrv, err := rpc.NewDRPCServer(
		ctx,
		rpcCtx,
		rpc.WithInterceptor(
			func(path string) error {
				return d.intercept(path)
			}),
		rpc.WithDRPCMetricsServerInterceptor(
			rpc.NewDRPCRequestMetricsInterceptor(requestMetrics, func(method string) bool {
				return shouldRecordRequestDuration(rpcCtx.Settings, method)
			}),
		))
	if err != nil {
		return nil, err
	}

	tlsCfg, err := rpcCtx.GetServerTLSConfig()
	if err != nil {
		return nil, err
	}

	d.DRPCServer = dsrv
	d.tlsCfg = tlsCfg

	if err := rpc.DRPCRegisterHeartbeat(d, rpcCtx.NewHeartbeatService()); err != nil {
		return nil, err
	}

	return d, nil
}

// health returns an error if the server is not operational, encoding the error
// with a DRPC code. Returns nil if the server is healthy and operational.
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

// drpcServiceRegistrar is implemented by servers that create a DRPC server and
// registers it with drpc.Mux
type drpcServiceRegistrar interface {
	RegisterDRPCService(drpc.Mux) error
}
