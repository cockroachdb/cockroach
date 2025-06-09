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
	"storj.io/drpc/drpcerr"
)

// drpcServer wraps a DRPCServer and manages its enabled state and TLS
// configuration. It also tracks the current serving mode for health checks.
type drpcServer struct {
	// Embeds logic for managing server mode (initializing, draining, operational).
	serveModeHandler
	// Underlying DRPC server implementation.
	rpc.DRPCServer
	// Indicates if DRPC is enabled for this server.
	enabled bool
	// TLS configuration for secure connections.
	tlsCfg *tls.Config
}

// newDRPCServer creates and configures a new drpcServer instance. It enables
// DRPC if the experimental setting is on, otherwise returns a dummy server.
//
// TODO: Register DRPC Heartbeat service
func newDRPCServer(ctx context.Context, rpcCtx *rpc.Context) (*drpcServer, error) {
	drpcServer := &drpcServer{}
	if rpc.ExperimentalDRPCEnabled.Get(&rpcCtx.Settings.SV) {
		d, err := rpc.NewDRPCServer(ctx, rpcCtx)
		if err != nil {
			return nil, err
		}
		drpcServer.DRPCServer = d
		drpcServer.enabled = true
	} else {
		drpcServer.DRPCServer = rpc.NewDummyDRPCServer()
		drpcServer.enabled = false
	}

	tlsCfg, err := rpcCtx.GetServerTLSConfig()
	if err != nil {
		return nil, err
	}

	drpcServer.tlsCfg = tlsCfg
	drpcServer.setMode(modeInitializing)

	return drpcServer, nil
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
