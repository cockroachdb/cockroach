// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// grpcServer is a wrapper on top of a grpc.Server that includes an interceptor
// and a mode of operation that can instruct the interceptor to refuse certain
// RPCs.
type grpcServer struct {
	*grpc.Server
	drpc                   *rpc.DRPCServer
	serverInterceptorsInfo rpc.ServerInterceptorInfo
	mode                   serveMode
}

func newGRPCServer(ctx context.Context, rpcCtx *rpc.Context) (*grpcServer, error) {
	s := &grpcServer{}
	s.mode.set(modeInitializing)
	srv, dsrv, interceptorInfo, err := rpc.NewServerEx(
		ctx, rpcCtx, rpc.WithInterceptor(func(path string) error {
			return s.intercept(path)
		}))
	if err != nil {
		return nil, err
	}
	s.Server = srv
	s.drpc = dsrv
	s.serverInterceptorsInfo = interceptorInfo
	return s, nil
}

type serveMode int32

// A list of the server states for bootstrap process.
const (
	// modeInitializing is intended for server initialization process.
	// It allows only bootstrap, heartbeat and gossip methods
	// to prevent calls to potentially uninitialized services.
	modeInitializing serveMode = iota
	// modeOperational is intended for completely initialized server
	// and thus allows all RPC methods.
	modeOperational
	// modeDraining is intended for an operational server in the process of
	// shutting down. The difference is that readiness checks will fail.
	modeDraining
)

func (s *grpcServer) setMode(mode serveMode) {
	s.mode.set(mode)
}

func (s *grpcServer) operational() bool {
	sMode := s.mode.get()
	return sMode == modeOperational || sMode == modeDraining
}

func (s *grpcServer) health(ctx context.Context) error {
	sm := s.mode.get()
	switch sm {
	case modeInitializing:
		return grpcstatus.Error(codes.Unavailable, "node is waiting for cluster initialization")
	case modeDraining:
		// grpc.mode is set to modeDraining when the Drain(DrainMode_CLIENT) has
		// been called (client connections are to be drained).
		return grpcstatus.Errorf(codes.Unavailable, "node is shutting down")
	case modeOperational:
		return nil
	default:
		return srverrors.ServerError(ctx, errors.Newf("unknown mode: %v", sm))
	}
}

var rpcsAllowedWhileBootstrapping = map[string]struct{}{
	"/cockroach.rpc.Heartbeat/Ping":             {},
	"/cockroach.gossip.Gossip/Gossip":           {},
	"/cockroach.server.serverpb.Init/Bootstrap": {},
	"/cockroach.server.serverpb.Admin/Health":   {},
}

// intercept implements filtering rules for each server state.
func (s *grpcServer) intercept(fullName string) error {
	if s.operational() {
		return nil
	}
	if _, allowed := rpcsAllowedWhileBootstrapping[fullName]; !allowed {
		return NewWaitingForInitError(fullName)
	}
	return nil
}

func (s *serveMode) set(mode serveMode) {
	atomic.StoreInt32((*int32)(s), int32(mode))
}

func (s *serveMode) get() serveMode {
	return serveMode(atomic.LoadInt32((*int32)(s)))
}

// NewWaitingForInitError creates an error indicating that the server cannot run
// the specified method until the node has been initialized.
func NewWaitingForInitError(methodName string) error {
	// NB: this error string is sadly matched in grpcutil.IsWaitingForInit().
	return grpcstatus.Errorf(codes.Unavailable, "node waiting for init; %s not available", methodName)
}
