// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// grpcServer is a wrapper on top of a grpc.Server that includes an interceptor
// and a mode of operation that can instruct the interceptor to refuse certain
// RPCs.
type grpcServer struct {
	*grpc.Server
	serverInterceptorsInfo rpc.ServerInterceptorInfo
	mode                   serveMode
}

func newGRPCServer(rpcCtx *rpc.Context) (*grpcServer, error) {
	s := &grpcServer{}
	s.mode.set(modeInitializing)
	srv, interceptorInfo, err := rpc.NewServerEx(
		rpcCtx, rpc.WithInterceptor(func(path string) error {
			return s.intercept(path)
		}))
	if err != nil {
		return nil, err
	}
	s.Server = srv
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
