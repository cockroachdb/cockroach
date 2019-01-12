// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"strings"
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
	mode serveMode
}

func newGRPCServer(rpcCtx *rpc.Context) *grpcServer {
	s := &grpcServer{}
	s.mode.set(modeInitializing)
	s.Server = rpc.NewServerWithInterceptor(rpcCtx, func(path string) error {
		return s.intercept(path)
	})
	return s
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
	"/cockroach.server.serverpb.Status/Details": {},
}

// intercept implements filtering rules for each server state.
func (s *grpcServer) intercept(fullName string) error {
	if s.operational() {
		return nil
	}
	if _, allowed := rpcsAllowedWhileBootstrapping[fullName]; !allowed {
		return s.waitingForInitError(fullName)
	}
	return nil
}

func (s *serveMode) set(mode serveMode) {
	atomic.StoreInt32((*int32)(s), int32(mode))
}

func (s *serveMode) get() serveMode {
	return serveMode(atomic.LoadInt32((*int32)(s)))
}

// waitingForInitError creates an error indicating that the server cannot run
// the specified method until the node has been initialized.
func (s *grpcServer) waitingForInitError(methodName string) error {
	return grpcstatus.Errorf(codes.Unavailable, "node waiting for init; %s not available", methodName)
}

// IsWaitingForInit checks whether the provided error is because the node is
// still waiting for initialization.
func IsWaitingForInit(err error) bool {
	s, ok := grpcstatus.FromError(err)
	return ok && s.Code() == codes.Unavailable && strings.Contains(err.Error(), "node waiting for init")
}
