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
	"sync/atomic"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

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

type serveMode int32

// Intercept implements filtering rules for each server state.
func (s *Server) Intercept() func(string) error {
	interceptors := map[string]struct{}{
		"/cockroach.rpc.Heartbeat/Ping":             {},
		"/cockroach.gossip.Gossip/Gossip":           {},
		"/cockroach.server.serverpb.Init/Bootstrap": {},
		"/cockroach.server.serverpb.Status/Details": {},
	}
	return func(fullName string) error {
		if s.serveMode.operational() {
			return nil
		}
		if _, allowed := interceptors[fullName]; !allowed {
			return grpcstatus.Errorf(
				codes.Unavailable, "node waiting for init; %s not available", fullName,
			)
		}
		return nil
	}
}

func (s *serveMode) set(mode serveMode) {
	atomic.StoreInt32((*int32)(s), int32(mode))
}

func (s *serveMode) get() serveMode {
	return serveMode(atomic.LoadInt32((*int32)(s)))
}

func (s *serveMode) operational() bool {
	sMode := s.get()
	return sMode == modeOperational || sMode == modeDraining
}
