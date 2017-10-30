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
	GossipMode int32 = iota
	InitMode
	OperationMode
)

// newInterceptor implements filtering rules for each server state.
func newInterceptor(s *Server) func(string) error {
	interceptors := map[int32]map[string]struct{}{
		GossipMode: {
			"/cockroach.rpc.Heartbeat/Ping":   {},
			"/cockroach.gossip.Gossip/Gossip": {},
		},
		InitMode: {
			"/cockroach.server.serverpb.Init/Bootstrap": {},
		},
	}
	modeNames := map[int32]string{
		GossipMode: "gossip",
		InitMode:   "init",
	}
	return func(fullName string) error {
		mode := atomic.LoadInt32(&s.grpcMode)
		if mode == OperationMode {
			return nil
		}
		if _, allowed := interceptors[mode][fullName]; !allowed {
			return grpcstatus.Errorf(
				codes.Unavailable, "node waiting for %s network; %s not available", modeNames[mode], fullName,
			)
		}
		return nil
	}
}

func turnOnGossipMode(s *Server) {
	atomic.StoreInt32(&s.grpcMode, GossipMode)
}

func turnOnInitMode(s *Server) {
	atomic.StoreInt32(&s.grpcMode, InitMode)
}

func turnOnOperationMode(s *Server) {
	atomic.StoreInt32(&s.grpcMode, OperationMode)
}

func isInitMode(s *Server) bool {
	return atomic.LoadInt32(&s.grpcMode) == InitMode
}
