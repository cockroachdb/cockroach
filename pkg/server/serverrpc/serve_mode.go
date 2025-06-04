// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serverrpc

import "sync/atomic"

type ServeMode int32

// A list of the server states for bootstrap process.
const (
	// modeInitializing is intended for server initialization process.
	// It allows only bootstrap, heartbeat and gossip methods
	// to prevent calls to potentially uninitialized services.
	ModeInitializing ServeMode = iota
	// modeOperational is intended for completely initialized server
	// and thus allows all RPC methods.
	ModeOperational
	// modeDraining is intended for an operational server in the process of
	// shutting down. The difference is that readiness checks will fail.
	ModeDraining
)

func (s *ServeMode) Set(mode ServeMode) {
	atomic.StoreInt32((*int32)(s), int32(mode))
}

func (s *ServeMode) Get() ServeMode {
	return ServeMode(atomic.LoadInt32((*int32)(s)))
}

func (s *ServeMode) Operational() bool {
	sMode := s.Get()
	return sMode == ModeOperational || sMode == ModeDraining
}
