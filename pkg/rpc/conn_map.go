// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type connMap struct {
	mu struct {
		syncutil.RWMutex
		m map[connKey]*peer
	}
}

func (m *connMap) getWithBreaker(k connKey) (peerSnap, *circuit.Breaker, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	p := m.mu.m[k]
	if p == nil {
		return peerSnap{}, nil, false
	}
	return p.snap(), p.b, true
}

// Conn returns a read-only version of the peer and a boolean indicating
// whether the peer exists.
func (m *connMap) get(k connKey) (peerSnap, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.getRLocked(k)
}

func (m *connMap) getRLocked(k connKey) (peerSnap, bool) {
	p, ok := m.mu.m[k]
	if !ok {
		return peerSnap{}, false
	}
	// Check the error to make sure the heartbeat loop (=breaker probe) is running
	// even if the caller won't check the circuit breaker.
	_ = p.b.Signal().Err()
	return p.snap(), true
}
