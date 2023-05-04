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

// TODO(during review): once dust settles, rename file to peer_map.go.

type peerMap struct {
	mu struct {
		syncutil.RWMutex
		m map[peerKey]*peer
	}
}

func (peers *peerMap) getWithBreaker(k peerKey) (PeerSnap, peerMetrics, *circuit.Breaker, bool) {
	peers.mu.RLock()
	defer peers.mu.RUnlock()
	p := peers.mu.m[k]
	if p == nil {
		return PeerSnap{}, peerMetrics{}, nil, false
	}
	return p.snap(), p.peerMetrics, p.b, true
}

// Conn returns a read-only version of the peer and a boolean indicating
// whether the peer exists.
func (peers *peerMap) get(k peerKey) (PeerSnap, bool) {
	peers.mu.RLock()
	defer peers.mu.RUnlock()
	return peers.getRLocked(k)
}

func (peers *peerMap) getRLocked(k peerKey) (PeerSnap, bool) {
	p, ok := peers.mu.m[k]
	if !ok {
		return PeerSnap{}, false
	}
	return p.snap(), true
}
