// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/redact"
)

// peerKey is used as key in the Context.peers map.
// Connections which carry a different class but share a target and nodeID
// will always specify distinct connections. Different remote node IDs get
// distinct *Connection objects to ensure that we don't mis-route RPC
// requests in the face of address reuse. Gossip connections and other
// non-Internal users of the Context are free to dial nodes without
// specifying a node ID (see GRPCUnvalidatedDial()) however later calls to
// Dial with the same target and class with a node ID will create a new
// underlying connection which will not be reused by calls specifying the
// NodeID.
type peerKey struct {
	TargetAddr string
	// NodeID of remote node, 0 when unknown, non-zero to check with remote node.
	// Never mutated.
	NodeID roachpb.NodeID
	Class  ConnectionClass
}

var _ redact.SafeFormatter = peerKey{}

// SafeFormat implements the redact.SafeFormatter interface.
func (c peerKey) SafeFormat(p redact.SafePrinter, _ rune) {
	p.Printf("{n%d: %s (%v)}", c.NodeID, c.TargetAddr, c.Class)
}

type peerMap[Conn rpcConn] struct {
	mu struct {
		syncutil.RWMutex
		m map[peerKey]*peer[Conn]
	}
}

func (peers *peerMap[Conn]) getWithBreaker(
	k peerKey,
) (PeerSnap[Conn], peerMetrics, *circuit.Breaker, bool) {
	peers.mu.RLock()
	defer peers.mu.RUnlock()
	p := peers.mu.m[k]
	if p == nil {
		return PeerSnap[Conn]{}, peerMetrics{}, nil, false
	}
	return p.snap(), p.peerMetrics, p.b, true
}

// Conn returns a read-only version of the peer and a boolean indicating
// whether the peer exists.
func (peers *peerMap[Conn]) get(k peerKey) (PeerSnap[Conn], bool) {
	peers.mu.RLock()
	defer peers.mu.RUnlock()
	return peers.getRLocked(k)
}

func (peers *peerMap[Conn]) getRLocked(k peerKey) (PeerSnap[Conn], bool) {
	p, ok := peers.mu.m[k]
	if !ok {
		return PeerSnap[Conn]{}, false
	}
	return p.snap(), true
}
