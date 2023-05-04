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
	"context"
	"runtime/pprof"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc"
)

// A peer is a remote node that we are trying to maintain a healthy RPC
// connection (for a given connection class not known to the peer itself) to. It
// maintains metrics on our connection state to the peer (see the embedded
// peerMetrics) and maintains a circuit breaker whose probe is the heartbeat
// loop; the breaker trips whenever a heartbeat fails and resets whenever a
// heartbeat succeeds.
// Usually, the probe is always active (either attempting to heal the connection
// or maintaining its health), but when a peer looks likely to be obsolete (for
// example, remote node is noticed as having been decommissioned) as indicated
// by the `deleteAfter` field being nonzero, the probe only runs on-demand, that
// is, whenever usage of the `peer` is detected (this is done by the circuit
// breaker); see (*peerMap).shouldDeleteAfter.
// When the current time has surpassed `deleteAfter`, the peer will be taken out
// of its surrounding rpc.Context and will no longer probe; see
// (*peer).maybeDelete.
// See (*peer).launch for details on the probe (heartbeat loop) itself.
type peer struct {
	peerMetrics
	k                 peerKey
	opts              *ContextOptions
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration
	dial              func(ctx context.Context, target string, class ConnectionClass) (*grpc.ClientConn, error)
	// b maintains connection health. This breaker's async probe is always
	// active - it is the heartbeat loop and manages `mu.c.` (including
	// recreating it after the connection fails and has to be redialed).
	//
	// NB: at the time of writing, we don't use the breaking capabilities,
	// i.e. we don't check the circuit breaker in `Connect`. We will do that
	// once the circuit breaker is mature, and then retire the breakers
	// returned by Context.getBreaker.
	//
	// Currently what will happen when a peer is down is that `c` will be
	// recreated (blocking new callers to `Connect()`), a connection attempt
	// will be made, and callers will see the failure to this attempt.
	//
	// With the breaker, callers would be turned away eagerly until there
	// is a known-healthy connection.
	//
	// mu must *NOT* be held while operating on `b`. This is because the async
	// probe will sometimes have to synchronously acquire mu before spawning off.
	b                  *circuit.Breaker
	logDisconnectEvery log.EveryN
	mu                 struct {
		syncutil.Mutex
		// Copies of PeerSnap may be leaked outside of lock, since the memory within
		// is never mutated in place.
		PeerSnap
	}
	remoteClocks *RemoteClockMonitor
	// NB: lock order: peers.mu then peers.mu.m[k].mu (but better to avoid
	// overlapping critical sections)
	peers *peerMap
}

// PeerSnap is the state of a peer.
type PeerSnap struct {
	c *Connection // never nil, only mutated in the breaker probe

	// Timestamp of latest successful initial heartbeat on `c`. This
	// is never cleared: it only ever moves forward. If the peer is
	// currently unhealthy (i.e. breaker tripped), `disconnected` will
	// be larger than `connected`. Otherwise, the peer is healthy.
	//
	// Example:
	// t=100: peer created (connected=0, disconnected=0)
	// t=120: peer heartbeats successfully (connected=120, disconnected=0)
	// t=130: peer heartbeats successfully (connected=120, disconnected=0)
	// t=150: peer fails heartbeat (connected=120, disconnected=150)
	// t=160: peer fails heartbeat (connected=120, disconnected=150)
	// t=170: peer heartbeats successfully (connected=170, disconnected=150)
	// t=200: peer fails heartbeat (connected=170, disconnected=200).
	//
	// INVARIANT: connected > disconnected <=> c.initialHeartbeatDone closed.
	// (Assuming monotonic local walltime).
	connected time.Time
	// disconnected is zero initially, reset on successful heartbeat, set on
	// heartbeat teardown if zero. In other words, does not move forward across
	// subsequent connection failures - it tracks the first disconnect since
	// having been healthy. See comment on `connected` for example.
	//
	// INVARIANT: disconnected != 0 <=> connected == 0.
	// INVARIANT: disconnected != 0  <=> breaker tripped
	// (manual manipulation of the breaker is a programming error)
	disconnected time.Time
	// deleteAfter is nonzero if the peer is failed and "likely not useful any more".
	// This must be set from the probe for that peer, and makes probing on-demand:
	// each attempt to access the breaker triggers a probe, if one is not
	// inflight, but the probe will terminate on failure.
	//
	// If the peer becomes healthy again, deleteAfter is cleared.
	//
	// Comments in shouldDeleteAfter document when a deletedAfter is set. This is somewhat
	// complex, owing to the possibility of listening addresses swapping around between
	// nodes, as well as the existence of unvalidated (NodeID-less) connections.
	//
	// INVARIANT: if deleteAfter != nil, all gauge contributions to metrics are zero.
	// INVARIANT: deleteAfter != 0 => disconnected != 0
	deleteAfter time.Duration
	// deleted indicates that the peer was removed from map and unregistered from
	// metrics. The probe must not do any more work (in particular the gauges must
	// no longer be touched).
	//
	// INVARIANT: deleted once => deleted forever
	// INVARIANT: deleted      => deleteAfter > 0
	deleted bool
}

func (p PeerSnap) deletable(now time.Time) bool {
	if p.deleteAfter == 0 {
		return false
	}
	ts := p.disconnected.Add(p.deleteAfter)
	return now.After(ts)
}

func (p *peer) snap() PeerSnap {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.mu.PeerSnap
}

// newPeer returns circuit breaker that trips when connection (associated
// with provided peerKey) is failed. The breaker's probe *is* the heartbeat loop
// and is thus running at all times. The exception is a decommissioned node, for
// which the probe simply exits (any future connection attempts to the same peer
// will trigger the probe but the probe will exit again), and a superseded peer,
// i.e. one for which a node restarted with a different IP address and we're the
// "old", unhealth, peer.
//
// Multiple peers to a given node can temporarily exist at any given point
// in time (if the node restarts under a different IP). We assume that
// ultimately one of those will become unhealthy and repeatedly fail its
// probe. On probe failure, we check the node map for duplicates and if a
// healthy duplicate exists, remove ourselves from the map. In the worst
// case, we are actually the new connection to the remote and got tripped
// up by a transient error while the old connection hasn't realized yet that
// it's dead - in that case, because we immediately remove ourselves from the
// map, the next attempt to dial the node will start from a blank slate. In
// other words, even with this theoretical race, the situation will sort itself
// out quickly.
func (rpcCtx *Context) newPeer(k peerKey) *peer {
	// Initialization here is a bit circular. The peer holds the breaker. The
	// breaker probe references the peer because it needs to replace the one-shot
	// Connection when it makes a new connection in the probe. And (all but the
	// first incarnation of) the Connection also holds on to the breaker since the
	// Connect method needs to do the short-circuiting (if a Connection is created
	// while the breaker is tripped, we want to block in Connect only once we've
	// seen the first heartbeat succeed).
	p := &peer{
		peerMetrics:        rpcCtx.metrics.acquire(k),
		logDisconnectEvery: log.Every(time.Minute),
		k:                  k,
		remoteClocks:       rpcCtx.RemoteClocks,
		opts:               &rpcCtx.ContextOptions,
		peers:              &rpcCtx.peers,
		dial: func(ctx context.Context, target string, class ConnectionClass) (*grpc.ClientConn, error) {
			return rpcCtx.grpcDialRaw(ctx, target, class, rpcCtx.testingDialOpts...)
		},
		heartbeatInterval: rpcCtx.heartbeatInterval,
		heartbeatTimeout:  rpcCtx.heartbeatTimeout,
	}
	var b *circuit.Breaker

	ctx := rpcCtx.makeDialCtx(k.TargetAddr, k.NodeID, k.Class)
	b = circuit.NewBreaker(circuit.Options{
		Name: "breaker", // log tags already represent `k`
		AsyncProbe: func(report func(error), done func()) {
			pprof.Do(ctx, pprof.Labels("tags", logtags.FromContext(ctx).String()), func(ctx context.Context) {
				p.launch(ctx, report, done)
			})
		},
		// Use a noop EventHandler; we do our own logging in the probe since we'll
		// have better information.
		EventHandler: &circuit.EventLogger{Log: func(buf redact.StringBuilder) {}},
	})
	p.b = b
	c := newConnectionToNodeID(k, b.Signal)
	p.mu.PeerSnap = PeerSnap{c: c}

	return p
}
