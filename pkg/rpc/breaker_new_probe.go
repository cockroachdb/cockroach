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

// TODO(during review): rename file once the review dust has settled.

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

type breakerProbe struct {
	*peer
	k            peerKey
	remoteClocks *RemoteClockMonitor
	opts         ContextOptions
	// NB: lock order: peers.mu then peers.mu.m[k].mu (but better to avoid
	// overlapping critical sections)
	peers             *peerMap
	dial              func(ctx context.Context, target string, class ConnectionClass) (*grpc.ClientConn, error)
	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration
}

func (p *breakerProbe) disabled() bool {
	return !enableRPCCircuitBreakers.Get(&p.opts.Settings.SV)
}

func (p *breakerProbe) launch(ctx context.Context, report func(error), done func()) {
	// Acquire mu just to show that we can, as the caller is supposed
	// to not hold the lock.
	p.mu.Lock()
	_ = 0 // bypass empty crit section lint
	p.mu.Unlock()

	if errors.Is(p.b.Signal().Err(), ErrNotHeartbeated) {
		// Special case: the probe is just launched for the very first time after
		// creating the connection. Immediately report a nil (but launch the probe)
		// to get the behavior that we want: the peer starts out healthy, but
		// callers will block on initialHeartbeatDone until the result of the first
		// heartbeat is known. In other words, we're optimistic that the first time
		// a node is dialled it will actually turn out healthy.
		report(nil)
	}

	taskName := fmt.Sprintf("conn to n%d@%s/%s", p.k.NodeID, p.k.TargetAddr, p.k.Class)

	if err := p.opts.Stopper.RunAsyncTask(ctx, taskName, func(ctx context.Context) {
		p.run(ctx, report, done)
	}); err != nil {
		// Stopper draining. Since we're trying to launch a probe, we know the
		// breaker is tripped. We overwrite the error since we want errQuiescing
		// (which has a gRPC status), not kvpb.NodeUnavailableError.
		err = errQuiescing
		report(err)
		// We also need to resolve gatedCC because a caller may be waiting on
		// (*Connection).ConnectNoBreaker, and they need to be signaled as well
		// but aren't listening to the stopper.
		p.mu.c.gatedCC.Resolve(nil, errQuiescing)
		done()
	}
}

func (p *breakerProbe) run(ctx context.Context, report func(error), done func()) {
	var t timeutil.Timer
	defer t.Stop()
	defer done()
	// Immediately run probe after breaker circuit is tripped, optimizing for the
	// case in which we can immediately reconnect.
	t.Reset(0)
	for {
		if p.snap().deleted {
			return
		}

		// NB: we don't need to close initialHeartbeatDone in these error cases.
		// Connect() is cancellation-sensitive as well.
		select {
		case <-ctx.Done():
			// Stopper quiescing, node shutting down. Mirroring what breakerProbe.launch
			// does when it can't launch an async task: leave the broken connection around,
			// no need to close initialHeartbeatDone, just report errQuiescing and quit.
			report(errQuiescing)
			return
		case <-t.C:
			t.Read = true
			// Retry every second. Note that if runHeartbeatUntilFailure takes >1, we'll
			// retry immediately once it returns. This means that a connection breaking
			// for the first time is usually followed by an immediate redial attempt.
			t.Reset(p.heartbeatInterval)
		}

		// Peer is currently initializing (first use) or unhealthy (looped around
		// from earlier attempt). `runOnce` will try to establish a connection and
		// keep it healthy for as long as possible. On first error, it will return
		// back to us.
		err := p.runOnce(ctx, report)
		// If ctx is done, Stopper is draining. Unconditionally override the error
		// to clean up the logging in this case.
		if ctx.Err() != nil {
			err = errQuiescing
		}

		// Transition peer into unhealthy state.
		now := p.opts.Clock.Now()
		p.onHeartbeatFailed(ctx, err, now, report)

		// Release peer and delete from map, if appropriate. We'll detect
		// whether this happened after looping around.
		p.maybeDelete(now)

		if errors.Is(err, errQuiescing) {
			// Heartbeat loop ended due to shutdown. Exit the probe, it won't be
			// started again since that means running an async task through the
			// Stopper.
			return
		}

		p.mu.Lock()
		p.mu.c = newConnectionToNodeID(p.k, p.mu.c.breakerSignalFn)
		p.mu.Unlock()
	}
}

func (p *breakerProbe) onHeartbeatFailed(
	ctx context.Context, err error, now time.Time, report func(err error),
) {
	prevErr := p.b.Signal().Err()
	// For simplicity, we have the convention that this method always returns
	// with an error. This is easier to reason about since we're the probe,
	// and - morally speaking - the connection is healthy as long as the
	// probe is running and happy. We don't want to consider a connection
	// healthy when the probe is not running but didn't report an error.
	if err == nil {
		err = errors.AssertionFailedf("unexpected connection shutdown")
	}

	// There might be other peers in the map that are pending deletion, but which
	// are no longer seeing activity. To eventually clear them out, we check all
	// conns when any conn fails. This avoids the need to have an extra goroutine
	// sitting in all of the goroutine stacks we look at during support.
	touchOldPeers(p.peers, now)

	// We're a bit careful with the locking here to avoid acquiring p.peers.mu
	// while holding p.peer.mu.
	var deleteAfter time.Duration
	if ts := shouldDeleteAfter(p.peers, p.k, err); ts != 0 {
		deleteAfter = ts
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	ls := &p.mu.PeerSnap // "locked snap"

	if !ls.c.gatedCC.Resolved() {
		// If the initial heartbeat failed (or we got an error creating the
		// *grpc.ClientConn), wrap the error. More importantly, resolve gatedCC;
		// someone might be waiting on it in ConnectNoBreaker who is not paying
		// attention to the circuit breaker.
		err = &netutil.InitialHeartbeatFailedError{WrappedErr: err}
		ls.c.gatedCC.Resolve(nil /* cc */, err)
	}
	// By convention, we stick to updating breaker before updating peer
	// to make it easier to write non-flaky tests.
	report(err)

	if ls.disconnected.IsZero() || ls.disconnected.Before(ls.connected) {
		ls.disconnected = now
	}
	// If we're not already soft-deleted and soft deletion is indicated now,
	// mark as such.
	if ls.deleteAfter == 0 && deleteAfter != 0 {
		ls.deleteAfter = deleteAfter
	}

	maybeLogOnFailedHeartbeat(ctx, now, err, prevErr, *ls, &p.logDisconnectEvery)

	nConnUnhealthy := int64(1)
	connUnhealthyFor := now.Sub(ls.disconnected).Nanoseconds() + 1 // 1ns for unit tests w/ manual clock
	if ls.deleteAfter != 0 {
		// The peer got marked as pending deletion, so the probe becomes lazy
		// (i.e. we terminate the for-loop here and only probe again when someone
		// consults the breaker). Reset the gauges, causing this peer to not be
		// reflected in aggregate stats any longer.
		nConnUnhealthy = 0
		connUnhealthyFor = 0
	}
	// Gauge updates.
	p.ConnectionHealthy.Update(0)
	p.ConnectionUnhealthy.Update(nConnUnhealthy)
	p.ConnectionHealthyFor.Update(0)
	p.ConnectionUnhealthyFor.Update(connUnhealthyFor)
	// NB: keep this last for TestGrpcDialInternal_ReconnectPeer.
	p.AvgRoundTripLatency.Update(0)
	p.roundTripLatency.Set(0)
	// Counter updates.
	p.ConnectionFailures.Inc(1)
}

func (p *breakerProbe) maybeDelete(now time.Time) {
	// If the peer can be deleted, delete it now.
	//
	// Also delete unconditionally if circuit breakers are (now) disabled. We want
	// to allow that setting to bypass an as wide as possible class of issues, so
	// we completely yank the peer out of the map.
	snap := p.snap()

	if snap.deleted {
		return
	}

	if !p.disabled() && !snap.deletable(now) {
		return
	}

	// Lock order: map, then peer. But here we can do better and
	// not hold both mutexes at the same time.
	//
	// Release metrics in the same critical section as p.deleted=true
	// to make sure the metrics are not updated after release, since that
	// causes the aggregate metrics to drift.

	p.mu.Lock()
	p.mu.deleted = true
	p.peerMetrics.release()
	p.mu.Unlock()

	p.peers.mu.Lock()
	delete(p.peers.mu.m, p.k)
	p.peers.mu.Unlock()
}

func (p *breakerProbe) onInitialHeartbeatSucceeded(
	ctx context.Context, now time.Time, cc *grpc.ClientConn, report func(err error),
) {
	// First heartbeat succeeded. By convention we update the breaker
	// before updating the peer. The other way is fine too, just the
	// tests need to be written a certain way to avoid being flaky.
	report(nil)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.connected = now
	p.mu.deleteAfter = 0

	// Gauge updates.
	p.ConnectionHealthy.Update(1)
	p.ConnectionUnhealthy.Update(0)
	// ConnectionHealthyFor is already zero.
	p.ConnectionUnhealthyFor.Update(0)
	// AvgRoundTripLatency is already zero. We don't use the initial
	// ping since it has overhead of TLS handshake, blocking dialback, etc.

	// Counter updates.
	p.ConnectionHeartbeats.Inc(1)
	// ConnectionFailures is not updated here.

	// Close the channel last which is helpful for unit tests that
	// first wait for a healthy conn to then check metrics.
	p.mu.c.gatedCC.Resolve(cc, nil /* err */)

	logOnHealthy(ctx, p.mu.disconnected, now)
}

func (p *breakerProbe) onSubsequentHeartbeatSucceeded(_ context.Context, now time.Time) {
	// Gauge updates.
	// ConnectionHealthy is already one.
	// ConnectionUnhealthy is already zero.
	p.ConnectionHealthyFor.Update(now.Sub(p.snap().connected).Nanoseconds() + 1) // add 1ns for unit tests w/ manual clock
	// ConnectionUnhealthyFor is already zero.
	p.AvgRoundTripLatency.Update(int64(p.roundTripLatency.Value()) + 1) // add 1ns for unit tests w/ manual clock

	// Counter updates.
	p.ConnectionHeartbeats.Inc(1)
	// ConnectionFailures is not updated here.
}

func (p *breakerProbe) runOnce(ctx context.Context, report func(error)) error {
	cc, err := p.dial(ctx, p.k.TargetAddr, p.k.Class)
	if err != nil {
		return err
	}
	defer func() {
		_ = cc.Close() // nolint:grpcconnclose
	}()

	// Set up notifications on a channel when gRPC tears down, so that we
	// can trigger another instant heartbeat for expedited circuit breaker
	// tripping.
	connFailedCh := make(chan connectivity.State, 1)
	launchConnStateWatcher(ctx, p.opts.Stopper, cc, connFailedCh)

	if p.remoteClocks != nil {
		p.remoteClocks.OnConnect(ctx, p.k.NodeID)
		defer p.remoteClocks.OnDisconnect(ctx, p.k.NodeID)
	}

	if err := runSingleHeartbeat(
		ctx, NewHeartbeatClient(cc), p.k, p.peerMetrics.roundTripLatency, nil /* no remote clocks */, p.opts, p.heartbeatTimeout, PingRequest_BLOCKING,
	); err != nil {
		return err
	}

	p.onInitialHeartbeatSucceeded(ctx, p.opts.Clock.Now(), cc, report)

	return p.runHeartbeatUntilFailure(ctx, connFailedCh)
}

func launchConnStateWatcher(
	ctx context.Context, stopper *stop.Stopper, grpcConn *grpc.ClientConn, ch chan connectivity.State,
) {
	// The connection should be `Ready` now since we just used it for a
	// heartbeat RPC. Any additional state transition indicates that we need
	// to remove it, and we want to do so reactively. Unfortunately, gRPC
	// forces us to spin up a separate goroutine for this purpose even
	// though it internally uses a channel.
	// Note also that the implementation of this in gRPC is clearly racy,
	// so consider this somewhat best-effort.
	_ = stopper.RunAsyncTask(ctx, "conn state watcher", func(ctx context.Context) {
		st := connectivity.Ready
		for {
			if !grpcConn.WaitForStateChange(ctx, st) {
				return
			}
			st = grpcConn.GetState()
			if st == connectivity.TransientFailure || st == connectivity.Shutdown {
				ch <- st
				return
			}
		}
	})
}

func logOnHealthy(ctx context.Context, disconnected, now time.Time) {
	var buf redact.StringBuilder
	_, _ = redact.Fprintf(&buf, "connection is now healthy")
	// When the breaker was first created, we tripped it but disconnected will
	// have been zero, so don't log a bogus duration in that case.
	if !disconnected.IsZero() {
		_, _ = redact.Fprintf(&buf, " (after %s)", now.Sub(disconnected).Round(time.Second))
	}
	log.Health.InfofDepth(ctx, 1, "%s", buf)
}
