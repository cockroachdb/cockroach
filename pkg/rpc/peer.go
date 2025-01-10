// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rpc

import (
	"context"
	"fmt"
	"runtime/pprof"
	"time"

	"github.com/VividCortex/ewma"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	"storj.io/drpc/drpcpool"
)

type peerStatus int

const (
	peerStatusInactive = iota
	peerStatusHealthy
	peerStatusUnhealthy
	peerStatusDeleted
)

func (p *peer) setHealthyLocked() {
	if p.mu.peerStatus == peerStatusDeleted {
		return
	}
	p.ConnectionUnhealthyFor.Update(0)
	switch p.mu.peerStatus {
	case peerStatusUnhealthy:
		p.ConnectionHealthy.Inc(1)
		p.ConnectionUnhealthy.Dec(1)
	case peerStatusInactive:
		p.ConnectionHealthy.Inc(1)
		p.ConnectionInactive.Dec(1)
	}
	p.mu.peerStatus = peerStatusHealthy
}

func (p *peer) setUnhealthyLocked(connUnhealthyFor int64) {
	if p.mu.peerStatus == peerStatusDeleted {
		return
	}
	p.ConnectionHealthyFor.Update(0)
	p.ConnectionUnhealthyFor.Update(connUnhealthyFor)
	p.AvgRoundTripLatency.Update(0)

	switch p.mu.peerStatus {
	case peerStatusHealthy:
		p.ConnectionUnhealthy.Inc(1)
		p.ConnectionHealthy.Dec(1)
	case peerStatusInactive:
		p.ConnectionUnhealthy.Inc(1)
		p.ConnectionInactive.Dec(1)
	}
	p.mu.peerStatus = peerStatusUnhealthy
}

func (p *peer) setInactiveLocked() {
	if p.mu.peerStatus == peerStatusDeleted {
		return
	}
	p.ConnectionHealthyFor.Update(0)
	p.ConnectionUnhealthyFor.Update(0)
	p.AvgRoundTripLatency.Update(0)

	switch p.mu.peerStatus {
	case peerStatusHealthy:
		p.ConnectionInactive.Inc(1)
		p.ConnectionHealthy.Dec(1)
	case peerStatusUnhealthy:
		p.ConnectionInactive.Inc(1)
		p.ConnectionUnhealthy.Dec(1)
	}
	p.mu.peerStatus = peerStatusInactive
}

func (p *peer) releaseMetricsLocked() {
	if p.mu.peerStatus == peerStatusDeleted {
		return
	}
	// Always set to inactive before releasing.
	p.setInactiveLocked()
	p.ConnectionInactive.Dec(1)
	p.mu.peerStatus = peerStatusDeleted
}

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
	dialDRPC          func(ctx context.Context, target string) (drpcpool.Conn, error)
	// b maintains connection health. This breaker's async probe is always
	// active - it is the heartbeat loop and manages `mu.c.` (including
	// recreating it after the connection fails and has to be redialed).
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
		peerStatus peerStatus
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
func (rpcCtx *Context) newPeer(k peerKey, locality roachpb.Locality) *peer {
	// Initialization here is a bit circular. The peer holds the breaker. The
	// breaker probe references the peer because it needs to replace the one-shot
	// Connection when it makes a new connection in the probe. And (all but the
	// first incarnation of) the Connection also holds on to the breaker since the
	// Connect method needs to do the short-circuiting (if a Connection is created
	// while the breaker is tripped, we want to block in Connect only once we've
	// seen the first heartbeat succeed).
	pm, lm := rpcCtx.metrics.acquire(k, locality)
	p := &peer{
		peerMetrics:        pm,
		logDisconnectEvery: log.Every(time.Minute),
		k:                  k,
		remoteClocks:       rpcCtx.RemoteClocks,
		opts:               &rpcCtx.ContextOptions,
		peers:              &rpcCtx.peers,
		dial: func(ctx context.Context, target string, class ConnectionClass) (*grpc.ClientConn, error) {
			additionalDialOpts := []grpc.DialOption{grpc.WithStatsHandler(&statsTracker{lm})}
			additionalDialOpts = append(additionalDialOpts, rpcCtx.testingDialOpts...)
			return rpcCtx.grpcDialRaw(ctx, target, class, additionalDialOpts...)
		},
		dialDRPC:          dialDRPC(rpcCtx),
		heartbeatInterval: rpcCtx.RPCHeartbeatInterval,
		heartbeatTimeout:  rpcCtx.RPCHeartbeatTimeout,
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
	})
	p.b = b
	c := newConnectionToNodeID(p.opts, k, b.Signal)
	p.mu.PeerSnap = PeerSnap{c: c}

	return p
}

func (p *peer) breakerDisabled() bool {
	return !enableRPCCircuitBreakers.Get(&p.opts.Settings.SV)
}

// launch starts the probe in the background. The probe typically runs forever[1],
// and has the following high-level structure (dashes reflect call depth).
//
// - run: loops "forever", except when connection is pending deletion, see [1].
// -- runOnce: starts a new *ClientConn and maintains it until it errors out.
// --- runSingleHeartbeat: performs the first heartbeat.
// --- onInitialHeartbeatSucceeded: signals the conn future (*ClientConn now accessible).
// --- runHeartbeatUntilFailure: performs subsequent heartbeats until error occurs.
// ---- onSubsequentHeartbeatSucceeded: metric updates.
// - onHeartbeatFailed: state transition into failed state (breaker, logging, etc).
//
// [1]: see comment on `peer` for exceptions, and (*peer).shouldDeleteAfter for
// an entry point into the code. In brief, if an unhealthy peer is suspected of
// being obsolete, the probe only runs when the breaker is checked by a caller.
// After a generous timeout, the peer is removed if still unhealthy.
func (p *peer) launch(ctx context.Context, report func(error), done func()) {
	// Acquire mu just to show that we can, as the caller is supposed
	// to not hold the lock.
	p.mu.Lock()
	_ = 0 // bypass empty crit section lint
	p.mu.Unlock()

	taskName := fmt.Sprintf("conn to n%d@%s/%s", p.k.NodeID, p.k.TargetAddr, p.k.Class)

	log.VEventf(ctx, 1, "probe starting")
	if err := p.opts.Stopper.RunAsyncTask(ctx, taskName, func(ctx context.Context) {
		p.run(ctx, report, done)
	}); err != nil {
		p.onQuiesce(report)
		done()
	}
}

// run synchronously runs the probe.
//
// INVARIANT: p.mu.c is a "fresh" connection (i.e. unresolved connFuture)
// whenever `run` is invoked.
func (p *peer) run(ctx context.Context, report func(error), done func()) {
	var t timeutil.Timer
	defer t.Stop()
	defer done()
	defer log.VEventf(ctx, 1, "probe stopping")
	// Immediately run probe after breaker circuit is tripped, optimizing for the
	// case in which we can immediately reconnect.
	t.Reset(0)
	for {
		if p.snap().deleted {
			return
		}

		select {
		case <-ctx.Done():
			p.onQuiesce(report)
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
		p.maybeDelete(ctx, now)

		if errors.Is(err, errQuiescing) {
			// Heartbeat loop ended due to shutdown. Exit the probe, it won't be
			// started again since that means running an async task through the
			// Stopper.
			return
		}

		func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.mu.c = newConnectionToNodeID(p.opts, p.k, p.mu.c.breakerSignalFn)
		}()

		if p.snap().deleteAfter != 0 {
			// Peer is in inactive mode, and we just finished up a probe, so
			// end the probe. Another one will be started if anyone accesses
			// the breaker.
			return
		}
	}
}

func (p *peer) runOnce(ctx context.Context, report func(error)) error {
	cc, err := p.dial(ctx, p.k.TargetAddr, p.k.Class)
	if err != nil {
		return err
	}
	defer func() {
		_ = cc.Close() // nolint:grpcconnclose
	}()
	dc, err := p.dialDRPC(ctx, p.k.TargetAddr)
	if err != nil {
		return err
	}
	defer func() {
		_ = dc.Close()
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

	p.onInitialHeartbeatSucceeded(ctx, p.opts.Clock.Now(), cc, dc, report)

	return p.runHeartbeatUntilFailure(ctx, connFailedCh)
}

func runSingleHeartbeat(
	ctx context.Context,
	heartbeatClient HeartbeatClient,
	k peerKey,
	roundTripLatency ewma.MovingAverage,
	remoteClocks *RemoteClockMonitor, // nil if no RemoteClocks update should be made
	opts *ContextOptions,
	heartbeatTimeout time.Duration,
	preferredDialback PingRequest_DialbackType,
) error {
	if !opts.NeedsDialback || !useDialback.Get(&opts.Settings.SV) {
		preferredDialback = PingRequest_NONE
	}

	// Pick up any asynchronous update to clusterID and NodeID.
	clusterID := opts.StorageClusterID.Get()

	var lastOffset RemoteOffset
	if remoteClocks != nil {
		lastOffset = remoteClocks.GetOffset(k.NodeID)
	}

	// The request object. Note that we keep the same object from
	// heartbeat to heartbeat: we compute a new .Offset at the end of
	// the current heartbeat as input to the next one.
	request := &PingRequest{
		OriginAddr:      opts.AdvertiseAddr,
		TargetNodeID:    k.NodeID,
		ServerVersion:   opts.Settings.Version.LatestVersion(),
		LocalityAddress: opts.LocalityAddresses,
		ClusterID:       &clusterID,
		OriginNodeID:    opts.NodeID.Get(),
		NeedsDialback:   preferredDialback,
		Offset:          lastOffset,
	}

	interceptor := func(context.Context, *PingRequest) error { return nil }
	if fn := opts.OnOutgoingPing; fn != nil {
		interceptor = fn
	}

	var response *PingResponse
	sendTime := opts.Clock.Now()
	ping := func(ctx context.Context) error {
		if err := interceptor(ctx, request); err != nil {
			return err
		}
		var err error

		response, err = heartbeatClient.Ping(ctx, request)
		return err
	}
	var err error
	if heartbeatTimeout > 0 {
		err = timeutil.RunWithTimeout(ctx, "conn heartbeat", heartbeatTimeout, ping)
	} else {
		err = ping(ctx)
	}

	if err != nil {
		log.VEventf(ctx, 2, "received error on ping response from n%d, %v", k.NodeID, err)
		return err
	}

	// We verify the cluster name on the initiator side (instead
	// of the heartbeat service side, as done for the cluster ID
	// and node ID checks) so that the operator who is starting a
	// new node in a cluster and mistakenly joins the wrong
	// cluster gets a chance to see the error message on their
	// management console.
	if !opts.DisableClusterNameVerification && !response.DisableClusterNameVerification {
		err = errors.Wrap(
			checkClusterName(opts.ClusterName, response.ClusterName),
			"cluster name check failed on ping response")
		if err != nil {
			return err
		}
	}

	err = checkVersion(ctx, opts.Settings.Version, response.ServerVersion)
	if err != nil {
		err := errors.Mark(err, VersionCompatError)
		return err
	}

	receiveTime := opts.Clock.Now()

	pingDuration, _, err := updateClockOffsetTracking(
		ctx, remoteClocks, k.NodeID,
		sendTime, timeutil.Unix(0, response.ServerTime), receiveTime,
		opts.ToleratedOffset,
	)
	if err != nil {
		if opts.FatalOnOffsetViolation {
			log.Ops.Fatalf(ctx, "%v", err)
		}
	} else {
		roundTripLatency.Add(float64(pingDuration.Nanoseconds())) // source for metrics
	}

	return nil
}

// runHeartbeatUntilFailure synchronously runs the heartbeat loop for the given
// RPC connection, returning once a heartbeat fails. The ctx passed as argument
// must be derived from rpcCtx.masterCtx, so that it respects the same
// cancellation policy.
func (p *peer) runHeartbeatUntilFailure(
	ctx context.Context, connFailedCh <-chan connectivity.State,
) error {
	var heartbeatTimer timeutil.Timer
	defer heartbeatTimer.Stop()
	// NB: the caller just sent the initial heartbeat, so we don't
	// queue for an immedidate heartbeat but wait out the interval.
	heartbeatTimer.Reset(p.heartbeatInterval)

	// If we get here, we know `connFuture` has been resolved (due to the
	// initial heartbeat having succeeded), so we have a Conn() we can
	// use.
	heartbeatClient := NewHeartbeatClient(p.snap().c.connFuture.Conn())

	for {
		select {
		case <-ctx.Done():
			return ctx.Err() // likely server shutdown
		case <-heartbeatTimer.C:
			heartbeatTimer.Read = true
		case <-connFailedCh:
			// gRPC has signaled that the connection is now failed, which implies that
			// we will need to start a new connection (since we set things up that way
			// using onlyOnceDialer). But we go through the motions and run the
			// heartbeat so that there is a unified path that reports the error,
			// in order to provide a good UX.
		}

		if err := runSingleHeartbeat(
			ctx, heartbeatClient, p.k, p.peerMetrics.roundTripLatency, p.remoteClocks,
			p.opts, p.heartbeatTimeout, PingRequest_NON_BLOCKING,
		); err != nil {
			return err
		}

		p.onSubsequentHeartbeatSucceeded(ctx, p.opts.Clock.Now())
		heartbeatTimer.Reset(p.heartbeatInterval)
	}
}

func logOnHealthy(ctx context.Context, disconnected, now time.Time) {
	var buf redact.StringBuilder
	buf.SafeString("connection is now healthy")
	// When the breaker was first created, we tripped it but disconnected will
	// have been zero, so don't log a bogus duration in that case.
	if !disconnected.IsZero() {
		buf.Printf(" (after %s)", redact.Safe(now.Sub(disconnected).Round(time.Second)))
	}
	log.Health.InfofDepth(ctx, 1, "%s", buf)
}

func (p *peer) onInitialHeartbeatSucceeded(
	ctx context.Context, now time.Time, cc *grpc.ClientConn, dc drpcpool.Conn, report func(err error),
) {
	// First heartbeat succeeded. By convention we update the breaker
	// before updating the peer. The other way is fine too, just the
	// tests need to be written a certain way to avoid being flaky.
	report(nil)

	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.connected = now
	// If the probe was inactive, the fact that we managed to heartbeat implies
	// that it ought not have been.
	p.mu.deleteAfter = 0
	p.setHealthyLocked()

	// Counter updates.
	p.ConnectionHeartbeats.Inc(1)
	// ConnectionFailures is not updated here.

	// Bind the connection's stream pool to the active gRPC connection. Do this
	// ahead of signaling the connFuture, so that the stream pool is ready for use
	// by the time the connFuture is resolved.
	p.mu.c.batchStreamPool.Bind(ctx, cc)
	p.mu.c.drpcBatchStreamPool.Bind(ctx, dc)

	// Close the channel last which is helpful for unit tests that
	// first waitOrDefault for a healthy conn to then check metrics.
	p.mu.c.connFuture.Resolve(cc, dc, nil /* err */)

	logOnHealthy(ctx, p.mu.disconnected, now)
}

func (p *peer) onSubsequentHeartbeatSucceeded(_ context.Context, now time.Time) {
	// Gauge updates.
	// ConnectionHealthy is already one.
	// ConnectionUnhealthy is already zero.
	p.ConnectionHealthyFor.Update(now.Sub(p.snap().connected).Nanoseconds() + 1) // add 1ns for unit tests w/ manual clock
	// ConnectionInactive is already zero.
	// ConnectionUnhealthyFor is already zero.
	p.AvgRoundTripLatency.Update(int64(p.roundTripLatency.Value()) + 1) // add 1ns for unit tests w/ manual clock

	// Counter updates.
	p.ConnectionHeartbeats.Inc(1)
	// ConnectionFailures is not updated here.
}

func maybeLogOnFailedHeartbeat(
	ctx context.Context,
	now time.Time,
	err, prevErr error,
	snap PeerSnap, // already accounting for `err`
	every *log.EveryN,
) {
	if errors.Is(err, errQuiescing) {
		return
	}
	// If the error is wrapped in InitialHeartbeatFailedError, unwrap it because that
	// error is noise in the logging we're doing here.
	if ihb := (*netutil.InitialHeartbeatFailedError)(nil); errors.As(err, &ihb) {
		err = ihb.WrappedErr
	}
	if prevErr == nil && !snap.connected.IsZero() {
		// If we just disconnected now after having been healthy, log without rate
		// limiting.

		logErr := err
		if errors.Is(logErr, grpcutil.ErrConnectionInterrupted) {
			//
			// We use onlyOnceDialer to prevent gRPC from redialing internally,
			// but this means that whenever the connection drops, we get a gRPC
			// error that comes from it internally trying to redial and hitting
			// the onlyOnceDialer (which refuses with ErrConnectionInterrupted).
			// The only actionable information in that is
			// ErrConnectionInterrupted; the true reason for the disconnect is
			// sadly not available to us (even gRPC logging doesn't contain it
			// as far as I can tell!), so the best we can do is be succinct.
			//
			// We'll basically always hit this path outside of tests when the
			// connection breaks (but not when the remote node version
			// mismatches, gets decommissioned, etc).
			logErr = grpcutil.ErrConnectionInterrupted
		}
		log.Health.Errorf(ctx, "disconnected (was healthy for %s): %v",
			now.Sub(snap.connected).Round(time.Millisecond), logErr,
		)
	} else {
		// Logging on each failed reconnection is quite noisy and often doesn't
		// add anything. So log only on first error, on code change, when V(1)
		// is set, or every ~minute. Errors not originating from gRPC are always
		// logged.
		prevStatus, havePrev := status.FromError(errors.UnwrapAll(prevErr))
		curStatus, _ := status.FromError(errors.UnwrapAll(err))
		if shouldLog := !havePrev ||
			curStatus.Code() != prevStatus.Code() ||
			every.ShouldLog() ||
			log.V(1); shouldLog {
			var buf redact.StringBuilder
			buf.SafeString("failed connection attempt")
			if !snap.disconnected.IsZero() {
				buf.Printf(" (last connected %s ago)", redact.Safe(now.Sub(snap.disconnected).Round(time.Millisecond)))
			} else {
				buf.SafeString(" (never connected)")
			}
			log.Health.Errorf(ctx, "%v: %v", buf, err)
		}
	}
}

func (p *peer) onHeartbeatFailed(
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
	if ts := p.peers.shouldDeleteAfter(p.k, err); ts != 0 {
		deleteAfter = ts
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	ls := &p.mu.PeerSnap // "locked snap"

	if !ls.c.connFuture.Resolved() {
		// If the initial heartbeat failed (or we got an error creating the
		// *grpc.ClientConn), wrap the error. More importantly, resolve connFuture;
		// someone might be waiting on it in ConnectNoBreaker who is not paying
		// attention to the circuit breaker.
		err = &netutil.InitialHeartbeatFailedError{WrappedErr: err}
		ls.c.connFuture.Resolve(nil /* cc */, nil /* dc */, err)
	}

	// Close down the stream pool that was bound to this connection.
	ls.c.batchStreamPool.Close()

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

	// Only update the unhealthy duration if it is considered unhealthy.
	if ls.deleteAfter == 0 {
		connUnhealthyFor := now.Sub(ls.disconnected).Nanoseconds() + 1 // 1ns for unit tests w/ manual clock
		p.setUnhealthyLocked(connUnhealthyFor)
	} else {
		p.setInactiveLocked()
	}
	// Counter updates.
	p.ConnectionFailures.Inc(1)
}

// onQuiesce is called when the probe exits or refuses to start due to
// quiescing.
func (p *peer) onQuiesce(report func(error)) {
	// Stopper quiescing, node shutting down.
	report(errQuiescing)
	// NB: it's important that connFuture is resolved, or a caller sitting on
	// `c.ConnectNoBreaker` would never be unblocked; after all, the probe won't
	// start again in the future.
	p.snap().c.connFuture.Resolve(nil, nil, errQuiescing)
}

func (p PeerSnap) deletable(now time.Time) bool {
	if p.deleteAfter == 0 {
		return false
	}
	ts := p.disconnected.Add(p.deleteAfter)
	return now.After(ts)
}

// hasSiblingConn takes a peer as identified by `self` and tries to find a sibling:
// - if self.NodeID != 0, finds another conn with matching NodeID but different TargetAddr.
// - if self.NodeID == 0, finds another conn with matching TargetAddr.
//
// In both cases, if such a conn exists that became healthy *after* ours became
// unhealthy, `healthy` will be true. If no such conn exists, (false, false) is
// returned.
func hasSiblingConn(peers map[peerKey]*peer, self peerKey) (healthy, ok bool) {
	for other, otherPeer := range peers {
		if self == other {
			continue // exclude self
		}
		// NB: we're careful not to call snap() on self because it might be locked
		// already.
		otherSigCh := otherPeer.b.Signal().C()

		if self.NodeID == 0 {
			if other.TargetAddr != self.TargetAddr {
				continue
			}
			// We're a GRPCUnvalidatedDial, which has a tricky life cycle because no
			// NodeID is associated. We can't detect an IP address change in the same
			// way we can for the NodeID!=0 branch below, nor do we get an event
			// telling is we're decommissioned.
			//
			// We do the simple thing: if there's any another connection to the same
			// address (no matter who) and it's healthy, report that. The caller can
			// figure out what to do with that information.
		} else {
			if self.NodeID != other.NodeID || self.TargetAddr == other.TargetAddr {
				continue
			}
			// We're a validated (i.e. with NodeID) connection and found another
			// healthy peer matching our NodeID but not our address. Node <NodeID> has
			// restarted under a new IP! We are likely obsolete.
			//
			// Note that if "we" are actually the newer connection (hypothetically
			// this could happen if the other connection is "still" healthy, and we
			// are "temporarily" unhealthy) then we'll remove the wrong connection,
			// but there will be a reconnection attempt (recreating the myKey peer),
			// so while causing a hiccup it wouldn't wedge anything.
		}
		ok = true
		select {
		// We don't just check `c.Health` because that will trigger
		// the probe. If otherSnap belongs to an inactive peer, we
		// don't want to randomly do that all of the time; only
		// a direct access to the peer by a client should start
		// the probe. Checking that the breaker channel is open
		// accomplishes that.
		case <-otherSigCh:
		default:
			healthy = true
		}
	}
	return healthy, ok
}

func (peers *peerMap) shouldDeleteAfter(myKey peerKey, err error) time.Duration {
	peers.mu.RLock()
	defer peers.mu.RUnlock()

	sibHealthy, ok := hasSiblingConn(peers.mu.m, myKey)

	var deleteAfter time.Duration
	if kvpb.IsDecommissionedStatusErr(err) {
		deleteAfter = 24 * time.Hour
	} else if myKey.NodeID != 0 && ok && sibHealthy {
		// We're a NodeID-keyed conn and found another conn to the NodeID that is
		// healthy, meaning our TargetAddr is stale. This would be common in k8s
		// deployments without stable IPs, where a node would receive a new address
		// after each restart. In such a setting, it's usually expected that nobody
		// tries to connect under the old address any more after a few minutes, so
		// delete more aggressively.
		deleteAfter = 5 * time.Minute
	} else if myKey.NodeID == 0 {
		if !ok || !sibHealthy {
			// If we're an unvalidated connection, if we have a healthy sibling we
			// assume our connection is still relevant. However, if we *don't* have a
			// sibling, what are we to do? We may or may no longer be relevant. Stick
			// around for up to 24h, then get deleted, so we avoid log spam (due to
			// frequent recreation of the peer) but also we don't leak. If we have a
			// sibling but it's not healthy, giving it a 24h grace period is also
			// fine; this simplifies the logic because our unhealthy sibling might
			// have detected that the IP has moved, but we don't want to have to peek
			// into its state too much; we'll spend the next 24h testing the
			// connection only when it's accessed, and then remove ourselves.
			deleteAfter = 24 * time.Hour
		}
	}

	return deleteAfter
}

func touchOldPeers(peers *peerMap, now time.Time) {
	sigs := func() (sigs []circuit.Signal) {
		peers.mu.RLock()
		defer peers.mu.RUnlock()
		for _, p := range peers.mu.m {
			if p.snap().deletable(now) {
				sigs = append(sigs, p.b.Signal())
			}
		}
		return sigs
	}()

	// Now, outside of the lock, query all of the collected Signals which will tip
	// off the respective probes, which will perform self-removal from the map. To
	// simplify logic, we only allow a peer's probe itself to remove the peer.
	// We could do this under peers.mu today but it seems better not to, since
	// there is always a chance that the breaker's `AsyncProbe` will one day be
	// update to acquire `peers.mu` as well, which would cause locking issues.
	for _, sig := range sigs {
		// NB: we don't assert that Err() != nil because (at least in theory)
		// the connection may have become healthy again in some scenarios.
		_ = sig.Err()
	}
}

func (p *peer) maybeDelete(ctx context.Context, now time.Time) {
	// If the peer can be deleted, delete it now.
	//
	// Also delete unconditionally if circuit breakers are (now) disabled. We want
	// to allow that setting to bypass an as wide as possible class of issues, so
	// we completely yank the peer out of the map.
	snap := p.snap()

	if snap.deleted {
		log.VEventf(ctx, 2, "peer already deleted")
		return
	}

	if !p.breakerDisabled() && !snap.deletable(now) {
		return
	}

	log.VEventf(ctx, 1, "deleting peer")

	// Lock order: map, then peer. We need to lock both because we want
	// to atomically release the metrics while removing from the map[1][2].
	//
	// [1]: see https://github.com/cockroachdb/cockroach/issues/105335
	// [2]: Releasing in one critical section with p.deleted=true ensures
	//      that the metrics are not updated after release, which would
	//      otherwise cause the aggregate metrics to drift away from zero
	//      permanently.
	p.peers.mu.Lock()
	defer p.peers.mu.Unlock()
	delete(p.peers.mu.m, p.k)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.deleted = true
	p.releaseMetricsLocked()
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
