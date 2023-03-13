// Copyright 2016 The Cockroach Authors.
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
	"fmt"
	"time"

	"github.com/cenkalti/backoff"
	circuit "github.com/cockroachdb/circuitbreaker"
	circuitbreaker "github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/facebookgo/clock"
)

const rpcBreakerMaxBackoff = time.Second

// breakerClock is an implementation of clock.Clock that internally uses an
// hlc.WallClock. It is used to adapt the WallClock to the circuit breaker
// clocks. Note that it only implements the After() and Now() methods needed by
// circuit breakers and backoffs.
type breakerClock struct {
	clock hlc.WallClock
}

var _ clock.Clock = &breakerClock{}

func (c *breakerClock) After(d time.Duration) <-chan time.Time {
	// TODO(andrei): This is broken, in that the returned timer has nothing to do
	// with c.clock. Fix this once hlc.HybridManualClock implements
	// timeutil.TimeSource, which will allow the breakerClock to wrap a
	// timeutil.TimeSource, and not a
	return time.After(d)
}

func (c *breakerClock) AfterFunc(d time.Duration, f func()) *clock.Timer {
	panic("unimplemented")
}

func (c *breakerClock) Now() time.Time {
	return c.clock.Now()
}

func (c *breakerClock) Sleep(d time.Duration) {
	panic("unimplemented")
}

func (c *breakerClock) Tick(d time.Duration) <-chan time.Time {
	panic("unimplemented")
}

func (c *breakerClock) Ticker(d time.Duration) *clock.Ticker {
	panic("unimplemented")
}

func (c *breakerClock) Timer(d time.Duration) *clock.Timer {
	panic("unimplemented")
}

// newBackOff creates a new exponential backoff properly configured for RPC
// connection backoff.
func newBackOff(clock backoff.Clock) backoff.BackOff {
	// This exponential backoff limits the circuit breaker to 1 second
	// intervals between successive attempts to resolve a node address
	// and connect via GRPC.
	//
	// NB (nota Ben): MaxInterval should be less than the Raft election timeout
	// (1.5s) to avoid disruptions. A newly restarted node will be in follower
	// mode with no knowledge of the Raft leader. If it doesn't hear from a
	// leader before the election timeout expires, it will start to campaign,
	// which can be disruptive. Therefore the leader needs to get in touch (via
	// Raft heartbeats) with such nodes within one election timeout of their
	// restart, which won't happen if their backoff is too high.
	b := &backoff.ExponentialBackOff{
		InitialInterval:     500 * time.Millisecond,
		RandomizationFactor: 0.5,
		Multiplier:          1.5,
		MaxInterval:         rpcBreakerMaxBackoff,
		MaxElapsedTime:      0,
		Clock:               clock,
	}
	b.Reset()
	return b
}

func newBreaker(ctx context.Context, name string, clock clock.Clock) *circuit.Breaker {
	return circuit.NewBreakerWithOptions(&circuit.Options{
		Name:       name,
		BackOff:    newBackOff(clock),
		Clock:      clock,
		ShouldTrip: circuit.ThresholdTripFunc(1),
		Logger:     breakerLogger{ctx},
	})
}

// breakerLogger implements circuit.Logger to expose logging from the
// circuitbreaker package. Debugf is logged with a vmodule level of 2 so to see
// the circuitbreaker debug messages set --vmodule=breaker=2
type breakerLogger struct {
	ctx context.Context
}

func (r breakerLogger) Debugf(format string, v ...interface{}) {
	if log.V(2) {
		log.Dev.InfofDepth(r.ctx, 1, format, v...)
	}
}

func (r breakerLogger) Infof(format string, v ...interface{}) {
	log.Ops.InfofDepth(r.ctx, 1, format, v...)
}

// newPeerBreaker returns circuit breaker that trips when connection (associated
// with provided connKey) is failed. The breaker's probe *is* the heartbeat loop
// and is thus running at all times. The exception is a decommissioned node, for
// which the probe simply exits (any future connection attempts to the same peer
// will trigger the probe but the probe will exit again).
func (rpcCtx *Context) newPeerBreaker(k connKey) *circuitbreaker.Breaker {
	ctx := rpcCtx.makeDialCtx(k.targetAddr, k.nodeID, k.class)
	breaker := circuitbreaker.NewBreaker(circuitbreaker.Options{
		Name: "breaker", // log tags already represent `k`
		AsyncProbe: func(report func(error), done func()) {
			if err := rpcCtx.Stopper.RunAsyncTask(ctx, fmt.Sprintf("conn to n%d@%s/%s", k.nodeID, k.targetAddr, k.class), func(ctx context.Context) {
				var t timeutil.Timer
				defer t.Stop()
				defer done()
				// Immediately run probe after breaker circuit is tripped, optimizing for the
				// case in which we can immediately reconnect.
				t.Reset(0)
				for {
					select {
					case <-rpcCtx.Stopper.ShouldQuiesce():
						return
					case <-t.C:
						t.Read = true
						// TODO(XXX): why divide by two?
						t.Reset(rpcCtx.heartbeatInterval / 2)
					}

					// INVARIANT: we have a peer here and we're the only one heartbeating
					// the peer (guaranteed by the breaker, since we're its probe).

					var decommissioned bool
					var conn *Connection
					rpcCtx.m.inspectPeer(k, func(k connKey, p *peer) {
						conn = p.c
						decommissioned = p.decommissioned
					})

					if decommissioned {
						// For decommissioned node, don't attempt to maintain
						// connection or change error, simply remain in the
						// same state.
						return
					}

					err := rpcCtx.runHeartbeat(ctx, conn, k, func() {
						rpcCtx.m.withPeer(k, func(k connKey, p *peer) {
							p.disconnected = time.Time{}
						})
						report(nil) // successful heartbeat
					})

					if errors.Is(err, errMarkDecommissioned) {
						rpcCtx.m.withPeer(k, func(k connKey, p *peer) {
							p.decommissioned = true
						})
						// Continue: the next loop will handle the decommission status.
					}

					// Heartbeat loop ended. Report the error and reset connection for next
					// attempt.
					if ctx.Err() != nil {
						// Heartbeat loop likely ended due to shutdown, so don't report a
						// spurious error and just exit.
						return
					}
					report(err)
					// Make a new connection and loop around for next attempt. While we wait
					// to reconnect, callers will receive this new connection and block on
					// it, meaning we avoid busy loops.
					rpcCtx.m.withPeer(k, func(k connKey, p *peer) {
						p.c = newConnectionToNodeID(k.nodeID, k.class)
						if p.disconnected.IsZero() {
							p.disconnected = timeutil.Now()
						}
					})
					// Either way, loop around.
				}
			}); err != nil {
				// Stopper draining. We need to terminate the connection.
				report(err)
				rpcCtx.m.withPeer(k, func(k connKey, p *peer) {
					p.c.err.Store(errQuiescing)
					close(p.c.initialHeartbeatDone)
				})
				done()
			}
		},
		EventHandler: &circuitbreaker.EventLogger{
			Log: func(buf redact.StringBuilder) {
				log.Health.Infof(ctx, "%s", buf)
			},
		},
	})
	return breaker
}
