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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type breakerProbe struct {
	*peer
	k                        ConnKey
	heartbeatInterval        time.Duration
	stopper                  *stop.Stopper
	runHeartbeatUntilFailure func(ctx context.Context, conn *Connection, k ConnKey, healthy func(), nm NodeMetrics) error
}

func (p *breakerProbe) launch(ctx context.Context, report func(error), done func()) {
	// Acquire mu just to show that we can, as the caller is supposed
	// to not hold the lock.
	p.mu.Lock()
	_ = p // avoid empty critical section lint
	p.mu.Unlock()

	taskName := fmt.Sprintf("conn to n%d@%s/%s", p.k.NodeID, p.k.TargetAddr, p.k.Class)

	if err := p.stopper.RunAsyncTask(ctx, taskName, func(ctx context.Context) {
		runProbe(ctx, p, p.heartbeatInterval, report, done)
	}); err != nil {
		// Stopper draining. We need to terminate the connection.
		report(err)
		snap := p.snap() // NB: caller does not hold mu, see field comment
		select {
		case <-snap.c.initialHeartbeatDone:
		default:
			snap.c.err.Store(errQuiescing)
			close(snap.c.initialHeartbeatDone)
		}
		done()
	}
}

func runProbe(
	ctx context.Context,
	p *breakerProbe,
	heartbeatInterval time.Duration,
	report func(error),
	done func(),
) {
	var t timeutil.Timer
	defer t.Stop()
	defer done()
	// Immediately run probe after breaker circuit is tripped, optimizing for the
	// case in which we can immediately reconnect.
	t.Reset(0)
	for {
		// INVARIANT: we have a peer here, and we're the only one heartbeating
		// the peer (guaranteed by the breaker, since we're its probe).
		// This means we're allowed to close the channel without holding
		// the peer's lock.

		snap := p.snap()

		if snap.decommissionedOrSuperseded {
			// For decommissionedOrSuperseded node, don't attempt to maintain
			// connection or change error, simply remain in the
			// same state.
			return
		}

		select {
		case <-ctx.Done(): // stopper quiescing
			select {
			case <-snap.c.initialHeartbeatDone:
			default:
				snap.c.err.Store(errQuiescing)
				close(snap.c.initialHeartbeatDone)
			}
			return
		case <-t.C:
			t.Read = true
			t.Reset(heartbeatInterval)
		}

		err := p.runHeartbeatUntilFailure(ctx, snap.c, p.k, func() {
			report(nil)
		}, p.nm)

		if ctx.Err() != nil {
			// Heartbeat loop ended due to shutdown (or it erroring raced with the
			// shutdown so might as well blame the shutdown), so don't report a
			// spurious error and just exit.
			return
		}

		report(err)

		if kvpb.IsDecommissionedStatusErr(err) {
			p.mu.Lock()
			p.mu.decommissionedOrSuperseded = true
			p.mu.Unlock()

			// Leave the torn down connection in place and exit the probe.
			// The early return above will do the same for all future attempts
			// to connect, so we don't enter `runHeartbeatUntilFailure` on the next probe
			// attempt.
			return
		}

		// Make a new connection and loop around for next attempt.
		p.mu.Lock()
		p.mu.c = newConnectionToNodeID(p.k, p.b.Signal)
		if p.mu.disconnected.IsZero() {
			p.mu.disconnected = timeutil.Now()
		}
		p.mu.Unlock()
	}
}
