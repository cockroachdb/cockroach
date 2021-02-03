// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// IdleMonitor monitors the active connections and calls onIdle() when
// there are no more active connections.
// It will stays dormant initially for the warmup duration interval.
// Once it detects that there are no more active connections, it will
// start a countdown timer and call onIdle() once the countdown timer expires.
// Any new connections before the times expires will stop the timer and start
// the active connection monitoring again.
type IdleMonitor struct {
	mu                    syncutil.Mutex
	onIdle                func()
	activated             bool
	activeConnectionCount uint32
	totalConnectionCount  uint32
	countdownTimer        *time.Timer
	shutdownInitiated     bool
	countdownDuration     time.Duration
}

// defaultCountdownDuration specifies the time, the monitor will wait, after
// the number of active connections drops to zero, before the onIdle is
// called.
// If this is changed, cli/cliflags/flags.go should be updated as
// well to reflect the new value.
const defaultCountdownDuration = 30 * time.Second

// MakeIdleMonitor creates a new IdleMonitor.
func MakeIdleMonitor(
	ctx context.Context,
	warmupDuration time.Duration,
	onIdle func(),
	countdownDuration ...time.Duration,
) *IdleMonitor {
	monitor := &IdleMonitor{onIdle: onIdle}

	if len(countdownDuration) > 0 {
		monitor.countdownDuration = countdownDuration[0]
	} else {
		// Default if there isn't an overwrite
		monitor.countdownDuration = defaultCountdownDuration
	}
	log.VEventf(ctx, 2,
		"create with warmup %s and countdown %s durations", warmupDuration,
		monitor.countdownDuration,
	)
	// Activate the monitor after the warmup duration is over and trigger the
	// countdown timer if the number of active connections is zero.
	time.AfterFunc(warmupDuration, func() {
		log.VEventf(ctx, 2, "warmup duration is over")
		monitor.mu.Lock()
		defer monitor.mu.Unlock()
		monitor.activated = true
		if monitor.activeConnectionCount == 0 {
			monitor.countdownTimer = time.AfterFunc(monitor.countdownDuration, func() {
				log.VEventf(ctx, 2, "firing because no "+
					"connection ever occurred and it has been %s after warmup",
					monitor.countdownDuration,
				)
				onIdle()
			})
		}
	})

	return monitor
}

// NewConnection registers a new connection and if there is a shutdown
// timer running - stops the time. It returns false to indicate that there is
// no shutdown in progress and true when the shutdown already started.
func (i *IdleMonitor) NewConnection(ctx context.Context) bool {
	log.VEventf(ctx, 3, "new connection")
	i.mu.Lock()
	defer i.mu.Unlock()

	// If there is countdown timer - stop it.
	if i.countdownTimer != nil {
		log.VEventf(ctx, 3, "countdown timer found - stopping")
		i.shutdownInitiated = !i.countdownTimer.Stop()
		if i.shutdownInitiated {
			log.VEventf(ctx, 2, "shutdown already initiated")
			return true
		}
		log.VEventf(ctx, 3, "countdown timer stopped successfully")
		i.countdownTimer = nil
	}

	// Update connection counts
	i.activeConnectionCount++
	i.totalConnectionCount++
	log.VEventf(ctx, 3, "active connections %d and total connections %d",
		i.activeConnectionCount, i.totalConnectionCount)
	return false
}

// CloseConnection is called when a connection terminates. It will update the
// connection counters and start a new shutdown timer if the number of
// active connections reaches zero and the warmup period is over.
func (i *IdleMonitor) CloseConnection(ctx context.Context) {
	log.VEventf(ctx, 3, "closed connection")
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.shutdownInitiated {
		log.VEventf(ctx, 2, "shutdown already initiated")
		return
	}
	i.activeConnectionCount--
	log.VEventf(ctx, 3, "post close connection active connection count %d",
		i.activeConnectionCount)
	if i.activeConnectionCount == 0 && i.activated {
		log.VEventf(ctx, 3,
			"zero active connections and warmup done - starting countdown timer")
		i.countdownTimer = time.AfterFunc(i.countdownDuration, func() {
			log.VEventf(ctx, 2, "firing because warmed up and no "+
				"active connections for %s", i.countdownDuration)
			i.onIdle()
		})
	}
}
