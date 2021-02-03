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
		monitor.countdownDuration = 30 * time.Second
	}
	log.VEventf(ctx, 2,
		"Create with warmup %s and countdown %s durations", warmupDuration,
		monitor.countdownDuration,
	)
	// Activate the monitor after the warmup duration is over and trigger the
	// countdown timer if the number of active connections is zero.
	time.AfterFunc(warmupDuration, func() {
		log.VEventf(ctx, 2, "Warmup duration is over")
		monitor.mu.Lock()
		defer monitor.mu.Unlock()
		monitor.activated = true
		if monitor.activeConnectionCount == 0 {
			monitor.countdownTimer = time.AfterFunc(monitor.countdownDuration, func() {
				log.VEventf(ctx, 2, "Firing because no "+
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
	log.VEventf(ctx, 3, "New connection")
	i.mu.Lock()
	defer i.mu.Unlock()

	// If there is countdown timer - stop it.
	if i.countdownTimer != nil {
		log.VEventf(ctx, 3, "Countdown timer found - stopping")
		i.shutdownInitiated = !i.countdownTimer.Stop()
		if i.shutdownInitiated {
			log.VEventf(ctx, 2, "Shutdown already initiated")
			return true
		}
		log.VEventf(ctx, 3, "Countdown timer stopped successfully")
		i.countdownTimer = nil
	}

	// Update connection counts
	i.activeConnectionCount++
	i.totalConnectionCount++
	log.VEventf(ctx, 3, "Active connections %d and total connections %d",
		i.activeConnectionCount, i.totalConnectionCount)
	return false
}

// CloseConnection is called when a connection terminates. It will update the
// connection counters and start a new shutdown timer if the number of
// active connections reaches zero and the warmup period is over.
func (i *IdleMonitor) CloseConnection(ctx context.Context) {
	log.VEventf(ctx, 3, "Closed connection")
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.shutdownInitiated {
		log.VEventf(ctx, 2, "Shutdown already initiated")
		return
	}
	i.activeConnectionCount--
	log.VEventf(ctx, 3, "Post close connection active connection count %d",
		i.activeConnectionCount)
	if i.activeConnectionCount == 0 && i.activated {
		log.VEventf(ctx, 3,
			"Zero active connections and warmup done - starting countdown timer")
		i.countdownTimer = time.AfterFunc(i.countdownDuration, func() {
			log.VEventf(ctx, 2, "Firing because warmed up and no "+
				"active connections for %s", i.countdownDuration)
			i.onIdle()
		})
	}
}
