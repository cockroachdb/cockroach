// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

var (
	forwardClockJumpCheckEnabled = settings.RegisterBoolSetting(
		settings.ApplicationLevel,
		"server.clock.forward_jump_check_enabled",
		"if enabled, forward clock jumps > max_offset/2 will cause a panic",
		false,
		settings.WithName("server.clock.forward_jump_check.enabled"),
		settings.WithPublic)

	persistHLCUpperBoundInterval = settings.RegisterDurationSetting(
		settings.ApplicationLevel,
		"server.clock.persist_upper_bound_interval",
		"the interval between persisting the wall time upper bound of the clock. The clock "+
			"does not generate a wall time greater than the persisted timestamp and will panic if "+
			"it sees a wall time greater than this value. When cockroach starts, it waits for the "+
			"wall time to catch-up till this persisted timestamp. This guarantees monotonic wall "+
			"time across server restarts. Not setting this or setting a value of 0 disables this "+
			"feature.",
		0,
		settings.WithPublic)
)

// startMonitoringForwardClockJumps starts a background task to monitor forward
// clock jumps based on a cluster setting.
func (s *topLevelServer) startMonitoringForwardClockJumps(ctx context.Context) error {
	forwardJumpCheckEnabled := make(chan bool, 1)
	s.stopper.AddCloser(stop.CloserFn(func() { close(forwardJumpCheckEnabled) }))

	forwardClockJumpCheckEnabled.SetOnChange(&s.st.SV, func(context.Context) {
		forwardJumpCheckEnabled <- forwardClockJumpCheckEnabled.Get(&s.st.SV)
	})

	if err := s.clock.StartMonitoringForwardClockJumps(
		ctx,
		forwardJumpCheckEnabled,
		time.NewTicker,
		nil, /* tick callback */
	); err != nil {
		return errors.Wrap(err, "monitoring forward clock jumps")
	}

	log.Ops.Info(ctx, "monitoring forward clock jumps based on server.clock.forward_jump_check.enabled")
	return nil
}

// checkHLCUpperBoundExists determines whether there's an HLC
// upper bound that will need to refreshed/persisted after
// the server has initialized.
func (s *topLevelServer) checkHLCUpperBoundExistsAndEnsureMonotonicity(
	ctx context.Context, initialStart bool,
) (hlcUpperBoundExists bool, err error) {
	if initialStart {
		// Clock monotonicity checks can be skipped on server bootstrap
		// because the server has never
		// been used before.
		return false, nil
	}

	hlcUpperBound, err := kvserver.ReadMaxHLCUpperBound(ctx, s.engines)
	if err != nil {
		return false, errors.Wrap(err, "reading max HLC upper bound")
	}
	hlcUpperBoundExists = hlcUpperBound > 0

	// If the server is being restarted, sleep to ensure monotonicity of the HLC
	// clock.
	ensureClockMonotonicity(
		ctx,
		s.clock,
		s.startTime,
		hlcUpperBound,
		s.clock.SleepUntil,
	)

	return hlcUpperBoundExists, nil
}

// ensureClockMonotonicity sleeps till the wall time reaches
// prevHLCUpperBound. prevHLCUpperBound > 0 implies we need to guarantee HLC
// monotonicity across server restarts. prevHLCUpperBound is the last
// successfully persisted timestamp greater then any wall time used by the
// server.
//
// If prevHLCUpperBound is 0, the function sleeps up to max offset.
func ensureClockMonotonicity(
	ctx context.Context,
	clock *hlc.Clock,
	startTime time.Time,
	prevHLCUpperBound int64,
	sleepUntilFn func(context.Context, hlc.Timestamp) error,
) {
	var sleepUntil int64
	if prevHLCUpperBound != 0 {
		// Sleep until previous HLC upper bound to ensure wall time monotonicity
		sleepUntil = prevHLCUpperBound + 1
	} else {
		// Previous HLC Upper bound is not known
		// We might have to sleep a bit to protect against this node producing non-
		// monotonic timestamps. Before restarting, its clock might have been driven
		// by other nodes' fast clocks, but when we restarted, we lost all this
		// information. For example, a client might have written a value at a
		// timestamp that's in the future of the restarted node's clock, and if we
		// don't do something, the same client's read would not return the written
		// value. So, we wait up to MaxOffset; we couldn't have served timestamps more
		// than MaxOffset in the future (assuming that MaxOffset was not changed, see
		// #9733).
		//
		// As an optimization for tests, we don't sleep if all the stores are brand
		// new. In this case, the node will not serve anything anyway until it
		// synchronizes with other nodes.
		sleepUntil = startTime.UnixNano() + int64(clock.MaxOffset()) + 1
	}

	currentWallTime := clock.Now().WallTime
	delta := time.Duration(sleepUntil - currentWallTime)
	if delta > 0 {
		log.Ops.Infof(
			ctx,
			"Sleeping till wall time %v to catches up to %v to ensure monotonicity. Sub: %v",
			currentWallTime,
			sleepUntil,
			delta,
		)
		_ = sleepUntilFn(ctx, hlc.Timestamp{WallTime: sleepUntil})
	}
}

// periodicallyPersistHLCUpperBound periodically persists an upper bound of
// the HLC's wall time. The interval for persisting is read from
// persistHLCUpperBoundIntervalCh. An interval of 0 disables persisting.
//
// persistHLCUpperBoundFn is used to persist the hlc upper bound, and should
// return an error if the persist fails.
//
// tickerFn is used to create the ticker used for persisting
//
// tickCallback is called whenever a tick is processed
func periodicallyPersistHLCUpperBound(
	clock *hlc.Clock,
	persistHLCUpperBoundIntervalCh chan time.Duration,
	persistHLCUpperBoundFn func(int64) error,
	tickerFn func(d time.Duration) *time.Ticker,
	stopCh <-chan struct{},
	tickCallback func(),
) {
	// Create a ticker which can be used in selects.
	// This ticker is turned on / off based on persistHLCUpperBoundIntervalCh
	ticker := tickerFn(time.Hour)
	ticker.Stop()

	// persistInterval is the interval used for persisting the
	// an upper bound of the HLC
	var persistInterval time.Duration

	persistHLCUpperBound := func() {
		if err := clock.RefreshHLCUpperBound(
			persistHLCUpperBoundFn,
			int64(persistInterval*3), /* delta to compute upper bound */
		); err != nil {
			log.Ops.Fatalf(
				context.Background(),
				"error persisting HLC upper bound: %v",
				err,
			)
		}
	}

	for {
		select {
		case updatedPersistInterval := <-persistHLCUpperBoundIntervalCh:
			if updatedPersistInterval == persistInterval {
				// No change.
				continue
			}
			persistInterval = updatedPersistInterval

			ticker.Stop()
			if persistInterval > 0 {
				ticker = tickerFn(persistInterval)
				persistHLCUpperBound()
				log.Ops.Infof(context.Background(), "persisting HLC upper bound is enabled [every %.2fs]",
					persistInterval.Seconds())
			} else {
				if err := clock.ResetHLCUpperBound(persistHLCUpperBoundFn); err != nil {
					log.Ops.Fatalf(
						context.Background(),
						"error resetting hlc upper bound: %v",
						err,
					)
				}
				log.Ops.Info(context.Background(), "persisting HLC upper bound is disabled")
			}

		case <-ticker.C:
			if persistInterval > 0 {
				persistHLCUpperBound()
			}

		case <-stopCh:
			ticker.Stop()
			return
		}

		if tickCallback != nil {
			tickCallback()
		}
	}
}

// startPersistingHLCUpperBound starts a goroutine to persist an upper bound
// to the HLC.
//
// persistHLCUpperBoundFn is used to persist upper bound of the HLC, and should
// return an error if the persist fails
//
// tickerFn is used to create a new ticker
//
// tickCallback is called whenever persistHLCUpperBoundCh or a ticker tick is
// processed
func (s *topLevelServer) startPersistingHLCUpperBound(
	ctx context.Context, hlcUpperBoundExists bool,
) error {
	tickerFn := time.NewTicker
	persistHLCUpperBoundFn := func(t int64) error { /* function to persist upper bound of HLC to all stores */
		return s.node.SetHLCUpperBound(context.Background(), t)
	}
	persistHLCUpperBoundIntervalCh := make(chan time.Duration, 1)
	// Seed channel with initial update, then install a callback to update it
	// on future changes. SetOnChange does not automatically trigger the callback
	// if the setting is initially set.
	persistHLCUpperBoundIntervalCh <- persistHLCUpperBoundInterval.Get(&s.st.SV)
	persistHLCUpperBoundInterval.SetOnChange(&s.st.SV, func(context.Context) {
		persistHLCUpperBoundIntervalCh <- persistHLCUpperBoundInterval.Get(&s.st.SV)
	})

	if hlcUpperBoundExists {
		// The feature to persist upper bounds to wall times is enabled.
		// Persist a new upper bound to continue guaranteeing monotonicity
		// Going forward the goroutine launched below will take over persisting
		// the upper bound
		if err := s.clock.RefreshHLCUpperBound(
			persistHLCUpperBoundFn,
			int64(5*time.Second),
		); err != nil {
			return errors.Wrap(err, "refreshing HLC upper bound")
		}
	}

	_ = s.stopper.RunAsyncTask(
		ctx,
		"persist-hlc-upper-bound",
		func(context.Context) {
			periodicallyPersistHLCUpperBound(
				s.clock,
				persistHLCUpperBoundIntervalCh,
				persistHLCUpperBoundFn,
				tickerFn,
				s.stopper.ShouldQuiesce(),
				nil, /* tick callback */
			)
		},
	)
	return nil
}
