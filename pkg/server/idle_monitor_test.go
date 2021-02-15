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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

var (
	warmupDuration    = 600 * time.Millisecond
	countdownDuration = 1200 * time.Millisecond
	delta             = 150 * time.Millisecond
)

func TestMakeIdleMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderShort(t)

	ctx := context.Background()

	start := timeutil.Now()

	var handlerCalled syncutil.AtomicBool
	monitor := MakeIdleMonitor(ctx, warmupDuration, func() {
		// The handler is called after the warmup and countdown durations
		require.WithinDuration(t,
			start.Add(warmupDuration+countdownDuration+delta), timeutil.Now(), delta,
		)
		handlerCalled.Set(true)
	}, countdownDuration)

	require.EqualValues(t, countdownDuration, monitor.countdownDuration)

	// activated is set after the warmup duration is over
	time.Sleep(warmupDuration - delta)
	monitor.mu.Lock()
	require.False(t, monitor.activated)
	require.Nil(t, monitor.countdownTimer)
	monitor.mu.Unlock()

	time.Sleep(2 * delta)

	monitor.mu.Lock()
	require.True(t, monitor.activated)
	// Countdown timer triggers as there is no connection.
	require.NotNil(t, monitor.countdownTimer)
	monitor.mu.Unlock()

	time.Sleep(countdownDuration + 2*delta)

	monitor.mu.Lock()
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	require.EqualValues(t, 0, monitor.totalConnectionCount)
	monitor.mu.Unlock()

	require.True(t, handlerCalled.Get())

	require.True(t, monitor.NewConnection(ctx))

	monitor.mu.Lock()
	require.True(t, monitor.shutdownInitiated)
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	require.EqualValues(t, 0, monitor.totalConnectionCount)
	monitor.mu.Unlock()
}

func TestMakeIdleMonitor_WithConnectionDuringWarmup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderShort(t)

	ctx := context.Background()

	start := timeutil.Now()

	var handlerCalled syncutil.AtomicBool
	monitor := MakeIdleMonitor(ctx, warmupDuration, func() {
		// The handler is called after the warmup and countdown durations
		require.WithinDuration(t,
			start.Add(warmupDuration+countdownDuration+delta), timeutil.Now(), delta,
		)
		handlerCalled.Set(true)
	}, countdownDuration)

	time.AfterFunc(warmupDuration/2, func() {
		require.False(t, monitor.NewConnection(ctx))
	})

	require.EqualValues(t, countdownDuration, monitor.countdownDuration)

	// activated is set after the warmup duration is over
	time.Sleep(warmupDuration - delta)

	monitor.mu.Lock()
	require.False(t, monitor.activated)
	require.Nil(t, monitor.countdownTimer)
	monitor.mu.Unlock()

	time.Sleep(2 * delta)

	monitor.mu.Lock()
	require.True(t, monitor.activated)
	// Countdown timer does not trigger as there is a connection.
	require.Nil(t, monitor.countdownTimer)
	monitor.mu.Unlock()

	time.Sleep(countdownDuration + 2*delta)

	monitor.mu.Lock()
	require.EqualValues(t, 1, monitor.activeConnectionCount)
	require.EqualValues(t, 1, monitor.totalConnectionCount)
	monitor.mu.Unlock()

	require.False(t, handlerCalled.Get())

	require.False(t, monitor.NewConnection(ctx))

	monitor.mu.Lock()
	require.False(t, monitor.shutdownInitiated)
	require.EqualValues(t, 2, monitor.activeConnectionCount)
	require.EqualValues(t, 2, monitor.totalConnectionCount)
	monitor.mu.Unlock()
}

func TestMakeIdleMonitor_WithBriefConnectionDuringWarmup(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderShort(t)

	ctx := context.Background()

	start := timeutil.Now()

	var handlerCalled syncutil.AtomicBool
	monitor := MakeIdleMonitor(ctx, warmupDuration, func() {
		// The handler is called after the warmup and countdown durations
		require.WithinDuration(t,
			start.Add(warmupDuration+countdownDuration+delta), timeutil.Now(), delta,
		)
		handlerCalled.Set(true)
	}, countdownDuration)

	require.EqualValues(t, countdownDuration, monitor.countdownDuration)

	time.AfterFunc(warmupDuration/3, func() {
		require.False(t, monitor.NewConnection(ctx))
	})
	time.AfterFunc(warmupDuration*2/3, func() {
		monitor.CloseConnection(ctx)
	})

	// activated is set after the warmup duration is over
	time.Sleep(warmupDuration - delta)

	monitor.mu.Lock()
	require.False(t, monitor.activated)
	require.Nil(t, monitor.countdownTimer)
	monitor.mu.Unlock()

	time.Sleep(2 * delta)

	monitor.mu.Lock()
	require.True(t, monitor.activated)
	// Countdown timer triggers as there is no connection.
	require.NotNil(t, monitor.countdownTimer)
	monitor.mu.Unlock()

	time.Sleep(countdownDuration + 2*delta)

	monitor.mu.Lock()
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	require.EqualValues(t, 1, monitor.totalConnectionCount)
	monitor.mu.Unlock()

	require.True(t, handlerCalled.Get())

	require.True(t, monitor.NewConnection(ctx))

	monitor.mu.Lock()
	require.True(t, monitor.shutdownInitiated)
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	require.EqualValues(t, 1, monitor.totalConnectionCount)
	monitor.mu.Unlock()
}

func TestMakeIdleMonitor_WithBriefConnectionDuringCountdown(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderShort(t)

	ctx := context.Background()

	start := timeutil.Now()

	var handlerCalled syncutil.AtomicBool
	monitor := MakeIdleMonitor(ctx, warmupDuration, func() {
		// The handler is called after the warmup and countdown durations
		require.WithinDuration(t,
			start.Add(warmupDuration+countdownDuration*5/3+delta), timeutil.Now(), delta,
		)
		handlerCalled.Set(true)
	}, countdownDuration)

	require.EqualValues(t, countdownDuration, monitor.countdownDuration)

	time.AfterFunc(warmupDuration+countdownDuration/3, func() {
		require.False(t, monitor.NewConnection(ctx))
	})
	time.AfterFunc(warmupDuration+countdownDuration*2/3, func() {
		monitor.CloseConnection(ctx)
	})

	// activated is set after the warmup duration is over
	time.Sleep(warmupDuration - delta)

	monitor.mu.Lock()
	require.False(t, monitor.activated)
	require.Nil(t, monitor.countdownTimer)
	monitor.mu.Unlock()

	time.Sleep(2 * delta)

	monitor.mu.Lock()
	require.True(t, monitor.activated)
	// Countdown timer triggers as there is no connection.
	require.NotNil(t, monitor.countdownTimer)
	monitor.mu.Unlock()

	time.Sleep(countdownDuration + 2*delta)

	monitor.mu.Lock()
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	require.EqualValues(t, 1, monitor.totalConnectionCount)
	monitor.mu.Unlock()

	require.False(t, handlerCalled.Get())

	time.Sleep(warmupDuration + countdownDuration*5/3 + 3*delta)

	require.True(t, handlerCalled.Get())

	require.True(t, monitor.NewConnection(ctx))

	monitor.mu.Lock()
	require.True(t, monitor.shutdownInitiated)
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	require.EqualValues(t, 1, monitor.totalConnectionCount)
	monitor.mu.Unlock()
}
