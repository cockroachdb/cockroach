package server

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestMakeIdleMonitor(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	start := time.Now()
	warmupDuration := 300 * time.Millisecond
	countdownDuration := 600 * time.Millisecond
	delta := 5 * time.Millisecond

	handlerCalled := false
	monitor := MakeIdleMonitor(ctx, warmupDuration, func() {
		// The handler is called after the warmup and countdown durations
		require.WithinDuration(t,
			start.Add(warmupDuration+countdownDuration+delta), time.Now(), delta,
		)
		handlerCalled = true
	}, countdownDuration)

	// activated is set after the warmup duration is over
	time.AfterFunc(warmupDuration-delta, func() {
		require.False(t, monitor.activated)
		require.Nil(t, monitor.countdownTimer)
	})
	time.AfterFunc(warmupDuration+delta, func() {
		require.True(t, monitor.activated)
		// Countdown timer triggers as there is no connection.
		require.NotNil(t, monitor.countdownTimer)
	})

	time.Sleep(warmupDuration + countdownDuration + 3*delta)

	require.EqualValues(t, countdownDuration, monitor.countdownDuration)
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	require.EqualValues(t, 0, monitor.totalConnectionCount)

	require.True(t, handlerCalled)

	require.True(t, monitor.NewConnection(ctx))
	require.True(t, monitor.shutdownInitiated)
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	require.EqualValues(t, 0, monitor.totalConnectionCount)
}

func TestMakeIdleMonitor_WithConnectionDuringWarmup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	start := time.Now()
	warmupDuration := 300 * time.Millisecond
	countdownDuration := 600 * time.Millisecond
	delta := 5 * time.Millisecond

	handlerCalled := false
	monitor := MakeIdleMonitor(ctx, warmupDuration, func() {
		// The handler is called after the warmup and countdown durations
		require.WithinDuration(t,
			start.Add(warmupDuration+countdownDuration+delta), time.Now(), delta,
		)
		handlerCalled = true
	}, countdownDuration)

	time.AfterFunc(warmupDuration/2, func() {
		require.False(t, monitor.NewConnection(ctx))
	})

	// activated is set after the warmup duration is over
	time.AfterFunc(warmupDuration-delta, func() {
		require.False(t, monitor.activated)
		require.Nil(t, monitor.countdownTimer)
	})
	time.AfterFunc(warmupDuration+delta, func() {
		require.True(t, monitor.activated)
		// Countdown timer not triggers as there is a connection
		require.Nil(t, monitor.countdownTimer)
	})

	time.Sleep(warmupDuration + countdownDuration + 3*delta)

	require.EqualValues(t, countdownDuration, monitor.countdownDuration)
	require.EqualValues(t, 1, monitor.activeConnectionCount)
	require.EqualValues(t, 1, monitor.totalConnectionCount)

	require.False(t, handlerCalled)

	require.False(t, monitor.NewConnection(ctx))
	require.False(t, monitor.shutdownInitiated)
	require.EqualValues(t, 2, monitor.activeConnectionCount)
	require.EqualValues(t, 2, monitor.totalConnectionCount)
}

func TestMakeIdleMonitor_WithBriefConnectionDuringWarmup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	start := time.Now()
	warmupDuration := 300 * time.Millisecond
	countdownDuration := 600 * time.Millisecond
	delta := 5 * time.Millisecond

	handlerCalled := false
	monitor := MakeIdleMonitor(ctx, warmupDuration, func() {
		// The handler is called after the warmup and countdown durations
		require.WithinDuration(t,
			start.Add(warmupDuration+countdownDuration+delta), time.Now(), delta,
		)
		handlerCalled = true
	}, countdownDuration)

	time.AfterFunc(warmupDuration/3, func() {
		require.False(t, monitor.NewConnection(ctx))
	})
	time.AfterFunc(warmupDuration*2/3, func() {
		monitor.CloseConnection(ctx)
	})

	// activated is set after the warmup duration is over
	time.AfterFunc(warmupDuration-delta, func() {
		require.False(t, monitor.activated)
		require.Nil(t, monitor.countdownTimer)
	})
	time.AfterFunc(warmupDuration+delta, func() {
		require.True(t, monitor.activated)
		// Countdown timer triggers as there is no connection
		require.NotNil(t, monitor.countdownTimer)
	})

	time.Sleep(warmupDuration + countdownDuration + 3*delta)

	require.EqualValues(t, countdownDuration, monitor.countdownDuration)
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	require.EqualValues(t, 1, monitor.totalConnectionCount)

	require.True(t, handlerCalled)

	require.True(t, monitor.NewConnection(ctx))
	require.True(t, monitor.shutdownInitiated)
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	require.EqualValues(t, 1, monitor.totalConnectionCount)
}

func TestMakeIdleMonitor_WithBriefConnectionDuringCountdown(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	start := time.Now()
	warmupDuration := 300 * time.Millisecond
	countdownDuration := 600 * time.Millisecond
	delta := 5 * time.Millisecond

	handlerCalled := false
	monitor := MakeIdleMonitor(ctx, warmupDuration, func() {
		// The handler is called after the warmup and countdown durations
		require.WithinDuration(t,
			start.Add(warmupDuration+countdownDuration*5/3+delta), time.Now(), delta,
		)
		handlerCalled = true
	}, countdownDuration)

	time.AfterFunc(warmupDuration+countdownDuration/3, func() {
		require.False(t, monitor.NewConnection(ctx))
	})
	time.AfterFunc(warmupDuration+countdownDuration*2/3, func() {
		monitor.CloseConnection(ctx)
	})

	// activated is set after the warmup duration is over
	time.AfterFunc(warmupDuration-delta, func() {
		require.False(t, monitor.activated)
		require.Nil(t, monitor.countdownTimer)
	})
	time.AfterFunc(warmupDuration+delta, func() {
		require.True(t, monitor.activated)
		// Countdown timer triggers as there is no connection
		require.NotNil(t, monitor.countdownTimer)
	})

	time.Sleep(warmupDuration + countdownDuration + 3*delta)

	require.EqualValues(t, countdownDuration, monitor.countdownDuration)
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	require.EqualValues(t, 1, monitor.totalConnectionCount)

	require.False(t, handlerCalled)

	time.Sleep(warmupDuration + countdownDuration*5/3 + 3*delta)

	require.True(t, handlerCalled)

	require.True(t, monitor.NewConnection(ctx))
	require.True(t, monitor.shutdownInitiated)
	require.EqualValues(t, 0, monitor.activeConnectionCount)
	require.EqualValues(t, 1, monitor.totalConnectionCount)
}
