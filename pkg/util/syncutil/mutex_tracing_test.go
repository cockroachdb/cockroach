// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package syncutil

import (
	"context"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// tracedLocker is a variant of sync.Locker that implements TracedLock and
// TimedLock instead of Lock.
type tracedLocker interface {
	TracedLock(context.Context)
	TimedLock() time.Duration
	Unlock()
}

type rTracedLocker RWMutex

func (r *rTracedLocker) TracedLock(ctx context.Context) { (*RWMutex)(r).TracedRLock(ctx) }
func (r *rTracedLocker) TimedLock() time.Duration       { return (*RWMutex)(r).TimedRLock() }
func (r *rTracedLocker) Unlock()                        { (*RWMutex)(r).RUnlock() }

func TestTracedLock(t *testing.T) {
	ctx := context.Background()

	// Assume that ExpensiveLogEnabled(ctx, vLevel) is true.
	defer setExpensiveLogEnabled(true)()

	// Consider any locking duration to be sufficient to log, as long as the
	// acquisition does not hit the TryLock fast-path. This avoids a timing
	// dependency.
	defer setSlowLockLogThreshold(0 * time.Microsecond)()

	// Each test case intercepts the log details and check that they are as
	// expected. In particular, this checks the log level and the log message.
	interceptLogLogVEventfDepth := func(t *testing.T) (*bool, func()) {
		logged := false
		cleanup := setLogVEventfDepth(func(_ context.Context, depth int, level int32, format string, args ...interface{}) {
			// Verify that depth is correct so that the log message is
			// attributed to the caller of TracedLock.
			require.Equal(t, 2, depth)
			// NOTE: +1 for the call to runtime.Caller.
			_, file, _, ok := runtime.Caller(depth + 1)
			require.True(t, ok)
			require.Equal(t, "mutex_tracing_test.go", filepath.Base(file))
			// Verify the log verbosity level.
			require.Equal(t, int32(3), level)
			// Verify the log message.
			logText := fmt.Sprintf(format, args...)
			require.Regexp(t, `slow mutex acquisition took .*`, logText)
			logged = true
		})
		return &logged, cleanup
	}

	testCases := []struct {
		name string
		mu   tracedLocker
	}{
		{"Mutex.TracedLock", &Mutex{}},
		{"RWMutex.TracedLock", &RWMutex{}},
		{"RWMutex.TracedRLock", (*rTracedLocker)(&RWMutex{})},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			for _, fastPath := range []bool{false, true} {
				if fastPath && DeadlockEnabled {
					// TryLock is a no-op for deadlock mutexes.
					continue
				}
				t.Run(fmt.Sprintf("fast-path=%t", fastPath), func(t *testing.T) {
					defer setEnableTracedLockFastPath(fastPath)()

					logged, cleanup := interceptLogLogVEventfDepth(t)
					defer cleanup()

					// NOTE: defer the Unlock to unlock even if the test fails.
					defer c.mu.Unlock()

					c.mu.TracedLock(ctx)
					require.Equal(t, !fastPath, *logged)
				})
			}
		})
	}
}

func TestTimedLock(t *testing.T) {
	testCases := []struct {
		name string
		mu   tracedLocker
	}{
		{"Mutex.TimedLock", &Mutex{}},
		{"RWMutex.TimedLock", &RWMutex{}},
		{"RWMutex.TimedRLock", (*rTracedLocker)(&RWMutex{})},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			for _, fastPath := range []bool{false, true} {
				if fastPath && DeadlockEnabled {
					// TryLock is a no-op for deadlock mutexes.
					continue
				}
				t.Run(fmt.Sprintf("fast-path=%t", fastPath), func(t *testing.T) {
					defer setEnableTracedLockFastPath(fastPath)()

					// NOTE: defer the Unlock to unlock even if the test fails.
					defer c.mu.Unlock()

					dur := c.mu.TimedLock()
					if fastPath {
						require.Zero(t, dur)
					} else {
						require.Greater(t, dur, time.Duration(0))
					}
				})
			}
		})
	}
}

// BenchmarkTracedLock benchmarks the performance of TracedLock. It includes
// subtests for uncontended and contended cases, which dictate whether the
// function's fast-path is hit. It also includes further subtests for if tracing
// is enabled or disabled and for if the lock duration is long enough to warrant
// logging.
//
// Note that when the fast path is disabled, the benchmark is not measuring the
// cost of the TryLock call.
//
// Note also that even on the slow path when tracing is enabled, the calls to
// log.ExpensiveLogEnabled and log.VEventfDepth are still mocked out, so this
// does not measure the full cost of tracing.
//
// Results with go1.21.4 on a Mac with an Apple M1 Pro processor:
//
// name                                          time/op
// TracedLock/uncontended-10                     3.82ns ± 7%
// TracedLock/contended/tracing_disabled-10      4.10ns ± 0%
// TracedLock/contended/tracing_enabled/fast-10  71.0ns ± 0%
// TracedLock/contended/tracing_enabled/slow-10   453ns ± 1%
func BenchmarkTracedLock(b *testing.B) {
	ctx := context.Background()
	run := func(b *testing.B) {
		mus := make([]Mutex, b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			mus[i].TracedLock(ctx)
		}
	}

	b.Run("uncontended", func(b *testing.B) {
		defer setEnableTracedLockFastPath(true)()
		run(b)
	})

	b.Run("contended", func(b *testing.B) {
		defer setEnableTracedLockFastPath(false)()

		b.Run("tracing disabled", func(b *testing.B) {
			defer setExpensiveLogEnabled(false)()
			run(b)
		})

		b.Run("tracing enabled", func(b *testing.B) {
			defer setExpensiveLogEnabled(true)()

			b.Run("fast", func(b *testing.B) {
				defer setSlowLockLogThreshold(10 * time.Minute)()
				run(b)
			})

			b.Run("slow", func(b *testing.B) {
				defer setSlowLockLogThreshold(0 * time.Microsecond)()
				run(b)
			})
		})
	})
}

// BenchmarkTimedLock benchmarks the performance of TimedLock. It includes
// subtests for uncontended and contended cases, which dictate whether the
// function's fast-path is hit.
//
// Note that when the fast path is disabled, the benchmark is not measuring the
// cost of the TryLock call.
//
// Results with go1.21.4 on a Mac with an Apple M1 Pro processor:
//
// name                      time/op
// TimedLock/uncontended-10  3.47ns ± 1%
// TimedLock/contended-10    72.6ns ± 2%
func BenchmarkTimedLock(b *testing.B) {
	run := func(b *testing.B) {
		mus := make([]Mutex, b.N)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = mus[i].TimedLock()
		}
	}

	b.Run("uncontended", func(b *testing.B) {
		defer setEnableTracedLockFastPath(true)()
		run(b)
	})

	b.Run("contended", func(b *testing.B) {
		defer setEnableTracedLockFastPath(false)()
		run(b)
	})
}

func setEnableTracedLockFastPath(b bool) func() {
	return setGlobal(&enableTracedLockFastPath, b)
}

func setSlowLockLogThreshold(dur time.Duration) func() {
	return setGlobal(&slowLockLogThreshold, dur)
}

func setExpensiveLogEnabled(b bool) func() {
	return setGlobal(&LogExpensiveLogEnabled, func(ctx context.Context, level int32) bool { return b })
}

func setLogVEventfDepth(
	f func(ctx context.Context, depth int, level int32, format string, args ...interface{}),
) func() {
	return setGlobal(&LogVEventfDepth, f)
}

func setGlobal[T any](g *T, v T) (cleanup func()) {
	old := *g
	*g = v
	return func() { *g = old }
}
