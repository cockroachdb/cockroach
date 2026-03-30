// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestSQLCPUHandleFastPath verifies that the CAS-based fast path deducts
// from reservation without acquiring refillMu or calling Admit.
func TestSQLCPUHandleFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, q)

	// First call exhausts reservation (0) and goes through slow path,
	// which calls Admit and refills reservation with heuristic buffer.
	tg.mu.Lock()
	tg.mu.returnValueFromTryGet = true
	tg.mu.Unlock()
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))

	// Reservation should now be diffNanos (heuristic=2*diff, minus diff consumed).
	reservationBefore := h.refillMu.reservation.Load()
	require.Equal(t, int64(1*time.Millisecond), reservationBefore,
		"reservation should be heuristic(2*diff) - diff = diff")

	// Second call should deduct via fast path CAS without calling Admit.
	// Clear the testGranter buffer to verify no new Admit call is made.
	_ = tg.buf.stringAndReset()
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 500*time.Microsecond, false))

	reservationAfter := h.refillMu.reservation.Load()
	require.Equal(t, reservationBefore-int64(500*time.Microsecond), reservationAfter)

	// Verify no Admit call was made (no tryGet in the buffer).
	output := tg.buf.stringAndReset()
	require.Empty(t, output, "fast path should not call Admit")

	// CPU should still be reported.
	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(1*time.Millisecond+500*time.Microsecond), gw)
}

// TestSQLCPUHandleSlowPath verifies that when reservation is exhausted,
// the slow path acquires refillMu, calls Admit, and refills reservation.
func TestSQLCPUHandleSlowPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, q)

	tg.mu.Lock()
	tg.mu.returnValueFromTryGet = true
	tg.mu.Unlock()

	// First call: reservation is 0, so slow path is taken.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 2*time.Millisecond, false))

	// heuristic = 2 * 2ms = 4ms requested. 2ms consumed, so reservation = 2ms.
	require.Equal(t, int64(2*time.Millisecond), h.refillMu.reservation.Load())

	// Exhaust the reservation with a large request.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 5*time.Millisecond, false))

	// 5ms > 2ms reservation, so slow path runs again.
	// Elapsed < 1ms (grow): buffer = 2ms * 2 = 4ms. Total = 5ms + 4ms = 9ms.
	// Reservation += 9ms - 5ms = 4ms, total = existing(2ms) + 4ms = 6ms.
	require.Equal(t, int64(6*time.Millisecond), h.refillMu.reservation.Load())
}

// TestSQLCPUHandleCloseReturnsTokens verifies that Close returns unused
// reservation tokens via AdmittedSQLWorkDone.
func TestSQLCPUHandleCloseReturnsTokens(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, q)

	tg.mu.Lock()
	tg.mu.returnValueFromTryGet = true
	tg.mu.Unlock()

	// Acquire tokens so reservation has a buffer.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))
	remaining := h.refillMu.reservation.Load()
	require.Equal(t, int64(1*time.Millisecond), remaining)

	// Clear buffer and close.
	_ = tg.buf.stringAndReset()
	h.Close()

	// Verify returnGrant was called with the remaining reservation.
	output := tg.buf.String()
	require.Contains(t, output, "returnGrant")

	// Reservation should be zeroed.
	require.Equal(t, int64(0), h.refillMu.reservation.Load())
	require.True(t, h.refillMu.closed.Load())
}

// TestSQLCPUHandleCloseZeroReservation verifies that Close calls
// AdmittedSQLWorkDone even when reservation is 0 (defensive).
func TestSQLCPUHandleCloseZeroReservation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, q)

	// No CPU consumed, reservation is 0. Close should still work.
	_ = tg.buf.stringAndReset()
	h.Close()

	require.True(t, h.refillMu.closed.Load())
	// No returnGrant or tookWithoutPermission since remaining is 0.
	output := tg.buf.String()
	require.NotContains(t, output, "returnGrant")
	require.NotContains(t, output, "tookWithoutPermission")
}

// TestSQLCPUHandleClosedSkipsAdmission verifies that after Close,
// reportAndAcquireConsumedCPU still reports CPU but skips Admit.
func TestSQLCPUHandleClosedSkipsAdmission(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, q)

	h.Close()
	require.True(t, h.refillMu.closed.Load())

	// Post-close reportAndAcquireConsumedCPU should still report CPU.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 3*time.Millisecond, false))
	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(3*time.Millisecond), gw)
}

// TestSQLCPUHandleNoWaitBypassAdmission verifies that the noWait path
// uses BypassAdmission=true and does not block or deduct from reservation.
func TestSQLCPUHandleNoWaitBypassAdmission(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	// Make tryGet return false — a blocking Admit would hang, but noWait
	// should bypass admission entirely.
	tg.mu.Lock()
	tg.mu.returnValueFromTryGet = false
	tg.mu.Unlock()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, q)

	// noWait=true should not block even when tryGet returns false,
	// because BypassAdmission=true skips the granter entirely.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, true))

	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(1*time.Millisecond), gw)
}

// TestSQLCPUHandleRegisterGoroutineIdempotent verifies that registering the
// same goroutine ID twice returns the existing handle.
func TestSQLCPUHandleRegisterGoroutineIdempotent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tenantID := roachpb.MustMakeTenantID(1)
	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, nil)

	gh1 := h.RegisterGoroutine()
	gh2 := h.RegisterGoroutine()
	require.Same(t, gh1, gh2, "same goroutine should get the same handle")

	h.mu.Lock()
	require.Len(t, h.mu.gHandles, 1)
	h.mu.Unlock()
}

// TestSQLCPUHandleClosePoolsHandles verifies that Close pools
// GoroutineCPUHandles that have been closed, and nils out unclosed ones.
func TestSQLCPUHandleClosePoolsHandles(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, nil)

	gh := h.RegisterGoroutine()
	gh.Close(ctx)
	require.True(t, gh.closed.Load())

	h.Close()
	h.mu.Lock()
	require.Nil(t, h.mu.gHandles, "gHandles should be nil after Close")
	h.mu.Unlock()
}

// TestSQLCPUHandleConcurrentFastPath exercises the CAS-based fast path
// under contention from multiple goroutines. All goroutines deduct from the
// same reservation via CompareAndSwap. The total deducted must equal the sum
// of individual deductions, and reservation must never go negative.
func TestSQLCPUHandleConcurrentFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	tg.mu.Lock()
	tg.mu.returnValueFromTryGet = true
	tg.mu.Unlock()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, q)

	// Seed the reservation by calling the slow path. With 50ms diff,
	// buffer = min(50ms, maxRefillHeuristic=10ms) = 10ms. Total =
	// 50ms + 10ms = 60ms. Reservation = 60ms - 50ms = 10ms.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 50*time.Millisecond, false))
	require.Equal(t, int64(10*time.Millisecond), h.refillMu.reservation.Load())

	// Clear the buffer so we can check if any slow-path Admit calls happen.
	_ = tg.buf.stringAndReset()

	// Launch N goroutines that each deduct a small amount via fast path.
	const numGoroutines = 20
	const perGoroutine = 100 * time.Microsecond // 100us each = 2ms total
	var wg sync.WaitGroup
	var errors atomic.Int64
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			if err := h.reportAndAcquireConsumedCPU(ctx, perGoroutine, false); err != nil {
				errors.Add(1)
			}
		}()
	}
	wg.Wait()
	require.Equal(t, int64(0), errors.Load())

	// Verify reservation was correctly deducted.
	expected := int64(10*time.Millisecond) - int64(numGoroutines)*int64(perGoroutine)
	require.Equal(t, expected, h.refillMu.reservation.Load(),
		"CAS deductions should be exact under contention")

	// Verify no Admit calls were made (all satisfied via fast path).
	output := tg.buf.stringAndReset()
	require.Empty(t, output, "all deductions should use CAS fast path")
}

// TestSQLCPUHandleConcurrentSlowPath exercises the slow path under
// contention. When reservation is exhausted, goroutines serialize on
// refillMu and only one calls Admit while others may find reservation
// refilled by the winner.
func TestSQLCPUHandleConcurrentSlowPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	tg.mu.Lock()
	tg.mu.returnValueFromTryGet = true
	tg.mu.Unlock()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, q)

	// Don't seed reservation — all goroutines must go through the slow
	// path (or find reservation refilled by another goroutine).
	const numGoroutines = 10
	const perGoroutine = 1 * time.Millisecond
	var wg sync.WaitGroup
	var errors atomic.Int64
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			if err := h.reportAndAcquireConsumedCPU(ctx, perGoroutine, false); err != nil {
				errors.Add(1)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(0), errors.Load())
	// Reservation should be non-negative.
	require.GreaterOrEqual(t, h.refillMu.reservation.Load(), int64(0),
		"reservation must never be negative")

	// All CPU should be reported.
	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(numGoroutines)*int64(perGoroutine), gw)
}

// TestSQLCPUHandleConcurrentCloseAndAdmit verifies that Close and
// reportAndAcquireConsumedCPU (both blocking and noWait) can run
// concurrently without races or panics. After Close, ongoing calls
// should see the closed flag and skip admission.
func TestSQLCPUHandleConcurrentCloseAndAdmit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	tg.mu.Lock()
	tg.mu.returnValueFromTryGet = true
	tg.mu.Unlock()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, q)

	// Seed reservation.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 5*time.Millisecond, false))

	var wg sync.WaitGroup

	// Start goroutines that repeatedly call reportAndAcquireConsumedCPU
	// via the blocking path (simulating MeasureAndAdmit).
	const numBlocking = 10
	wg.Add(numBlocking)
	for i := 0; i < numBlocking; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = h.reportAndAcquireConsumedCPU(ctx, 10*time.Microsecond, false)
			}
		}()
	}

	// Start goroutines that call the noWait path (simulating
	// GoroutineCPUHandle.Close).
	const numNoWait = 10
	wg.Add(numNoWait)
	for i := 0; i < numNoWait; i++ {
		go func() {
			defer wg.Done()
			_ = h.reportAndAcquireConsumedCPU(ctx, 100*time.Microsecond, true /* noWait */)
		}()
	}

	// Close concurrently.
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.Close()
	}()

	wg.Wait()

	require.True(t, h.refillMu.closed.Load())
	require.Equal(t, int64(0), h.refillMu.reservation.Load(),
		"Close should have swapped reservation to 0")
}

// TestSQLCPUHandleConcurrentCASAndSwap verifies that the fast-path CAS
// and Close's Swap(0) don't lose tokens. After Close, the reservation is
// 0 and Close has returned whatever was left.
func TestSQLCPUHandleConcurrentCASAndSwap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	tg.mu.Lock()
	tg.mu.returnValueFromTryGet = true
	tg.mu.Unlock()

	provider := &sqlCPUProviderImpl{}

	// Run many iterations to exercise the race window.
	for iter := 0; iter < 100; iter++ {
		h := newSQLCPUAdmissionHandle(
			WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, q)

		// Seed a large reservation.
		require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 10*time.Millisecond, false))

		var wg sync.WaitGroup
		var casDeducted atomic.Int64

		// Goroutines try CAS deductions concurrently.
		const numGoroutines = 5
		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				amount := int64(500 * time.Microsecond)
				if h.tryDeductReservation(amount) {
					casDeducted.Add(amount)
				}
			}()
		}

		// Close concurrently (Swap(0)).
		wg.Add(1)
		var swapped int64
		go func() {
			defer wg.Done()
			h.refillMu.Lock()
			h.refillMu.closed.Store(true)
			swapped = h.refillMu.reservation.Swap(0)
			h.refillMu.Unlock()
		}()

		wg.Wait()

		// The initial reservation (10ms) should be fully accounted for:
		// either deducted via CAS or returned via Swap.
		initialReservation := int64(10 * time.Millisecond)
		totalAccountedFor := casDeducted.Load() + swapped
		require.Equal(t, initialReservation, totalAccountedFor,
			"iter %d: CAS(%d) + Swap(%d) should equal initial reservation(%d)",
			iter, casDeducted.Load(), swapped, initialReservation)
	}
}

// TestSQLCPUHandleConcurrentRegisterGoroutine verifies that concurrent
// calls to RegisterGoroutine from different goroutines are safe and each
// goroutine gets its own handle.
func TestSQLCPUHandleConcurrentRegisterGoroutine(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tenantID := roachpb.MustMakeTenantID(1)
	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, nil)

	const numGoroutines = 20
	handles := make([]*GoroutineCPUHandle, numGoroutines)
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		i := i
		go func() {
			defer wg.Done()
			handles[i] = h.RegisterGoroutine()
		}()
	}
	wg.Wait()

	// Each goroutine should have a unique handle.
	seen := make(map[*GoroutineCPUHandle]bool)
	for _, gh := range handles {
		require.NotNil(t, gh)
		require.False(t, seen[gh], "each goroutine should get a unique handle")
		seen[gh] = true
	}

	h.mu.Lock()
	require.Equal(t, numGoroutines, len(h.mu.gHandles))
	h.mu.Unlock()
}

// TestSQLCPUHandleConcurrentMeasureAndClose exercises the real
// GoroutineCPUHandle.MeasureAndAdmit and Close paths under concurrency,
// simulating the actual DistSQL flow pattern.
func TestSQLCPUHandleConcurrentMeasureAndClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	tg.mu.Lock()
	tg.mu.returnValueFromTryGet = true
	tg.mu.Unlock()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, q)

	const numGoroutines = 5
	var wg sync.WaitGroup

	// Each goroutine registers, calls MeasureAndAdmit several times,
	// then closes its handle — mimicking a DistSQL flow.
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			gh := h.RegisterGoroutine()
			for j := 0; j < 10; j++ {
				_ = gh.MeasureAndAdmit(ctx)
			}
			gh.Close(ctx)
		}()
	}
	wg.Wait()

	// All goroutine handles should be closed.
	h.mu.Lock()
	for _, gh := range h.mu.gHandles {
		require.True(t, gh.closed.Load())
	}
	h.mu.Unlock()

	// SQLCPUHandle close should pool all the closed handles.
	h.Close()
	require.True(t, h.refillMu.closed.Load())
	h.mu.Lock()
	require.Nil(t, h.mu.gHandles)
	h.mu.Unlock()
}

// TestSQLCPUHandleNoWorkQueue verifies that when no WorkQueue is
// attached (CTT AC disabled), reportAndAcquireConsumedCPU still reports
// CPU but skips all admission logic.
func TestSQLCPUHandleNoWorkQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, nil)

	// Both blocking and noWait paths should succeed without a WorkQueue.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 2*time.Millisecond, true))

	// CPU should still be reported.
	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(3*time.Millisecond), gw)

	// Reservation stays at 0 (no refills without a WorkQueue).
	require.Equal(t, int64(0), h.refillMu.reservation.Load())

	// Close should work cleanly.
	h.Close()
	require.True(t, h.refillMu.closed.Load())
}

// TestSQLCPUHandlePauseMeasuring verifies that pausing and unpausing
// measurement works correctly with concurrent MeasureAndAdmit calls.
func TestSQLCPUHandlePauseMeasuring(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tenantID := roachpb.MustMakeTenantID(1)
	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true /* atGateway */, provider, nil)

	gh := h.RegisterGoroutine()

	// Nested pause/unpause.
	gh.PauseMeasuring()
	require.Equal(t, 1, gh.paused)
	gh.PauseMeasuring()
	require.Equal(t, 2, gh.paused)

	// MeasureAndAdmit while paused should be a no-op.
	ctx := context.Background()
	require.NoError(t, gh.MeasureAndAdmit(ctx))

	gh.UnpauseMeasuring()
	require.Equal(t, 1, gh.paused)
	gh.UnpauseMeasuring()
	require.Equal(t, 0, gh.paused)

	gh.Close(ctx)
	h.Close()
}

// TestRefillHeuristic exercises the adaptive buffer sizing in
// refillHeuristicLocked using datadriven testdata.
func TestRefillHeuristic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	h := &SQLCPUHandle{}
	callHeuristic := func(diffNanos int64) (buffer, total int64) {
		h.refillMu.Lock()
		defer h.refillMu.Unlock()
		total = h.refillHeuristicLocked(diffNanos)
		buffer = h.refillMu.lastHeuristic
		return buffer, total
	}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "refill_heuristic"),
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "refill":
				var diffStr string
				d.ScanArgs(t, "diff", &diffStr)
				diff, err := time.ParseDuration(diffStr)
				require.NoError(t, err)
				buffer, total := callHeuristic(diff.Nanoseconds())
				return fmt.Sprintf("buffer: %s, total: %s\n",
					time.Duration(buffer), time.Duration(total))

			case "set-age":
				var ageStr string
				d.ScanArgs(t, "age", &ageStr)
				age, err := time.ParseDuration(ageStr)
				require.NoError(t, err)
				h.refillMu.lastRefillTime = timeutil.Now().Add(-age)
				return "ok\n"

			case "set-buffer":
				var valStr string
				d.ScanArgs(t, "val", &valStr)
				val, err := time.ParseDuration(valStr)
				require.NoError(t, err)
				h.refillMu.lastHeuristic = val.Nanoseconds()
				return "ok\n"

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

// makeBenchWorkQueue creates a WorkQueue in CTT mode for benchmarks.
func makeBenchWorkQueue(b *testing.B) (q *WorkQueue, tg *testGranter) {
	var buf builderWithMu
	tg = &testGranter{buf: &buf}
	tg.mu.returnValueFromTryGet = true

	st := cluster.MakeTestingClusterSettings()
	metrics := makeWorkQueueMetrics("", metric.NewRegistry())
	initialTime := timeutil.FromUnixMicros(
		int64(100) * int64(time.Millisecond/time.Microsecond))
	opts := makeWorkQueueOptions(KVWork)
	opts.mode = usesCPUTimeTokens
	cpuMetrics := makeCPUTimeTokenMetrics()
	opts.perTenantAggMetrics = &tenantAggMetrics{
		admittedCount:  cpuMetrics.AdmittedCountPerTenant[systemTenant],
		waitTimeNanos:  cpuMetrics.WaitTimeNanosPerTenant[systemTenant],
		tokensUsed:     cpuMetrics.TokensUsedPerTenant[systemTenant],
		tokensReturned: cpuMetrics.TokensReturnedPerTenant[systemTenant],
	}
	opts.timeSource = timeutil.NewManualTime(initialTime)
	opts.disableEpochClosingGoroutine = true
	opts.disableGCTenantsAndResetUsed = true
	q = makeWorkQueue(log.MakeTestingAmbientContext(tracing.NewTracer()),
		KVWork, tg, st, metrics, opts).(*WorkQueue)
	tg.r = q
	b.Cleanup(q.close)
	return q, tg
}

// BenchmarkSQLCPUHandleReservation measures the throughput benefit of the
// reservation under concurrent goroutine contention.
//
// "with-reservation" uses the normal flow: the heuristic buffer lets ~half
// the checkpoints deduct via lock-free CAS (fast path), avoiding Admit
// entirely.
//
// "without-reservation" drains the reservation before each checkpoint,
// forcing every call through the slow path (refillMu lock + Admit).
//
// The ns/op difference shows the real cost saved by the reservation: under
// contention the fast path avoids both the refillMu lock and the WorkQueue
// internal lock, so throughput scales with goroutine count.
func BenchmarkSQLCPUHandleReservation(b *testing.B) {
	const cpuPerCheckpoint = 100 * time.Microsecond

	for _, numGoroutines := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("goroutines=%d", numGoroutines), func(b *testing.B) {
			for _, useReservation := range []bool{true, false} {
				label := "with-reservation"
				if !useReservation {
					label = "without-reservation"
				}
				b.Run(label, func(b *testing.B) {
					ctx := context.Background()
					tenantID := roachpb.MustMakeTenantID(1)
					q, _ := makeBenchWorkQueue(b)

					provider := &sqlCPUProviderImpl{}
					h := newSQLCPUAdmissionHandle(
						WorkInfo{TenantID: tenantID}, true, provider, q)

					// Pre-seed reservation so the first iteration isn't
					// a cold start.
					_ = h.reportAndAcquireConsumedCPU(ctx, cpuPerCheckpoint, false)

					b.ResetTimer()
					var wg sync.WaitGroup
					wg.Add(numGoroutines)
					opsPerGoroutine := b.N / numGoroutines
					for g := 0; g < numGoroutines; g++ {
						go func() {
							defer wg.Done()
							for i := 0; i < opsPerGoroutine; i++ {
								if !useReservation {
									// Drain reservation to force every call
									// through the slow path (Admit).
									h.refillMu.reservation.Store(0)
								}
								_ = h.reportAndAcquireConsumedCPU(
									ctx, cpuPerCheckpoint, false)
							}
						}()
					}
					wg.Wait()
					b.StopTimer()
					h.Close()
				})
			}
		})
	}
}
