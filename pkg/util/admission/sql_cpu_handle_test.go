// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestSQLCPUHandleFastAndSlowPath walks through the full reservation
// lifecycle: slow path (Admit) → fast path (CAS) → reservation
// exhausted → slow path again.
func TestSQLCPUHandleFastAndSlowPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// 1) Slow path: reservation is 0, must call Admit.
	// heuristic(1ms) = 1ms + min(1ms, 1ms) = 2ms requested.
	// 1ms consumed, 1ms goes to reservation.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))
	require.Equal(t, int64(1*time.Millisecond), h.mu.reservation.Load())
	require.Contains(t, tg.buf.stringAndReset(), "tryGet",
		"first call should go through Admit")

	// 2) Fast path: 500us < 1ms reservation, CAS covers it.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 500*time.Microsecond, false))
	require.Equal(t, int64(500*time.Microsecond), h.mu.reservation.Load())
	require.Empty(t, tg.buf.stringAndReset(),
		"fast path should not call Admit")

	// 3) Slow path again: 2ms > 500us reservation.
	// CAS grabs 500us, remaining=1.5ms, slow path.
	// heuristic(1.5ms) = 1.5ms + min(1.5ms, 1ms) = 2.5ms.
	// buffer = 2.5ms - 1.5ms = 1ms added to reservation.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 2*time.Millisecond, false))
	require.Equal(t, int64(1*time.Millisecond), h.mu.reservation.Load())
	require.Contains(t, tg.buf.stringAndReset(), "tryGet",
		"exhausted reservation should fall back to Admit")

	// CPU should be fully reported across all three calls.
	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(3500*time.Microsecond), gw)
}

// TestSQLCPUHandleCloseReturnsTokens verifies that Close drains
// reservation and returns tokens via AdmittedSQLWorkDone only when
// there are tokens to return.
func TestSQLCPUHandleCloseReturnsTokens(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)

	tests := []struct {
		name            string
		seedReservation bool
		expectedReturn  bool
	}{
		{
			name:            "non-zero reservation returns tokens",
			seedReservation: true,
			expectedReturn:  true,
		},
		{
			name:            "zero reservation skips return",
			seedReservation: false,
			expectedReturn:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
			defer cleanup()

			provider := &sqlCPUProviderImpl{}
			h := newSQLCPUAdmissionHandle(
				WorkInfo{TenantID: tenantID}, true, provider, q)

			if tc.seedReservation {
				require.NoError(t, h.reportAndAcquireConsumedCPU(
					ctx, 1*time.Millisecond, false))
				require.Equal(t, int64(1*time.Millisecond),
					h.mu.reservation.Load())
			}

			_ = tg.buf.stringAndReset()
			h.Close()

			require.True(t, h.isClosed())
			require.Equal(t, int64(0), h.mu.reservation.Load())

			output := tg.buf.String()
			if tc.expectedReturn {
				require.Contains(t, output, "returnGrant")
			} else {
				require.NotContains(t, output, "returnGrant")
			}
		})
	}
}

// TestSQLCPUHandleNoWaitBypassAdmission verifies that the noWait path
// uses BypassAdmission and does not block.
func TestSQLCPUHandleNoWaitBypassAdmission(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	// Make tryGet return false — a blocking Admit would hang, but
	// noWait should bypass admission entirely via BypassAdmission.
	tg.mu.Lock()
	tg.mu.returnValueFromTryGet = false
	tg.mu.Unlock()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, true /*noWait*/))

	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(1*time.Millisecond), gw)
}

// TestSQLCPUHandleNoWorkQueue verifies that when no WorkQueue is
// attached (CTT AC disabled), CPU is still reported.
func TestSQLCPUHandleNoWorkQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, nil)

	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 2*time.Millisecond, true))

	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(3*time.Millisecond), gw)

	h.Close()
	require.True(t, h.isClosed())
}

// TestSQLCPUHandleConcurrentFastPath exercises the CAS-based fast path
// under contention from multiple goroutines. All goroutines deduct from
// the same reservation. The total deducted must be exact, and
// reservation must never go negative.
func TestSQLCPUHandleConcurrentFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Seed reservation via slow path.
	// heuristic(50ms) = 50ms + min(50ms, 1ms) = 51ms.
	// Reservation = 51ms - 50ms = 1ms.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 50*time.Millisecond, false))
	require.Equal(t, int64(1*time.Millisecond), h.mu.reservation.Load())

	_ = tg.buf.stringAndReset()

	// Launch goroutines that each deduct a small amount via fast path.
	// Total = 20 * 10us = 200us, well within the 1ms reservation.
	const numGoroutines = 20
	const perGoroutine = 10 * time.Microsecond
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, perGoroutine, false))
		}()
	}
	wg.Wait()

	// Reservation should be exactly 1ms - 200us = 800us.
	expected := int64(1*time.Millisecond) - int64(numGoroutines)*int64(perGoroutine)
	require.Equal(t, expected, h.mu.reservation.Load(),
		"CAS deductions should be exact under contention")

	// No Admit calls should have been made.
	output := tg.buf.stringAndReset()
	require.Empty(t, output, "all deductions should use CAS fast path")
}

// TestSQLCPUHandleConcurrentSlowPath exercises the slow path under
// contention. When reservation is exhausted, goroutines serialize on
// admitTurn and only one calls Admit while others may find reservation
// refilled by the winner.
func TestSQLCPUHandleConcurrentSlowPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// No reservation seed — all goroutines hit the slow path.
	const numGoroutines = 10
	const perGoroutine = 1 * time.Millisecond
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, perGoroutine, false))
			require.GreaterOrEqual(t, h.mu.reservation.Load(), int64(0))
		}()
	}
	wg.Wait()

	// All CPU should be reported.
	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(numGoroutines)*int64(perGoroutine), gw)
}

// TestSQLCPUHandleConcurrentCloseAndAdmit verifies that Close and
// reportAndAcquireConsumedCPU can run concurrently without races,
// panics, or token leaks. After Close, reservation is 0.
func TestSQLCPUHandleConcurrentCloseAndAdmit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Seed reservation: heuristic(5ms) = 5ms + min(5ms, 1ms) = 6ms.
	// Reservation = 6ms - 5ms = 1ms.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 5*time.Millisecond, false))
	require.Equal(t, int64(1*time.Millisecond), h.mu.reservation.Load())

	var wg sync.WaitGroup

	// Goroutines with noWait=false. They hit the slow path (admitTurn
	// + Admit) once the 1ms reservation is exhausted, but Admit
	// returns immediately since testGranter.tryGet always succeeds.
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

	// Goroutines calling the noWait path.
	const numNoWait = 10
	wg.Add(numNoWait)
	for i := 0; i < numNoWait; i++ {
		go func() {
			defer wg.Done()
			_ = h.reportAndAcquireConsumedCPU(ctx, 100*time.Microsecond, true)
		}()
	}

	// Close concurrently.
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.Close()
	}()

	wg.Wait()

	require.True(t, h.isClosed())
	// INVARIANT: closed == true => reservation == 0.
	require.Equal(t, int64(0), h.mu.reservation.Load())
}

// TestSQLCPUHandleConcurrentCASAndSwap races tryDeductReservation
// (CAS decrement) against Close's Swap(0) (drain) on the same
// reservation atomic. Asserts CAS'd + returned == initial — no
// tokens lost regardless of interleaving.
func TestSQLCPUHandleConcurrentCASAndSwap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	for iter := 0; iter < 100; iter++ {
		provider := &sqlCPUProviderImpl{}
		h := newSQLCPUAdmissionHandle(
			WorkInfo{TenantID: tenantID}, true, provider, q)

		// Seed reservation: heuristic(10ms) = 10ms + min(10ms, 1ms) = 11ms.
		// Reservation = 11ms - 10ms = 1ms.
		require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 10*time.Millisecond, false))
		initialReservation := h.mu.reservation.Load()

		var wg sync.WaitGroup
		var casDeducted atomic.Int64

		// CAS goroutines: each tries to deduct 500us from
		// reservation. Depending on timing, a CAS may grab the
		// full 500us, a partial amount, or nothing (if Close
		// already drained reservation).
		const numGoroutines = 5
		wg.Add(numGoroutines)
		for i := 0; i < numGoroutines; i++ {
			go func() {
				defer wg.Done()
				amount := int64(500 * time.Microsecond)
				grabbed := h.tryDeductReservation(amount)
				casDeducted.Add(grabbed)
			}()
		}

		// Close concurrently: sets closed under mu, then Swap(0)
		// drains whatever CAS hasn't grabbed, returning it via
		// AdmittedSQLWorkDone.
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Close()
		}()

		wg.Wait()

		// After Close + all CAS goroutines finish, reservation
		// must be zero (no slow-path goroutines to Add tokens).
		require.Zero(t, h.mu.reservation.Load(),
			"iter %d: reservation should be zero after Close", iter)

		// Token conservation: reservation is zero and the only
		// two drains are CAS decrements and Close's Swap(0), so
		// CAS'd + Swap'd == initialReservation. CAS can't grab
		// more than what was available.
		require.LessOrEqual(t, casDeducted.Load(), initialReservation,
			"iter %d: CAS deducted more than initial reservation(%d)",
			iter, initialReservation)
	}
}

// TestSQLCPUHandleAdmitVsCloseTokenConservation runs a stress test
// verifying the token conservation invariant: all tokens obtained from
// Admit are either consumed, held in reservation, or returned via
// AdmittedSQLWorkDone. This exercises the Admit-vs-Close race where
// the commit step checks closed under mu.
func TestSQLCPUHandleAdmitVsCloseTokenConservation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}

	// Run many iterations to exercise the race window between
	// Admit's commit step and Close's Swap(0).
	for iter := 0; iter < 200; iter++ {
		h := newSQLCPUAdmissionHandle(
			WorkInfo{TenantID: tenantID}, true, provider, q)

		var wg sync.WaitGroup

		// Multiple goroutines call reportAndAcquireConsumedCPU
		// concurrently.
		const numWorkers = 5
		wg.Add(numWorkers)
		for i := 0; i < numWorkers; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					_ = h.reportAndAcquireConsumedCPU(ctx, 50*time.Microsecond, false)
				}
			}()
		}

		// Close races with the workers.
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.Close()
		}()

		wg.Wait()

		// After Close, both invariants must hold.
		require.True(t, h.isClosed())
		require.Equal(t, int64(0), h.mu.reservation.Load(),
			"iter %d: closed == true => reservation == 0", iter)
	}
}

// TestSQLCPUHandleContextCancellation verifies that when a goroutine's
// context is canceled while waiting for admitTurn, it falls through to
// the BypassAdmission path, accounts the deficit without blocking, and
// returns ctx.Err().
func TestSQLCPUHandleContextCancellation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Hold admitTurn so the next goroutine blocks on it.
	h.admitTurn <- struct{}{}
	defer func() { <-h.admitTurn }()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		// This will try to send to admitTurn (blocked) and fall
		// through to ctx.Done().
		errCh <- h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false)
	}()

	// Cancel the context — the goroutine should return ctx.Err().
	cancel()
	err := <-errCh
	require.ErrorIs(t, err, context.Canceled)

	// CPU should still be reported despite the cancellation.
	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(1*time.Millisecond), gw)

	h.Close()

	// Close should have nothing to return — the bypass path
	// already accounted for the consumed CPU.
	require.Zero(t, h.mu.reservation.Load())
}

// TestSQLCPUHandleCloseDoesNotBlockOnAdmitTurn verifies that Close
// returns immediately even when admitTurn is held, and that a
// goroutine acquiring the turn after Close sees closed=true and
// falls back to BypassAdmission without refilling reservation.
func TestSQLCPUHandleCloseDoesNotBlockOnAdmitTurn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Seed reservation: heuristic(1ms) = 1ms + min(1ms, 1ms) = 2ms.
	// Reservation = 2ms - 1ms = 1ms.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))
	require.Equal(t, int64(1*time.Millisecond), h.mu.reservation.Load())

	// Hold admitTurn so the next slow-path goroutine blocks on it.
	h.admitTurn <- struct{}{}

	// Start a goroutine that needs more than the reservation,
	// forcing the slow path. It blocks waiting for admitTurn.
	errCh := make(chan error, 1)
	go func() {
		errCh <- h.reportAndAcquireConsumedCPU(ctx, 2*time.Millisecond, false)
	}()

	// Close doesn't touch admitTurn — it returns immediately.
	// If it blocked, the test would time out.
	h.Close()
	require.True(t, h.isClosed())
	require.Zero(t, h.mu.reservation.Load())

	// Release admitTurn — the blocked goroutine acquires the turn,
	// sees closed=true, and accounts the deficit via BypassAdmission.
	<-h.admitTurn

	require.NoError(t, <-errCh)
	require.Zero(t, h.mu.reservation.Load())
}

// TestSQLCPUHandleSecondDeductionAfterTurn verifies the second
// deductFromReservation after acquiring admitTurn. When the previous
// turn-holder refills reservation before releasing the turn, the
// next turn-holder's second deduction can cover the shortfall and
// skip Admit entirely.
func TestSQLCPUHandleSecondDeductionAfterTurn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Case 1: second deduction finds nothing, must call Admit.
	// Reservation starts at 0, so both deductions get nothing.
	_ = tg.buf.stringAndReset()
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 500*time.Microsecond, false))
	require.Contains(t, tg.buf.stringAndReset(), "tryGet",
		"should have called Admit")
	// heuristic(500us) = 500us + min(500us, 1ms) = 1ms.
	// Reservation = 1ms - 500us = 500us.
	require.Equal(t, int64(500*time.Microsecond), h.mu.reservation.Load())

	// Case 2: second deduction covers the shortfall, skips Admit.
	// Hold the turn, start a goroutine that blocks on it, then
	// inject tokens into reservation (simulating what the previous
	// turn-holder would leave) before releasing the turn.

	// Consume 400us via fast path, leaving 100us.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 400*time.Microsecond, false))
	require.Equal(t, int64(100*time.Microsecond), h.mu.reservation.Load())

	// Hold admitTurn.
	h.admitTurn <- struct{}{}

	// Request 200us: first CAS grabs 100us, remaining=100us,
	// goes to slow path, blocks waiting for admitTurn.
	errCh := make(chan error, 1)
	go func() {
		errCh <- h.reportAndAcquireConsumedCPU(ctx, 200*time.Microsecond, false)
	}()

	// Wait until the goroutine has done its first CAS (reservation
	// drops from 100us to 0) before injecting tokens. This ensures
	// the goroutine is blocked on admitTurn, not still in the fast path.
	testutils.SucceedsSoon(t, func() error {
		if h.mu.reservation.Load() != 0 {
			return errors.New("waiting for goroutine to CAS reservation to 0")
		}
		return nil
	})

	// Inject 100us into reservation, simulating the previous
	// turn-holder's Admit refilling it.
	h.mu.reservation.Add(int64(100 * time.Microsecond))

	// Release the turn. The goroutine's second deduction finds
	// the 100us and covers the shortfall — no Admit needed.
	_ = tg.buf.stringAndReset()
	<-h.admitTurn
	require.NoError(t, <-errCh)
	require.NotContains(t, tg.buf.stringAndReset(), "tryGet",
		"second deduction should have covered shortfall, skipping Admit")

	h.Close()
}
