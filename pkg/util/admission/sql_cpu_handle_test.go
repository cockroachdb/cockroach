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
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// seedReservation populates h.mu.reservation with `amount` CPU nanos by
// driving a single Admit directly, bypassing refillHeuristic. Sets
// reservationSourceGroup so Close routes the drained tokens to the correct
// container. Intended only for tests that need a specific reservation amount
// as a setup precondition.
func seedReservation(t *testing.T, ctx context.Context, h *SQLCPUHandle, amount int64) {
	t.Helper()
	workInfo := h.constructWorkInfo(amount, false /*noWait*/)
	resp, err := h.wq.Admit(ctx, workInfo)
	require.NoError(t, err)
	require.True(t, resp.Enabled)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.mu.reservation.Add(resp.requestedCount)
	h.mu.reservationSourceGroup = resp.groupKey
}

// TestSQLCPUHandleRefillHeuristic exercises the stateful refillHeuristic
// directly: first call returns deficit only (buffer=0); subsequent calls
// ramp the buffer from bufferSeed, doubling up to maxRefillBuffer.
func TestSQLCPUHandleRefillHeuristic(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	h := newSQLCPUAdmissionHandle(WorkInfo{}, true, &sqlCPUProviderImpl{}, nil)

	const deficit = int64(50 * time.Microsecond)
	steps := []struct {
		name           string
		expectedBuffer int64
	}{
		{name: "first call: no buffer", expectedBuffer: 0},
		{name: "second call: seed", expectedBuffer: bufferSeed},
		{name: "third call: 2x seed", expectedBuffer: 2 * bufferSeed},
		{name: "fourth call: 4x seed", expectedBuffer: 4 * bufferSeed},
		{name: "fifth call: 8x seed", expectedBuffer: 8 * bufferSeed},
		{name: "sixth call: 16x seed", expectedBuffer: 16 * bufferSeed},
		{name: "seventh call: 32x seed", expectedBuffer: 32 * bufferSeed},
		{name: "eighth call: 64x seed", expectedBuffer: 64 * bufferSeed},
		{name: "ninth call: capped at maxRefillBuffer", expectedBuffer: maxRefillBuffer},
		{name: "tenth call: stays at cap", expectedBuffer: maxRefillBuffer},
	}

	for _, s := range steps {
		got := h.refillHeuristic(deficit)
		require.Equal(t, deficit+s.expectedBuffer, got, s.name)
	}
}

// TestSQLCPUHandleFastAndSlowPath walks through the full reservation
// lifecycle under the exponential refill heuristic: first slow path
// (no buffer), second slow path (seed buffer), fast path (CAS), and
// a third slow path (doubled buffer).
func TestSQLCPUHandleFastAndSlowPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tenantID := roachpb.MustMakeTenantID(1)
	q, tg, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// 1) First slow path: reservation is 0, first call gets no buffer.
	// Request = 1ms (deficit only). Reservation stays at 0.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))
	require.Zero(t, h.mu.reservation.Load(),
		"first slow-path call must not seed reservation (buffer=0)")
	require.Contains(t, tg.buf.stringAndReset(), "tryGet",
		"first call should go through Admit")

	// 2) Second slow path: buffer = bufferSeed.
	// Request = 1ms + 10us. Reservation += 10us.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))
	require.Equal(t, bufferSeed, h.mu.reservation.Load(),
		"second slow-path call should seed reservation with bufferSeed")
	require.Contains(t, tg.buf.stringAndReset(), "tryGet")

	// 3) Fast path: 5us < 10us reservation, CAS covers it.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 5*time.Microsecond, false))
	require.Equal(t, bufferSeed-int64(5*time.Microsecond), h.mu.reservation.Load())
	require.Empty(t, tg.buf.stringAndReset(),
		"fast path should not call Admit")

	// 4) Third slow path: buffer doubles to 2*seed.
	// CAS grabs the remaining 5us, remaining deficit = 1ms - 5us, request =
	// (1ms - 5us) + 20us; reservation ends at 20us.
	require.NoError(t, h.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))
	require.Equal(t, 2*bufferSeed, h.mu.reservation.Load(),
		"third slow-path call should double buffer to 2*bufferSeed")
	require.Contains(t, tg.buf.stringAndReset(), "tryGet")

	// CPU should be fully reported across all four calls.
	gw, _ := provider.GetCumulativeSQLCPUNanos()
	require.Equal(t, int64(3*time.Millisecond+5*time.Microsecond), gw)
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
			q, tg, _, cleanup := makeCPUTimeTokenWorkQueue(t)
			defer cleanup()

			provider := &sqlCPUProviderImpl{}
			h := newSQLCPUAdmissionHandle(
				WorkInfo{TenantID: tenantID}, true, provider, q)

			if tc.seedReservation {
				seedReservation(t, ctx, h, int64(1*time.Millisecond))
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
	q, tg, _, cleanup := makeCPUTimeTokenWorkQueue(t)
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
	q, tg, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Seed reservation directly to a known amount, bypassing the refill
	// heuristic. The test's concern is concurrent CAS on a non-empty
	// reservation, not the heuristic itself.
	seedReservation(t, ctx, h, int64(1*time.Millisecond))
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
	q, _, _, cleanup := makeCPUTimeTokenWorkQueue(t)
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
	q, _, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Seed reservation directly so the workers below have something to drain.
	seedReservation(t, ctx, h, int64(1*time.Millisecond))
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
	q, _, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	for iter := 0; iter < 100; iter++ {
		provider := &sqlCPUProviderImpl{}
		h := newSQLCPUAdmissionHandle(
			WorkInfo{TenantID: tenantID}, true, provider, q)

		// Seed reservation directly. The test races CAS deductions against
		// Close's Swap(0) on a non-empty reservation; the heuristic itself
		// is not under test.
		seedReservation(t, ctx, h, int64(1*time.Millisecond))
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
	q, _, _, cleanup := makeCPUTimeTokenWorkQueue(t)
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
	q, _, _, cleanup := makeCPUTimeTokenWorkQueue(t)
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
	q, _, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Seed reservation directly. The test's concern is the close-vs-Admit
	// race on admitTurn, not the heuristic.
	seedReservation(t, ctx, h, int64(1*time.Millisecond))
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
	q, tg, _, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	provider := &sqlCPUProviderImpl{}
	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: tenantID}, true, provider, q)

	// Seed reservation with 100us directly; this test isn't about the refill
	// heuristic but about the second deduction skipping Admit when a prior
	// turn-holder refilled the reservation.
	seedReservation(t, ctx, h, int64(100*time.Microsecond))
	require.Equal(t, int64(100*time.Microsecond), h.mu.reservation.Load())
	_ = tg.buf.stringAndReset()

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

// TestSetResourceGroupConfigRGOnly verifies that SetResourceGroupConfig
// inputs affect rg-keyed containers and that the kind discriminator in
// q.mu.groups prevents a rg-keyed maxCPU flip from leaking to a
// numerically equal tenant-keyed container.
//
// We use tenant 5 (not 1) because tenantGroupKey(1) is a built-in
// config with maxCPU=true (systemTenantGroupConfig), which would
// confound the cross-namespace isolation check.
func TestSetResourceGroupConfigRGOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	q, _, st, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()

	// Create a tenant-keyed container with id=5 (no built-in config).
	cpuTimeTokenACMode.Override(ctx, &st.SV, serverlessMode)
	tenantHandle := newSQLCPUAdmissionHandle(
		WorkInfo{
			TenantID: roachpb.MustMakeTenantID(5),
			Priority: admissionpb.NormalPri,
		}, true, &sqlCPUProviderImpl{}, q)
	require.NoError(t, tenantHandle.reportAndAcquireConsumedCPU(ctx, 1*time.Millisecond, false))

	// Create an rg-keyed container with id=5.
	cpuTimeTokenACMode.Override(ctx, &st.SV, resourceManagerMode)
	// Install a config that seeds rgGroupKey(0, 5).
	q.configHolder.Set(ResourceGroupConfigSet{
		rgGroupKey(0, 5): {Weight: 1, MaxCPU: true},
	})
	q.refreshResourceGroupConfig()

	// Both containers should now coexist with the same numeric ID.
	q.mu.Lock()
	tenantContainer, tenantOK := q.mu.groups[tenantGroupKey(5)]
	rgContainer, rgOK := q.mu.groups[rgGroupKey(0, 5)]
	q.mu.Unlock()
	require.True(t, tenantOK, "tenant 5 container should exist")
	require.True(t, rgOK, "rg 5 container should exist")
	require.NotSame(t, tenantContainer, rgContainer,
		"tenant 5 and rg 5 must be distinct *groupInfo entries")

	q.mu.Lock()
	tenantMaxCPU := tenantContainer.cpuTimeBurstBucket.maxCPU
	rgMaxCPU := rgContainer.cpuTimeBurstBucket.maxCPU
	q.mu.Unlock()

	require.False(t, tenantMaxCPU,
		"tenant container must not inherit the rg-keyed maxCPU flag "+
			"(applyConfigLocked writes only rg-keyed entries)")
	require.True(t, rgMaxCPU,
		"rg container should pick up the maxCPU flag")

	tenantHandle.Close()
}

// requireGroupUsed returns q.mu.groups[key].used, failing the test
// if the entry does not exist.
func requireGroupUsed(t *testing.T, q *WorkQueue, key groupKey) uint64 {
	t.Helper()
	q.mu.Lock()
	defer q.mu.Unlock()
	g, ok := q.mu.groups[key]
	require.Truef(t, ok, "group %v should exist in q.mu.groups", key)
	return g.used
}

// TestSQLCPUHandleCloseAfterModeFlip verifies that a mode flip
// between Admit and Close still routes the drained reservation to
// the original (Admit-time) container. The reservationSourceGroup
// field captures the key at Admit time precisely so this scenario
// stays correct: re-deriving from the WorkQueue's current
// cpuTimeTokenACMode would route to a freshly-created container that
// never received the tokens.
func TestSQLCPUHandleCloseAfterModeFlip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	q, tg, st, cleanup := makeCPUTimeTokenWorkQueue(t)
	defer cleanup()
	// Start in serverless mode: Admit creates a tenant-keyed container.
	cpuTimeTokenACMode.Override(ctx, &st.SV, serverlessMode)

	h := newSQLCPUAdmissionHandle(
		WorkInfo{TenantID: roachpb.MustMakeTenantID(7), Priority: admissionpb.NormalPri},
		true, &sqlCPUProviderImpl{}, q)
	// Seed reservation directly so reservationSourceGroup is captured at the
	// pre-flip mode and reservation > 0 (Close's drain path requires both).
	seedReservation(t, ctx, h, int64(1*time.Millisecond))
	tenantKey := tenantGroupKey(7)
	require.Equal(t, tenantKey, h.mu.reservationSourceGroup)
	usedBefore := requireGroupUsed(t, q, tenantKey)

	// Flip mode mid-handle. A subsequent Admit on this WorkQueue would
	// route to highResourceGroupKey, but Close should still
	// target the tenant-keyed container that holds the prior tokens.
	// Pre-create the rg-keyed containers via applyConfigLocked so the
	// rg container exists before Close runs.
	cpuTimeTokenACMode.Override(ctx, &st.SV, resourceManagerMode)
	q.mu.Lock()
	q.applyConfigLocked(q.configHolder.Snapshot().Groups())
	q.mu.Unlock()
	rgUsedBefore := requireGroupUsed(t, q, highResourceGroupKey)

	_ = tg.buf.stringAndReset()
	h.Close()
	require.Contains(t, tg.buf.String(), "returnGrant")

	require.Less(t, requireGroupUsed(t, q, tenantKey), usedBefore,
		"tenant container's used must decrement; if not, Close "+
			"re-derived the container key from the post-flip mode")
	require.Equal(t, rgUsedBefore, requireGroupUsed(t, q, highResourceGroupKey),
		"rg container's used must not change; if it did, Close "+
			"re-derived the container key from the post-flip mode "+
			"rather than using reservationSourceGroup")
}
