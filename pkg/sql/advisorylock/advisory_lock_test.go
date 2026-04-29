// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package advisorylock_test

import (
	"context"
	gosql "database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// assertLockCycleBrokenError checks the error returned when the lock table breaks a
// wait cycle (pure advisory deadlock, or row lock vs advisory). The losing transaction
// should surface a transaction retry error, matching TestDeadlockDetection.
func assertLockCycleBrokenError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.Contains(t, err.Error(), "TransactionRetryWithProtoRefreshError",
		"expected lock-cycle resolution to surface as a transaction retry error, got: %v", err)
}

// finishAdvisoryDeadlockTxn captures the error surfaced when the deadlock
// cycle is broken. The losing side's failure can show up either on the
// second pg_advisory_xact_lock call or at COMMIT — KV deadlock detection
// force-aborts the victim, but the abort may only become visible to the
// client at commit time if the lock acquisition slips past the abort (e.g.
// because the lock holder released the lock before the abort propagated).
// This helper tries COMMIT when the lock acquire returned nil so either
// failure mode is captured on errCh.
func finishAdvisoryDeadlockTxn(ctx context.Context, conn *gosql.Conn, lockErr error) error {
	if lockErr != nil {
		return lockErr
	}
	_, err := conn.ExecContext(ctx, `COMMIT`)
	return err
}

// TestAdvisoryLockSQLAcquireRelease verifies locks are released on commit so a
// later transaction can acquire the same key.
func TestAdvisoryLockSQLAcquireRelease(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	_, err := db.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)

	const key = 910002

	for i := 0; i < 3; i++ {
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, key)
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	var got bool
	err = tx.QueryRowContext(ctx, `SELECT pg_try_advisory_xact_lock($1)`, key).Scan(&got)
	require.NoError(t, err)
	require.True(t, got)
	require.NoError(t, tx.Commit())
}

// TestAdvisoryLockSQLConcurrency runs many transactions that contend on
// one exclusive transaction lock, at most one should hold it at a time.
func TestAdvisoryLockSQLConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	_, err := db.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)

	const key = 910003
	const workers = 32
	const iters = 64

	var held, maxHeld atomic.Int32

	g := ctxgroup.WithContext(ctx)
	for w := 0; w < workers; w++ {
		g.GoCtx(func(ctx context.Context) error {
			for i := 0; i < iters; i++ {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, key); err != nil {
					_ = tx.Rollback()
					return err
				}
				cur := held.Add(1)
				for {
					m := maxHeld.Load()
					if cur <= m {
						break
					}
					if maxHeld.CompareAndSwap(m, cur) {
						break
					}
				}
				held.Add(-1)
				if err := tx.Commit(); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())
	require.Equal(t, int32(1), maxHeld.Load(),
		"exclusive xact advisory lock should never allow more than one holder at a time")
}

// TestAdvisoryLockSQLSharedConcurrency runs concurrent transactions that each
// take the same shared transaction advisory lock, many may hold it at once.
func TestAdvisoryLockSQLSharedConcurrency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	_, err := db.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)

	const key = 910004
	const workers = 8
	const iters = 25

	var held, maxHeld atomic.Int32

	g := ctxgroup.WithContext(ctx)
	for w := 0; w < workers; w++ {
		g.GoCtx(func(ctx context.Context) error {
			for i := 0; i < iters; i++ {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					return err
				}
				if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock_shared($1)`, key); err != nil {
					_ = tx.Rollback()
					return err
				}
				cur := held.Add(1)
				for {
					m := maxHeld.Load()
					if cur <= m {
						break
					}
					if maxHeld.CompareAndSwap(m, cur) {
						break
					}
				}
				time.Sleep(2 * time.Millisecond)
				held.Add(-1)
				if err := tx.Commit(); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(t, g.Wait())
	require.Greater(t, maxHeld.Load(), int32(1),
		"shared xact advisory lock should allow multiple holders concurrently")
}

// TestAdvisoryLockSQLDeadlock uses two dedicated connections to
// build a lock cycle, CockroachDB should break the deadlock (one txn errors).
func TestAdvisoryLockSQLDeadlock(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	concurrency.LockTableDeadlockOrLivenessDetectionPushDelay.Override(
		ctx, &srv.SystemLayer().ClusterSettings().SV, 10*time.Millisecond)

	conn1, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn1.Close()) }()
	conn2, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn2.Close()) }()

	for _, c := range []*gosql.Conn{conn1, conn2} {
		_, err := c.ExecContext(ctx, `SET database = defaultdb`)
		require.NoError(t, err)
		_, err = c.ExecContext(ctx, `SET deadlock_timeout = '100ms'`)
		require.NoError(t, err)
	}

	const k1, k2 = 920001, 920002

	start2 := make(chan struct{})
	proceed1 := make(chan struct{})

	errCh := make(chan error, 2)

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		if _, err := conn1.ExecContext(ctx, `BEGIN`); err != nil {
			errCh <- err
			return nil //nolint:returnerrcheck // errors are collected on errCh; errgroup must not short-circuit.
		}
		defer func() { _, _ = conn1.ExecContext(context.Background(), `ROLLBACK`) }()
		if _, err := conn1.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, k1); err != nil {
			return err
		}
		select {
		case start2 <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		select {
		case <-proceed1:
		case <-ctx.Done():
			return ctx.Err()
		}
		_, err := conn1.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, k2)
		// Note: The final error cannot be returned here, since it will cancel the context
		// if something failed.
		errCh <- finishAdvisoryDeadlockTxn(ctx, conn1, err)
		return nil
	})

	g.GoCtx(func(ctx context.Context) error {
		select {
		case <-start2:
		case <-ctx.Done():
			errCh <- ctx.Err()
			return nil
		}
		if _, err := conn2.ExecContext(ctx, `BEGIN`); err != nil {
			return err
		}
		defer func() { _, _ = conn2.ExecContext(context.Background(), `ROLLBACK`) }()
		if _, err := conn2.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, k2); err != nil {
			return err
		}
		select {
		case proceed1 <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		_, err := conn2.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, k1)
		errCh <- finishAdvisoryDeadlockTxn(ctx, conn2, err)
		return nil
	})

	require.NoError(t, g.Wait())
	errs := []error{<-errCh, <-errCh}
	var sawBreakingErr bool
	for _, err := range errs {
		if err == nil {
			continue
		}
		sawBreakingErr = true
		t.Logf("advisory-only deadlock error: %v", err)
		assertLockCycleBrokenError(t, err)
	}
	require.True(t, sawBreakingErr,
		"expected at least one failing statement or commit when breaking a lock cycle, got %#v", errs)
}

// TestAdvisoryLockSQLDeadlockForUpdateCrossAdvisory builds a wait cycle between a
// row lock (SELECT FOR UPDATE) and a transaction advisory lock: one connection
// holds the row and waits on the advisory key; the other holds the advisory key
// and waits on the row. The lock table should detect and break the deadlock.
func TestAdvisoryLockSQLDeadlockForUpdateCrossAdvisory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	hostRunner := sqlutils.MakeSQLRunner(srv.ApplicationLayer().SQLConn(t))
	// Stabilize lock behavior under metamorphic builds by disabling buffered writes.
	hostRunner.Exec(t, "SET CLUSTER SETTING kv.transaction.write_buffering.enabled = false")
	// Read committed uses a separate write-buffer path; metamorphic tests can enable
	// sql.txn.write_buffering_for_weak_isolation.enabled and mask lock-cycle errors.
	hostRunner.Exec(t, "SET CLUSTER SETTING sql.txn.write_buffering_for_weak_isolation.enabled = false")
	concurrency.LockTableDeadlockOrLivenessDetectionPushDelay.Override(
		ctx, &srv.SystemLayer().ClusterSettings().SV, 10*time.Millisecond)

	_, err := db.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `CREATE TABLE dead_adv_row (id INT PRIMARY KEY)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO dead_adv_row VALUES (1)`)
	require.NoError(t, err)

	conn1, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn1.Close()) }()
	conn2, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn2.Close()) }()

	for _, c := range []*gosql.Conn{conn1, conn2} {
		_, err := c.ExecContext(ctx, `SET database = defaultdb`)
		require.NoError(t, err)
		_, err = c.ExecContext(ctx, `SET deadlock_timeout = '100ms'`)
		require.NoError(t, err)
		// Read committed avoids the serializable post-statement forced-retry path in
		// conn_executor (IsSerializablePushAndRefreshNotPossible), which can rewind and
		// replay an explicit txn after FOR UPDATE and mask the lock-cycle retry error.
		_, err = c.ExecContext(ctx, `SET default_transaction_isolation = 'read committed'`)
		require.NoError(t, err)
	}

	const advKey = 930501

	// g1HasRow is closed after conn1 holds FOR UPDATE on the row.
	// g2HasAdv is closed after conn2 holds the transaction advisory lock.
	g1HasRow := make(chan struct{})
	g2HasAdv := make(chan struct{})

	errCh := make(chan error, 2)

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		if _, err := conn1.ExecContext(ctx, `BEGIN`); err != nil {
			return err
		}
		defer func() { _, _ = conn1.ExecContext(context.Background(), `ROLLBACK`) }()
		if _, err := conn1.ExecContext(ctx,
			`SELECT * FROM dead_adv_row WHERE id = 1 FOR UPDATE`); err != nil {
			return err
		}
		close(g1HasRow)
		select {
		case <-g2HasAdv:
		case <-ctx.Done():
			return ctx.Err()
		}
		_, err := conn1.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, advKey)
		// Note: The final error cannot be returned here, since it will cancel the context
		// if something failed.
		errCh <- finishAdvisoryDeadlockTxn(ctx, conn1, err)
		return nil
	})

	g.GoCtx(func(ctx context.Context) error {
		select {
		case <-g1HasRow:
		case <-ctx.Done():
			return ctx.Err()
		}
		if _, err := conn2.ExecContext(ctx, `BEGIN`); err != nil {
			return err
		}
		defer func() { _, _ = conn2.ExecContext(context.Background(), `ROLLBACK`) }()
		if _, err := conn2.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, advKey); err != nil {
			return err
		}
		close(g2HasAdv)
		_, err := conn2.ExecContext(ctx, `SELECT * FROM dead_adv_row WHERE id = 1 FOR UPDATE`)
		errCh <- finishAdvisoryDeadlockTxn(ctx, conn2, err)
		return nil
	})

	require.NoError(t, g.Wait())
	errs := []error{<-errCh, <-errCh}
	var sawBreakingErr bool
	for _, err := range errs {
		if err == nil {
			continue
		}
		sawBreakingErr = true
		t.Logf("FOR UPDATE / advisory deadlock error: %v", err)
		assertLockCycleBrokenError(t, err)
	}
	require.True(t, sawBreakingErr,
		"expected at least one failing statement or commit when breaking FOR UPDATE vs advisory cycle, got %#v", errs)
}
