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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// requirePGCode asserts that err carries the given Postgres error code.
// Matching by code is more robust than substring-matching the error
// message, which can drift when wording is tweaked.
func requirePGCode(t *testing.T, err error, code pgcode.Code) {
	t.Helper()
	require.Error(t, err)
	var pqErr *pq.Error
	require.True(t, errors.As(err, &pqErr), "expected *pq.Error, got %T: %v", err, err)
	require.Equal(t, pq.ErrorCode(code.String()), pqErr.Code,
		"unexpected pgcode; full error: %v", err)
}

// waitForAdvisoryLockWaiter polls crdb_internal.cluster_locks until at
// least one non-granted lock on system.advisory_locks is visible. This
// proves a concurrent acquirer has actually parked in the lock-table
// waiter, which is far more reliable than a timing-based sleep when the
// test wants to assert blocking behavior.
func waitForAdvisoryLockWaiter(t *testing.T, ctx context.Context, db *gosql.DB) {
	t.Helper()
	testutils.SucceedsSoon(t, func() error {
		var count int
		if err := db.QueryRowContext(ctx,
			`SELECT count(*) FROM crdb_internal.cluster_locks
			 WHERE table_name = 'advisory_locks' AND granted = false`,
		).Scan(&count); err != nil {
			return err
		}
		if count == 0 {
			return errors.New("no advisory-lock waiter visible yet")
		}
		return nil
	})
}

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

// TestAdvisoryLockSQLBlockingUnblocks verifies that a transaction blocked
// on pg_advisory_xact_lock unblocks and proceeds once the holder commits.
// The existing deadlock tests exercise the wait path but always end in an
// error; this is the happy-path counterpart.
func TestAdvisoryLockSQLBlockingUnblocks(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	conn1, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn1.Close()) }()
	conn2, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn2.Close()) }()

	for _, c := range []*gosql.Conn{conn1, conn2} {
		_, err := c.ExecContext(ctx, `SET database = defaultdb`)
		require.NoError(t, err)
	}

	const key = 940101

	// conn1 holds the lock until we tell it to commit.
	_, err = conn1.ExecContext(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn1.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, key)
	require.NoError(t, err)

	// conn2 will block in the lock table until conn1 commits. We capture
	// the time at which conn2 returns so we can assert it finished only
	// after conn1 committed.
	type result struct {
		err    error
		doneAt time.Time
	}
	resCh := make(chan result, 1)

	go func() {
		var r result
		if _, err := conn2.ExecContext(ctx, `BEGIN`); err != nil {
			r.err = err
			r.doneAt = timeutil.Now()
			resCh <- r
			return
		}
		_, err := conn2.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, key)
		r.doneAt = timeutil.Now()
		if err != nil {
			r.err = err
			_, _ = conn2.ExecContext(context.Background(), `ROLLBACK`)
			resCh <- r
			return
		}
		_, r.err = conn2.ExecContext(ctx, `COMMIT`)
		resCh <- r
	}()

	// Wait until conn2 has actually parked in the lock-table waiter
	// rather than relying on a timing sleep.
	waitForAdvisoryLockWaiter(t, ctx, db)
	releaseAt := timeutil.Now()
	_, err = conn1.ExecContext(ctx, `COMMIT`)
	require.NoError(t, err)

	r := <-resCh
	require.NoError(t, r.err)
	// conn2 must finish *after* conn1 released, which is the proof that
	// it observed the release rather than racing past.
	require.True(t, r.doneAt.After(releaseAt),
		"conn2 finished at %s but conn1 only committed at %s", r.doneAt, releaseAt)
}

// TestAdvisoryLockVersionGate verifies that all transaction-level advisory
// lock builtins return FeatureNotSupported when the cluster has not yet
// finalized the upgrade to the version that introduced the
// system.advisory_locks table.
func TestAdvisoryLockVersionGate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Pin the cluster to a released version older than the gate. The test
	// infrastructure can only bootstrap at versions with initial values —
	// typically prior releases — so an in-flight internal version of the
	// gated cluster version is not a valid bootstrap target. Any released
	// version older than the gate exercises the gated-off path.
	preGate := clusterversion.PreviousRelease
	st := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.Latest.Version(),
		preGate.Version(),
		false, /* initializeVersion */
	)
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: st,
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				ClusterVersionOverride:         preGate.Version(),
				DisableAutomaticVersionUpgrade: make(chan struct{}),
			},
			SQLEvalContext: &eval.TestingKnobs{
				TenantLogicalVersionKeyOverride: preGate,
			},
		},
	})
	defer srv.Stopper().Stop(ctx)

	_, err := db.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)

	tests := []struct {
		name string
		stmt string
	}{
		{name: "xact_lock_int8", stmt: `SELECT pg_advisory_xact_lock(1)`},
		{name: "xact_lock_int4_pair", stmt: `SELECT pg_advisory_xact_lock(1::INT4, 2::INT4)`},
		{name: "xact_lock_shared_int8", stmt: `SELECT pg_advisory_xact_lock_shared(1)`},
		{name: "xact_lock_shared_int4_pair", stmt: `SELECT pg_advisory_xact_lock_shared(1::INT4, 2::INT4)`},
		{name: "try_xact_lock_int8", stmt: `SELECT pg_try_advisory_xact_lock(1)`},
		{name: "try_xact_lock_int4_pair", stmt: `SELECT pg_try_advisory_xact_lock(1::INT4, 2::INT4)`},
		{name: "try_xact_lock_shared_int8", stmt: `SELECT pg_try_advisory_xact_lock_shared(1)`},
		{name: "try_xact_lock_shared_int4_pair", stmt: `SELECT pg_try_advisory_xact_lock_shared(1::INT4, 2::INT4)`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := db.ExecContext(ctx, tc.stmt)
			requirePGCode(t, err, pgcode.FeatureNotSupported)
		})
	}
}

// TestAdvisoryLockStatementTimeout verifies that a session blocked on
// pg_advisory_xact_lock fails cleanly when its statement_timeout fires,
// the session remains usable for further queries, and no waiter leaks
// in the lock table after the holder commits.
func TestAdvisoryLockStatementTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	conn1, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn1.Close()) }()
	conn2, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn2.Close()) }()

	for _, c := range []*gosql.Conn{conn1, conn2} {
		_, err := c.ExecContext(ctx, `SET database = defaultdb`)
		require.NoError(t, err)
	}

	const key = 940201

	// conn1 holds the lock.
	_, err = conn1.ExecContext(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn1.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, key)
	require.NoError(t, err)

	// conn2 sets a statement_timeout and tries to acquire; expect a
	// query-canceled error. The timeout is generous enough that a slow
	// CI environment still has time to install the lock-table waiter
	// before the timer fires.
	_, err = conn2.ExecContext(ctx, `SET statement_timeout = '2s'`)
	require.NoError(t, err)
	_, err = conn2.ExecContext(ctx, `BEGIN`)
	require.NoError(t, err)
	_, err = conn2.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, key)
	requirePGCode(t, err, pgcode.QueryCanceled)

	// conn2 must still be usable. The txn is in an error state, so roll back
	// and run a sanity query.
	_, err = conn2.ExecContext(ctx, `ROLLBACK`)
	require.NoError(t, err)
	_, err = conn2.ExecContext(ctx, `RESET statement_timeout`)
	require.NoError(t, err)
	var one int
	require.NoError(t, conn2.QueryRowContext(ctx, `SELECT 1`).Scan(&one))
	require.Equal(t, 1, one)

	// Releasing the holder's lock must let a fresh session acquire — i.e.
	// the cancelled waiter on conn2 was not left behind.
	_, err = conn1.ExecContext(ctx, `COMMIT`)
	require.NoError(t, err)
	conn3, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn3.Close()) }()
	_, err = conn3.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)
	_, err = conn3.ExecContext(ctx, `BEGIN`)
	require.NoError(t, err)
	defer func() { _, _ = conn3.ExecContext(context.Background(), `ROLLBACK`) }()
	var got bool
	require.NoError(t, conn3.QueryRowContext(ctx,
		`SELECT pg_try_advisory_xact_lock($1)`, key).Scan(&got))
	require.True(t, got, "lock should be acquirable after holder commits")
	_, err = conn3.ExecContext(ctx, `COMMIT`)
	require.NoError(t, err)
}

// TestAdvisoryLockSQLCrossIsolationWait verifies that a blocked advisory
// lock acquisition unblocks across isolation levels — i.e. an RC waiter
// completes after an SR holder commits, and vice versa. This complements
// TestAdvisoryLockSQLDeadlockForUpdateCrossAdvisory, which exercises the
// cycle path; this test exercises the linear wait path.
func TestAdvisoryLockSQLCrossIsolationWait(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())

	hostRunner := sqlutils.MakeSQLRunner(srv.ApplicationLayer().SQLConn(t))
	// Disable write buffering so RC lock conflicts surface synchronously
	// rather than being deferred to commit (matches the existing cross-
	// isolation deadlock test).
	hostRunner.Exec(t, "SET CLUSTER SETTING kv.transaction.write_buffering.enabled = false")
	hostRunner.Exec(t, "SET CLUSTER SETTING sql.txn.write_buffering_for_weak_isolation.enabled = false")

	cases := []struct {
		name      string
		holderIso string
		waiterIso string
		key       int64
	}{
		{name: "rc_holds_sr_waits", holderIso: "read committed", waiterIso: "serializable", key: 940301},
		{name: "sr_holds_rc_waits", holderIso: "serializable", waiterIso: "read committed", key: 940302},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			holder, err := db.Conn(ctx)
			require.NoError(t, err)
			defer func() { require.NoError(t, holder.Close()) }()
			waiter, err := db.Conn(ctx)
			require.NoError(t, err)
			defer func() { require.NoError(t, waiter.Close()) }()

			for _, c := range []*gosql.Conn{holder, waiter} {
				_, err := c.ExecContext(ctx, `SET database = defaultdb`)
				require.NoError(t, err)
			}
			_, err = holder.ExecContext(ctx,
				`SET default_transaction_isolation = '`+tc.holderIso+`'`)
			require.NoError(t, err)
			_, err = waiter.ExecContext(ctx,
				`SET default_transaction_isolation = '`+tc.waiterIso+`'`)
			require.NoError(t, err)

			_, err = holder.ExecContext(ctx, `BEGIN`)
			require.NoError(t, err)
			_, err = holder.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, tc.key)
			require.NoError(t, err)

			type result struct {
				err    error
				doneAt time.Time
			}
			resCh := make(chan result, 1)
			go func() {
				var r result
				if _, err := waiter.ExecContext(ctx, `BEGIN`); err != nil {
					r.err = err
					r.doneAt = timeutil.Now()
					resCh <- r
					return
				}
				_, err := waiter.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, tc.key)
				r.doneAt = timeutil.Now()
				if err != nil {
					r.err = err
					_, _ = waiter.ExecContext(context.Background(), `ROLLBACK`)
					resCh <- r
					return
				}
				_, r.err = waiter.ExecContext(ctx, `COMMIT`)
				resCh <- r
			}()

			waitForAdvisoryLockWaiter(t, ctx, db)
			releaseAt := timeutil.Now()
			_, err = holder.ExecContext(ctx, `COMMIT`)
			require.NoError(t, err)

			r := <-resCh
			require.NoError(t, r.err, "waiter (%s) should succeed after holder (%s) commits",
				tc.waiterIso, tc.holderIso)
			require.True(t, r.doneAt.After(releaseAt),
				"waiter finished at %s but holder only committed at %s", r.doneAt, releaseAt)
		})
	}
}
