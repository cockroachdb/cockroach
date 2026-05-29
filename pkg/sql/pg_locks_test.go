// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestPGLocksAdvisorySingleKeyExclusive validates pg_locks for a single-int8
// transaction advisory key (classid=0, objid=key, objsubid=1) and that the
// row clears after commit.
func TestPGLocksAdvisorySingleKeyExclusive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	_, err := db.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)

	const k = 10001
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, k)
	require.NoError(t, err)

	var n int
	err = tx.QueryRowContext(ctx, `
		SELECT count(*) FROM pg_catalog.pg_locks
		WHERE locktype = 'advisory' AND granted AND classid = 0 AND objid = $1::oid AND objsubid = 1
		AND relation IS NULL AND page IS NULL AND NOT fastpath
	`, k).Scan(&n)
	require.NoError(t, err)
	require.Equal(t, 1, n, "expected one granted advisory row for the held lock")

	var mode string
	var fast bool
	var rel, vxid gosql.NullString
	var vtxn string
	err = tx.QueryRowContext(ctx, `
		SELECT mode, fastpath, relation, virtualxid, virtualtransaction
		FROM pg_catalog.pg_locks
		WHERE locktype = 'advisory' AND granted AND objid = $1::oid
	`, k).Scan(&mode, &fast, &rel, &vxid, &vtxn)
	require.NoError(t, err)
	require.Equal(t, "ExclusiveLock", mode)
	require.False(t, fast)
	require.False(t, rel.Valid)  // NULL relation for advisory
	require.False(t, vxid.Valid) // no virtual xid
	require.Greater(t, len(vtxn), 2)
	require.Equal(t, "0/", vtxn[0:2], "virtualtransaction is like PostgreSQL '0/pid'")

	require.NoError(t, tx.Commit())

	err = db.QueryRowContext(ctx, `SELECT count(*)::int FROM pg_catalog.pg_locks WHERE objid = $1`, k).Scan(&n)
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

// TestPGLocksAdvisorySingleKeyShared matches ShareLock and objsubid 1.
func TestPGLocksAdvisorySingleKeyShared(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	_, err := db.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)

	const k = 10002
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock_shared($1)`, k)
	require.NoError(t, err)

	var mode string
	err = tx.QueryRowContext(ctx, `SELECT mode FROM pg_catalog.pg_locks WHERE locktype = 'advisory' AND granted AND objid = $1`, k).Scan(&mode)
	require.NoError(t, err)
	require.Equal(t, "ShareLock", mode)
	require.NoError(t, tx.Commit())
}

// TestPGLocksAdvisoryTwoInt4 maps to objsubid=2 with classid/objid as the two int4 parts.
func TestPGLocksAdvisoryTwoInt4(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	_, err := db.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(7::int4, 8::int4)`)
	require.NoError(t, err)

	var classid, objid int
	var subid int16
	err = tx.QueryRowContext(ctx, `
		SELECT classid::int, objid::int, objsubid::int
		FROM pg_catalog.pg_locks
		WHERE locktype = 'advisory' AND granted AND objsubid = 2
	`).Scan(&classid, &objid, &subid)
	require.NoError(t, err)
	require.Equal(t, 7, classid)
	require.Equal(t, 8, objid)
	require.Equal(t, int16(2), subid)
	require.NoError(t, tx.Commit())
}

// TestPGLocksAdvisoryNotGrantedWaiting asserts pg_locks exposes a non-granted
// row for a second transaction blocked on pg_advisory_xact_lock, using the
// crdb_internal.cluster_locks leg of the view (granted = false, joined to
// cluster_sessions for the waiting session).
func TestPGLocksAdvisoryNotGrantedWaiting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant})
	defer s.Stopper().Stop(ctx)

	_, err := db.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)

	conn1, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn1.Close()) }()
	conn2, err := db.Conn(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn2.Close()) }()
	_, err = conn1.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)
	_, err = conn2.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)

	const k = 888777666
	tx1, err := conn1.BeginTx(ctx, &gosql.TxOptions{})
	require.NoError(t, err)
	_, err = tx1.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, k)
	require.NoError(t, err)
	defer func() {
		if tx1 == nil {
			return
		}
		_ = tx1.Rollback()
	}()

	// Conn2 blocks on the same exclusive transaction lock until conn1 rolls back.
	errCh := make(chan error, 1)
	go func() {
		tx2, werr := conn2.BeginTx(ctx, &gosql.TxOptions{})
		if werr != nil {
			errCh <- werr
			return
		}
		defer func() {
			_ = tx2.Rollback()
		}()
		// Blocks while conn1 holds the lock.
		_, werr = tx2.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, k)
		errCh <- werr
	}()
	// Give the waiter time to start blocking before we poll.
	time.Sleep(50 * time.Millisecond)

	// Wait for a non-granted advisory row visible from pg_locks.
	var n int
	deadline := time.After(30 * time.Second)
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
waitLoop:
	for {
		select {
		case <-deadline:
			require.FailNowf(t, "timed out waiting for a non-granted pg_locks row for the blocked session", "")
		case <-tick.C:
			err = db.QueryRowContext(ctx, `
				SELECT count(*)
				FROM pg_catalog.pg_locks
				WHERE locktype = 'advisory'
				  AND NOT granted
				  AND classid::int8 = 0
				  AND objid::int8 = $1
				  AND objsubid = 1
				  AND mode = 'ExclusiveLock'
			`, k).Scan(&n)
			if err == nil && n >= 1 {
				break waitLoop
			}
			tick.Reset(20 * time.Millisecond)
		}
	}
	require.NoError(t, tx1.Rollback())
	tx1 = nil
	require.GreaterOrEqual(t, n, 1, "expected at least one waiting (non-granted) advisory row")

	select {
	case werr := <-errCh:
		require.NoError(t, werr, "second session should have acquired the lock after release")
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for conn2 to finish after holder released")
	}

	// Unblock path should have cleared waiters; allow a short settle.
	err = db.QueryRowContext(ctx, `SELECT count(*)::int FROM pg_catalog.pg_locks WHERE NOT granted`).Scan(&n)
	require.NoError(t, err)
	require.Equal(t, 0, n, "no non-granted rows once transactions complete")
}
