// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pgadvisory_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestSession(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	conn1, err := tc.Conns[0].Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn1.Close() }()
	session1 := sqlutils.MakeSQLRunner(conn1)
	conn2, err := tc.Conns[0].Conn(ctx)
	require.NoError(t, err)
	defer func() { _ = conn2.Close() }()
	session2 := sqlutils.MakeSQLRunner(conn2)

	const (
		begin    = "BEGIN;"
		commit   = "COMMIT;"
		rollback = "ROLLBACK"

		txnLock             = "SELECT pg_advisory_xact_lock(1);"
		txnLockShared       = "SELECT pg_advisory_xact_lock_shared(1);"
		sessionLock         = "SELECT pg_advisory_lock(1);"
		sessionLockShared   = "SELECT pg_advisory_lock_shared(1);"
		sessionUnlock       = "SELECT pg_advisory_unlock(1);"
		sessionUnlockShared = "SELECT pg_advisory_unlock_shared(1);"

		uncommitted = 0
		committed   = 1
		rolledBack  = 1
		unlocked    = 0
		locked      = 1

		sleepTime = time.Second / 20
	)

	sleep := func() {
		select {
		case <-time.After(sleepTime):
		}
	}

	t.Run("concurrent txns block on txn-scoped exclusive lock", func(t *testing.T) {
		var wg sync.WaitGroup
		session1.Exec(t, begin)
		session2.Exec(t, begin)
		session1.Exec(t, txnLock)
		var txn1Committed int32
		go func() {
			session2.Exec(t, txnLock)
			require.True(t, atomic.LoadInt32(&txn1Committed) == committed, "first txn should have committed")
			session2.Exec(t, commit)
			wg.Done()
		}()
		wg.Add(1)
		sleep()
		atomic.StoreInt32(&txn1Committed, committed)
		session1.Exec(t, commit)
		wg.Wait()
	})

	t.Run("txn proceeds after rollback of another txn holding the exclusive lock", func(t *testing.T) {
		var wg sync.WaitGroup
		session1.Exec(t, begin)
		session2.Exec(t, begin)
		session1.Exec(t, txnLock)
		var txn1RolledBack int32
		go func() {
			session2.Exec(t, txnLock)
			require.True(t, atomic.LoadInt32(&txn1RolledBack) == rolledBack, "first txn should have rolled back")
			session2.Exec(t, commit)
			wg.Done()
		}()
		wg.Add(1)
		sleep()
		atomic.StoreInt32(&txn1RolledBack, rolledBack)
		session1.Exec(t, rollback)
		wg.Wait()
	})

	t.Run("concurrent txns do *not* block on txn-scoped shared lock", func(t *testing.T) {
		var wg sync.WaitGroup
		session1.Exec(t, begin)
		session2.Exec(t, begin)
		session1.Exec(t, txnLockShared)
		var txn1Committed int32
		go func() {
			session2.Exec(t, txnLockShared)
			require.True(t, atomic.LoadInt32(&txn1Committed) == uncommitted, "first txn should have NOT committed")
			session2.Exec(t, commit)
			wg.Done()
		}()
		wg.Add(1)
		sleep()
		atomic.StoreInt32(&txn1Committed, committed)
		session1.Exec(t, commit)
		wg.Wait()
	})

	t.Run("concurrent sessions block on session-scoped exclusive lock", func(t *testing.T) {
		var wg sync.WaitGroup
		var session1Locked int32
		atomic.StoreInt32(&session1Locked, locked)
		session1.Exec(t, sessionLock)
		go func() {
			session2.Exec(t, sessionLock)
			require.True(t, atomic.LoadInt32(&session1Locked) == unlocked, "first session should have unlocked")
			session2.Exec(t, sessionUnlock)
			wg.Done()
		}()
		wg.Add(1)
		sleep()
		atomic.StoreInt32(&session1Locked, unlocked)
		session1.Exec(t, sessionUnlock)
		wg.Wait()
	})

	t.Run("concurrent sessions do *not* block on session-scoped shared lock", func(t *testing.T) {
		var wg sync.WaitGroup
		var session1Locked int32
		atomic.StoreInt32(&session1Locked, locked)
		session1.Exec(t, sessionLockShared)
		go func() {
			session2.Exec(t, sessionLockShared)
			require.True(t, atomic.LoadInt32(&session1Locked) == locked, "first session should have NOT unlocked")
			session2.Exec(t, sessionUnlockShared)
			wg.Done()
		}()
		wg.Add(1)
		sleep()
		atomic.StoreInt32(&session1Locked, unlocked)
		session1.Exec(t, sessionUnlockShared)
		wg.Wait()
	})

	t.Run("no mixed-scope locking in the same session", func(t *testing.T) {
		session1.Exec(t, sessionLock)
		session1.Exec(t, begin)
		session1.ExpectErr(t, ".*", txnLock)
		session1.Exec(t, rollback)
		session1.Exec(t, sessionUnlock)

		session2.Exec(t, begin)
		session2.Exec(t, txnLockShared)
		session2.ExpectErr(t, ".*", sessionLockShared)
		session2.Exec(t, commit)
	})

	t.Run("concurrent sessions block on mixed-scoped exclusive lock", func(t *testing.T) {
		var wg sync.WaitGroup
		var session1Locked int32
		atomic.StoreInt32(&session1Locked, locked)
		session1.Exec(t, sessionLock)
		go func() {
			session2.Exec(t, begin)
			session2.Exec(t, txnLock)
			require.True(t, atomic.LoadInt32(&session1Locked) == unlocked, "first session should have unlocked")
			session2.Exec(t, commit)
			wg.Done()
		}()
		wg.Add(1)
		sleep()
		atomic.StoreInt32(&session1Locked, unlocked)
		session1.Exec(t, sessionUnlock)
		wg.Wait()
	})

	t.Run("concurrent sessions do *not* block on mixed-scoped shared lock", func(t *testing.T) {
		var wg sync.WaitGroup
		var session1Locked int32
		atomic.StoreInt32(&session1Locked, locked)
		session1.Exec(t, sessionLockShared)
		go func() {
			session2.Exec(t, begin)
			session2.Exec(t, txnLockShared)
			require.True(t, atomic.LoadInt32(&session1Locked) == locked, "first session should have NOT unlocked")
			session2.Exec(t, commit)
			wg.Done()
		}()
		wg.Add(1)
		sleep()
		atomic.StoreInt32(&session1Locked, unlocked)
		session1.Exec(t, sessionUnlockShared)
		wg.Wait()
	})

	t.Run("unlock all session-scoped locks", func(t *testing.T) {
		var wg sync.WaitGroup
		var (
			session1Locked1 int32
			session1Locked2 int32
		)
		const (
			sessionLock2     = "SELECT pg_advisory_lock(2);"
			sessionUnlockAll = "SELECT pg_advisory_unlock_all();"
		)
		atomic.StoreInt32(&session1Locked1, locked)
		session1.Exec(t, sessionLock)
		atomic.StoreInt32(&session1Locked2, locked)
		session1.Exec(t, sessionLock2)
		go func() {
			session2.Exec(t, sessionLock)
			require.True(t, atomic.LoadInt32(&session1Locked1) == unlocked, "first session should have unlocked 1")
			session2.Exec(t, sessionLock2)
			require.True(t, atomic.LoadInt32(&session1Locked2) == unlocked, "first session should have unlocked 2")
			session2.Exec(t, sessionUnlockAll)
			wg.Done()
		}()
		wg.Add(1)
		sleep()
		atomic.StoreInt32(&session1Locked1, unlocked)
		atomic.StoreInt32(&session1Locked2, unlocked)
		session1.Exec(t, sessionUnlockAll)
		wg.Wait()
	})

	t.Run("force retry with session lock", func(t *testing.T) {
		session1.Exec(t, sessionLock)
		session1.Exec(t, "SELECT crdb_internal.force_retry('1s':::INTERVAL), pg_advisory_lock(1);")
		session1.Exec(t, sessionUnlock)
		session1.Exec(t, sessionUnlock)
		session2.Exec(t, sessionLock)
		session2.Exec(t, sessionUnlock)

		session1.Exec(t, "SELECT crdb_internal.force_retry('1s':::INTERVAL), pg_advisory_lock(1);")
		session1.Exec(t, sessionUnlock)
		session2.Exec(t, sessionLock)
		session2.Exec(t, sessionUnlock)
	})
}
