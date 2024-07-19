// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestDeadlockDetection asserts that the session setting `deadlock_timeout`
// and the cluster setting `deadlock_detection_push_delay` work as expected in
// case there was a deadlock.
func TestDeadlockDetection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer srv.Stopper().Stop(ctx)
	sql0 := sqlutils.MakeSQLRunner(db)

	sql0.Exec(t, "CREATE TABLE test1 (id int, age int)")
	sql0.Exec(t, "CREATE TABLE test2 (id int, age int)")
	sql0.Exec(t, "INSERT INTO test1 (id, age) VALUES (1, 0)")
	sql0.Exec(t, "INSERT INTO test2 (id, age) VALUES (1, 0)")

	for _, tc := range []struct {
		clusterDeadlockPushDelay     time.Duration
		sessionDeadlockTimeout       time.Duration
		minDeadlockDetectionDuration time.Duration
	}{
		{
			clusterDeadlockPushDelay: time.Millisecond * 25,
			sessionDeadlockTimeout:   0,
			// If the session deadlock timeout is not set, expect that the cluster
			// setting is the timeout that will be used.
			minDeadlockDetectionDuration: time.Millisecond * 25,
		},
		{
			clusterDeadlockPushDelay: time.Hour * 24,
			sessionDeadlockTimeout:   time.Millisecond * 25,
			// If the session deadlock timeout is set, it will be used regardless of
			// the cluster setting.
			minDeadlockDetectionDuration: time.Millisecond * 25,
		},
	} {

		concurrency.LockTableDeadlockOrLivenessDetectionPushDelay.Override(ctx,
			&srv.SystemLayer().ClusterSettings().SV, tc.clusterDeadlockPushDelay)

		// The tx1 writes to the first key, and tx2 writes to the second key.
		// After that, tx1 tries to write to the second key, and tx2 tries to write
		// to the first key. This should cause a deadlock condition.
		tx1 := sql0.Begin(t)

		// Skip setting the session variable if tc.sessionDeadlockTimeout is 0 to
		// ensure that the default value works as intended (the session variable
		// gets ignored).
		if tc.sessionDeadlockTimeout != 0 {
			_, err := tx1.Exec(fmt.Sprintf("SET deadlock_timeout =%d",
				tc.sessionDeadlockTimeout.Milliseconds()))
			require.NoError(t, err)
		}

		_, err := tx1.Exec("UPDATE test1 SET age = 1 WHERE id = 1")
		require.NoError(t, err)

		tx2 := sql0.Begin(t)

		if tc.sessionDeadlockTimeout != 0 {
			_, err = tx2.Exec(fmt.Sprintf("SET deadlock_timeout =%d",
				tc.sessionDeadlockTimeout.Milliseconds()))
			require.NoError(t, err)
		}

		_, err = tx2.Exec("UPDATE test2 SET age = 2 WHERE id = 1")
		require.NoError(t, err)

		// Let tx2 attempt to update the first key.
		var tx2Err error
		wg := &sync.WaitGroup{}
		wg.Add(1)
		go func() {
			_, tx2Err = tx2.Exec("UPDATE test1 SET age = 2 WHERE id = 1")
			wg.Done()
		}()

		// After executing the next operation, a deadlock is created.  Measure the
		// time it takes for the deadlock condition to be broken.
		t1 := time.Now()
		_, tx1Err := tx1.Exec("UPDATE test2 SET age = 1 WHERE id = 1")
		t2 := time.Now()
		wg.Wait()

		// At this point, the deadlock condition is resolved, and one of the two
		// transactions received a TransactionRetry error.
		if tx1Err != nil {
			require.NoError(t, tx2Err)
			require.Contains(t, tx1Err.Error(), "TransactionRetryWithProtoRefreshError")
		} else {
			require.NoError(t, tx1Err)
			require.Contains(t, tx2Err.Error(), "TransactionRetryWithProtoRefreshError")
		}

		require.GreaterOrEqual(t, t2.Sub(t1), tc.minDeadlockDetectionDuration)

		require.NoError(t, tx1.Rollback())
		require.NoError(t, tx2.Rollback())
	}
}
