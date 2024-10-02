// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
)

// TestDeadlockDetection asserts that the session setting `deadlock_timeout`
// and the cluster setting `deadlock_detection_push_delay` work as expected in
// case there was a deadlock.
func TestDeadlockDetection(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Trace the query that will cause a deadlock to make sure that the expected
	// time has been waited before pushing the Txn.
	traceQuery := `UPDATE test2 SET age = 1 WHERE id = 1`
	recCh := make(chan tracingpb.Recording, 1)
	knobs := base.TestingKnobs{
		SQLExecutor: &sql.ExecutorTestingKnobs{
			WithStatementTrace: func(trace tracingpb.Recording, stmt string) {
				if stmt == traceQuery {
					recCh <- trace
				}
			},
		},
	}

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{Knobs: knobs})
	ctx := context.Background()
	defer srv.Stopper().Stop(ctx)
	sqlr := sqlutils.MakeSQLRunner(db)

	sqlr.Exec(t, "CREATE TABLE test1 (id int, age int)")
	sqlr.Exec(t, "CREATE TABLE test2 (id int, age int)")
	sqlr.Exec(t, "INSERT INTO test1 (id, age) VALUES (1, 0)")
	sqlr.Exec(t, "INSERT INTO test2 (id, age) VALUES (1, 0)")

	for _, tc := range []struct {
		clusterDeadlockPushDelay  time.Duration
		sessionDeadlockTimeout    time.Duration
		expectedDeadlockPushDelay time.Duration
	}{
		{
			clusterDeadlockPushDelay: time.Millisecond * 25,
			sessionDeadlockTimeout:   0,
			// If the session deadlock timeout is not set, expect that the cluster
			// setting is the timeout that will be used.
			expectedDeadlockPushDelay: time.Millisecond * 25,
		},
		{
			clusterDeadlockPushDelay: time.Hour * 24,
			sessionDeadlockTimeout:   time.Millisecond * 25,
			// If the session deadlock timeout is set, it will be used regardless of
			// the cluster setting.
			expectedDeadlockPushDelay: time.Millisecond * 25,
		},
	} {

		concurrency.LockTableDeadlockOrLivenessDetectionPushDelay.Override(ctx,
			&srv.SystemLayer().ClusterSettings().SV, tc.clusterDeadlockPushDelay)

		// The tx1 writes to the first key, and tx2 writes to the second key.
		// After that, tx1 tries to write to the second key, and tx2 tries to write
		// to the first key. This should cause a deadlock condition.
		tx1 := sqlr.Begin(t)

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

		tx2 := sqlr.Begin(t)

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
		_, tx1Err := tx1.Exec("UPDATE test2 SET age = 1 WHERE id = 1")
		wg.Wait()
		rec := <-recCh

		// At this point, the deadlock condition is resolved, and one of the two
		// transactions received a TransactionRetry error.
		if tx1Err != nil {
			require.NoError(t, tx2Err)
			require.Contains(t, tx1Err.Error(), "TransactionRetryWithProtoRefreshError")
		} else {
			require.NoError(t, tx1Err)
			require.Contains(t, tx2Err.Error(), "TransactionRetryWithProtoRefreshError")
		}

		require.NoError(t, tx1.Rollback())
		require.NoError(t, tx2.Rollback())
		require.True(t, strings.Contains(rec.String(),
			"pushing after "+tc.expectedDeadlockPushDelay.String()))
	}
}
