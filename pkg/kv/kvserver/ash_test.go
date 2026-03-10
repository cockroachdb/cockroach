// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs/ash"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestASHLockContention verifies that lock contention between two
// transactions produces ASH samples with work_event_type = 'LOCK' and
// work_event = 'LockWait'.
func TestASHLockContention(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// ASH settings are system-level, so we need to run against the
	// system tenant directly.
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer srv.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Enable ASH with a short sample interval.
	runner.Exec(t, `SET CLUSTER SETTING obs.ash.enabled = true`)
	runner.Exec(t, `SET CLUSTER SETTING obs.ash.sample_interval = '10ms'`)

	// Wait for the setting to propagate and the sampler to start
	// producing samples.
	testutils.SucceedsSoon(t, func() error {
		samples := ash.GetSamples()
		if len(samples) == 0 {
			return errors.New("waiting for ASH to produce samples")
		}
		return nil
	})

	// Create a table and insert a row.
	runner.Exec(t, `CREATE TABLE t (k INT PRIMARY KEY, v INT)`)
	runner.Exec(t, `INSERT INTO t VALUES (1, 0)`)

	// txn1 holds a lock on the row.
	txn1, err := sqlDB.Begin()
	require.NoError(t, err)
	_, err = txn1.Exec(`UPDATE t SET v = 1 WHERE k = 1`)
	require.NoError(t, err)

	// txn2 tries to update the same row in a goroutine, blocking on
	// the lock held by txn1.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		// This will block until txn1 commits or rolls back.
		_, _ = sqlDB.Exec(`UPDATE t SET v = 2 WHERE k = 1`)
	}()

	// Wait until ASH captures at least one LockWait sample, then
	// commit txn1 to unblock txn2. Using SucceedsSoon avoids a
	// hard-coded sleep that could be flaky under load.
	testutils.SucceedsSoon(t, func() error {
		samples := ash.GetSamples()
		for _, s := range samples {
			if s.WorkEvent == "LockWait" && s.WorkEventType == ash.WorkLock {
				return nil
			}
		}
		return errors.Newf(
			"waiting for ASH LockWait sample, got %d total samples",
			len(samples))
	})

	// Commit txn1 to unblock txn2.
	require.NoError(t, txn1.Commit())
	wg.Wait()
}
