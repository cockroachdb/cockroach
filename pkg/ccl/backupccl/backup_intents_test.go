// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestCleanupIntentsDuringBackupPerformanceRegression(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()

	skip.UnderRace(t, "measures backup times not to regress, can't work under race")

	// Time to create backup in presence of intents differs roughly 10x so some
	// arbitrary number is picked which is 2x higher than current backup time on
	// current (laptop) hardware.
	const backupTimeout = time.Second * 20

	const totalRowCount = 10000
	const perTransactionRowCount = 10

	// Interceptor catches requests that cleanup transactions of size 1000 which are
	// test data transactions. All other transaction commits pass though.
	interceptor := func(ctx context.Context, req roachpb.BatchRequest) *roachpb.Error {
		endTxn := req.Requests[0].GetEndTxn()
		if endTxn != nil && !endTxn.Commit && len(endTxn.LockSpans) == perTransactionRowCount {
			// If this is a rollback of one the test's SQL transactions, allow the
			// EndTxn to proceed and mark the transaction record as ABORTED, but strip
			// the request of its lock spans so that no intents are recorded into the
			// transaction record or eagerly resolved. This is a bit of a hack, but it
			// mimics the behavior of an abandoned transaction which is aborted by a
			// pusher after expiring due to an absence of heartbeats.
			endTxn.LockSpans = nil
		}
		return nil
	}
	serverKnobs := kvserver.StoreTestingKnobs{TestingRequestFilter: interceptor}

	s, sqlDb, _ := serverutils.StartServer(t,
		base.TestServerArgs{Knobs: base.TestingKnobs{Store: &serverKnobs}})
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDb.Exec("create table foo(v int not null)")
	require.NoError(t, err)

	for i := 0; i < totalRowCount; i += perTransactionRowCount {
		tx, err := sqlDb.Begin()
		require.NoError(t, err)
		for j := 0; j < perTransactionRowCount; j += 1 {
			statement := fmt.Sprintf("insert into foo (v) values (%d)", i+j)
			_, err = tx.Exec(statement)
			require.NoError(t, err)
		}
		require.NoError(t, tx.Rollback())
	}

	start := timeutil.Now()
	_, err = sqlDb.Exec("backup table foo to 'userfile:///test.foo'")
	stop := timeutil.Now()
	require.NoError(t, err, "Failed to run backup")
	t.Logf("Backup took %s", stop.Sub(start))
	require.WithinDuration(t, stop, start, backupTimeout, "Time to make backup")
}
