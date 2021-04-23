// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl_test

import (
	"context"
	"errors"
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

func TestBackupPerformanceRegression(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer utilccl.TestingEnableEnterprise()()

	skip.UnderRace(t, "measures backup times not to regress, can't work under race")

	const rowCount = 10000
	const transactionSize = 1000

	// Interceptor catches requests that cleanup transactions of size 1000 which are
	// test data transactions. All other transaction commits pass though.
	interceptor := func(ctx context.Context, req roachpb.BatchRequest) *roachpb.Error {
		if req.Txn == nil || req.Txn.Name != "sql txn" {
			return nil
		}
		endTxn := req.Requests[0].GetEndTxn()
		if endTxn == nil {
			return nil
		}
		if endTxn.InFlightWrites != nil {
			return nil
		}
		if len(endTxn.LockSpans) != transactionSize {
			return nil
		}
		return roachpb.NewError(errors.New("blocking intent cleanup for our transaction only"))
	}
	serverKnobs := kvserver.StoreTestingKnobs{TestingRequestFilter: interceptor}

	s, sqlDb, _ := serverutils.StartServer(t,
		base.TestServerArgs{Knobs: base.TestingKnobs{Store: &serverKnobs}})
	defer s.Stopper().Stop(context.Background())

	_, err := sqlDb.Exec("create table foo(v int)")
	require.NoError(t, err)

	for i := 0; i < rowCount; {
		tx, err := sqlDb.Begin()
		require.NoError(t, err)
		ps, err := tx.Prepare("insert into foo (v) values ($1)")
		require.NoError(t, err)
		for j := 0; j < transactionSize; j++ {
			_, err = ps.Exec(i)
			require.NoError(t, err)
			i++
		}
		require.NoError(t, tx.Commit())
	}

	start := timeutil.Now()
	_, err = sqlDb.Exec("backup table foo to 'userfile:///test.foo'")
	require.NoError(t, err, "Failed to run backup")
	// Time to cleanup intents differs roughly 10x so some arbitrary number is picked which is 2x higher
	// than current backup time on current hardware.
	require.WithinDuration(t, timeutil.Now(), start, time.Second*10, "Time to make backup")
}
