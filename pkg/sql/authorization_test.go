// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCheckAnyPrivilegeForNodeUser(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ts := serverutils.StartServerOnly(t, base.TestServerArgs{})

	defer ts.Stopper().Stop(ctx)

	require.NotNil(t, ts.InternalExecutor())

	ief := ts.InternalDB().(descs.DB)

	if err := ief.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {

		row, err := txn.QueryRowEx(
			ctx, "get-all-databases", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			"SELECT count(1) FROM crdb_internal.databases",
		)
		require.NoError(t, err)
		// 3 databases (system, defaultdb, postgres).
		require.Equal(t, row.String(), "(3)")

		_, err = txn.ExecEx(ctx, "create-database1", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			"CREATE DATABASE test1")
		require.NoError(t, err)

		_, err = txn.ExecEx(ctx, "create-database2", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			"CREATE DATABASE test2")
		require.NoError(t, err)

		// Revoke CONNECT on all non-system databases and ensure that when querying
		// with node, we can still see all the databases.
		_, err = txn.ExecEx(ctx, "revoke-privileges", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			"REVOKE CONNECT ON DATABASE test1 FROM public")
		require.NoError(t, err)
		_, err = txn.ExecEx(ctx, "revoke-privileges", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			"REVOKE CONNECT ON DATABASE test2 FROM public")
		require.NoError(t, err)
		_, err = txn.ExecEx(ctx, "revoke-privileges", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			"REVOKE CONNECT ON DATABASE defaultdb FROM public")
		require.NoError(t, err)
		_, err = txn.ExecEx(ctx, "revoke-privileges", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			"REVOKE CONNECT ON DATABASE postgres FROM public")
		require.NoError(t, err)

		row, err = txn.QueryRowEx(
			ctx, "get-all-databases", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			"SELECT count(1) FROM crdb_internal.databases",
		)
		require.NoError(t, err)
		// 3 databases (system, defaultdb, postgres, test1, test2).
		require.Equal(t, row.String(), "(5)")
		return nil
	}); err != nil {
		t.Fatal(err)
	}

}

// TestConcurrentGrant tests that populating MembershipCache with a high
// priority can avoid deadlock from concurrent GRANTs, an issue detailed in
// #117144.
func TestConcurrentGrants(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	skip.UnderDuress(t, "this test needs to sleep half a second")

	runConcurrentGrantsWithPriority := func(t *testing.T, priority string) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)
		tdb := sqlutils.MakeSQLRunner(db)

		// Shortening this cluster setting is essential for this test. A small value
		// will force `txn1` to bump up its write timestamp by the time it gets to
		// commit. This is needed to force `txn2` to encounter a WriteTooOldError
		// after being unblocked, and hence enter a retry, as retry in `txn2` is
		// prerequisite for the potential deadlock described in #117144.
		tdb.Exec(t, "SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1ms'")

		tdb.Exec(t, "CREATE ROLE developer;")
		tdb.Exec(t, "CREATE USER user1")
		tdb.Exec(t, "CREATE USER user2")

		// 1. Start a txn1, GRANT something
		// 2. Start another txn2 that issues a concurrent GRANT (it will block)
		// 3. Wait a short while
		// 4. Commit txn1 and that will unblock txn2
		cg := ctxgroup.WithContext(ctx)
		wg := sync.WaitGroup{}
		wg.Add(1)

		cg.Go(func() error {
			wg.Wait()
			// txn2
			_, err := db.Exec(fmt.Sprintf(`
BEGIN PRIORITY %s;
SET LOCAL autocommit_before_ddl = false;
GRANT developer TO user2;
COMMIT`,
				priority))
			return err
		})

		txn1, err := db.Begin()
		require.NoError(t, err)
		_, err = txn1.Exec(fmt.Sprintf("SET TRANSACTION PRIORITY %s", priority))
		require.NoError(t, err)
		_, err = txn1.Exec("SET LOCAL autocommit_before_ddl = false;")
		require.NoError(t, err)
		_, err = txn1.Exec("GRANT developer TO user1")
		require.NoError(t, err)
		wg.Done()

		// Wait for a few seconds. It allows:
		// 1. txn2 to start running and blocking
		// 2. (more importantly) `txn1` to bump up its write timestamp upon commit,
		//    so, it will cause txn2 to encounter a WriteTooOld error after
		//    unblocking and retry.
		time.Sleep(500 * time.Millisecond)
		err = txn1.Commit()
		require.NoError(t, err)

		// Wait to ensure `grant developer to user2` also completed successfully.
		err = cg.Wait()
		require.NoError(t, err)
	}

	for _, priority := range []string{"NORMAL", "HIGH"} {
		t.Run(fmt.Sprintf("priority=%s", priority), func(
			t *testing.T,
		) {
			runConcurrentGrantsWithPriority(t, priority)
		})
	}
}
