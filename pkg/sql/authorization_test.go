// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
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

		_, err = txn.ExecEx(ctx, "create-database1", txn.KV(), sessiondata.RootUserSessionDataOverride,
			"CREATE DATABASE test1")
		require.NoError(t, err)

		_, err = txn.ExecEx(ctx, "create-database2", txn.KV(), sessiondata.RootUserSessionDataOverride,
			"CREATE DATABASE test2")
		require.NoError(t, err)

		// Revoke CONNECT on all non-system databases and ensure that when querying
		// with node, we can still see all the databases.
		_, err = txn.ExecEx(ctx, "revoke-privileges", txn.KV(), sessiondata.RootUserSessionDataOverride,
			"REVOKE CONNECT ON DATABASE test1 FROM public")
		require.NoError(t, err)
		_, err = txn.ExecEx(ctx, "revoke-privileges", txn.KV(), sessiondata.RootUserSessionDataOverride,
			"REVOKE CONNECT ON DATABASE test2 FROM public")
		require.NoError(t, err)
		_, err = txn.ExecEx(ctx, "revoke-privileges", txn.KV(), sessiondata.RootUserSessionDataOverride,
			"REVOKE CONNECT ON DATABASE defaultdb FROM public")
		require.NoError(t, err)
		_, err = txn.ExecEx(ctx, "revoke-privileges", txn.KV(), sessiondata.RootUserSessionDataOverride,
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
	skip.UnderStress(t, "this test needs to sleep a few seconds")

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	tdb := sqlutils.MakeSQLRunner(db)

	tdb.Exec(t, "CREATE ROLE developer;")
	tdb.Exec(t, "CREATE USER user1")
	tdb.Exec(t, "CREATE USER user2")

	// 1. Start a txn1, GRANT something
	// 2. Issue a concurrent GRANT and it will block
	// 3. Wait a few seconds
	// 4. Commit txn1 and that will unblock the other blocking GRANT
	cg := ctxgroup.WithContext(ctx)
	wg := sync.WaitGroup{}
	wg.Add(1)

	cg.Go(func() error {
		wg.Wait()
		_, err := db.Exec("GRANT developer TO user2")
		return err
	})

	txn1, err := db.Begin()
	require.NoError(t, err)
	_, err = txn1.Exec("GRANT developer TO user1")
	require.NoError(t, err)
	wg.Done()

	// Wait for a few seconds. It allows:
	// 1. the other `GRANT developer TO user` to start running and blocking
	// 2. (more importantly) `txn1` to increase its write timestamp so that
	//    upon commit, it will cause a writeTooOld retry error on that other
	//    blocking GRANT such that upon retry, the other GRANT will launch
	//    the singleflight to re-populate the now-invalid MembershipCache,
	//    which would previously have caused a deadlock.
	time.Sleep(5 * time.Second)
	err = txn1.Commit()
	require.NoError(t, err)

	// Wait to ensure `grant developer to user2` also completed successfully.
	err = cg.Wait()
	require.NoError(t, err)
}
