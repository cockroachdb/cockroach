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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCheckAnyPrivilegeForNodeUser(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})

	defer s.Stopper().Stop(ctx)

	ts := s.(*server.TestServer)

	require.NotNil(t, ts.InternalExecutor())

	cf := ts.CollectionFactory().(*descs.CollectionFactory)

	if err := cf.TxnWithExecutor(ctx, s.DB(), nil, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection, ie sqlutil.InternalExecutor,
	) error {
		row, err := ie.QueryRowEx(
			ctx, "get-all-databases", txn, sessiondata.NodeUserSessionDataOverride,
			"SELECT count(1) FROM crdb_internal.databases",
		)
		require.NoError(t, err)
		// 3 databases (system, defaultdb, postgres).
		require.Equal(t, row.String(), "(3)")

		_, err = ie.ExecEx(ctx, "create-database1", txn, sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			"CREATE DATABASE test1")
		require.NoError(t, err)

		_, err = ie.ExecEx(ctx, "create-database2", txn, sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			"CREATE DATABASE test2")
		require.NoError(t, err)

		// Revoke CONNECT on all non-system databases and ensure that when querying
		// with node, we can still see all the databases.
		_, err = ie.ExecEx(ctx, "revoke-privileges", txn, sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			"REVOKE CONNECT ON DATABASE test1 FROM public")
		require.NoError(t, err)
		_, err = ie.ExecEx(ctx, "revoke-privileges", txn, sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			"REVOKE CONNECT ON DATABASE test2 FROM public")
		require.NoError(t, err)
		_, err = ie.ExecEx(ctx, "revoke-privileges", txn, sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			"REVOKE CONNECT ON DATABASE defaultdb FROM public")
		require.NoError(t, err)
		_, err = ie.ExecEx(ctx, "revoke-privileges", txn, sessiondata.InternalExecutorOverride{User: username.RootUserName()},
			"REVOKE CONNECT ON DATABASE postgres FROM public")
		require.NoError(t, err)

		row, err = ie.QueryRowEx(
			ctx, "get-all-databases", txn, sessiondata.NodeUserSessionDataOverride,
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
