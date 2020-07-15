// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl

import (
	"context"
	"errors"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

// TestSQLServer starts up a semi-dedicated SQL server and runs some smoke test
// queries. The SQL server shares some components, notably Gossip, with a test
// server serving as a KV backend.
//
// TODO(tbg): start narrowing down and enumerating the unwanted dependencies. In
// the end, the SQL server in this test should not depend on a Gossip instance
// and must not rely on having a NodeID/NodeDescriptor/NodeLiveness/...
//
// In short, it should not rely on the test server through anything other than a
// `*kv.DB` and a small number of allowlisted RPCs.
func TestSQLServer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := serverutils.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	db := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10)})
	defer db.Close()
	r := sqlutils.MakeSQLRunner(db)
	r.QueryStr(t, `SELECT 1`)
	r.Exec(t, `CREATE DATABASE foo`)
	r.Exec(t, `CREATE TABLE foo.kv (k STRING PRIMARY KEY, v STRING)`)
	r.Exec(t, `INSERT INTO foo.kv VALUES('foo', 'bar')`)
	// Cause an index backfill operation.
	r.Exec(t, `CREATE INDEX ON foo.kv (v)`)
	t.Log(sqlutils.MatrixToStr(r.QueryStr(t, `SET distsql=off; SELECT * FROM foo.kv`)))
	t.Log(sqlutils.MatrixToStr(r.QueryStr(t, `SET distsql=auto; SELECT * FROM foo.kv`)))
}

func TestTenantCannotSetClusterSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := serverutils.StartTestCluster(t, 1, base.TestClusterArgs{})
	defer tc.Stopper().Stop(ctx)

	// StartTenant with the default permissions to
	db := serverutils.StartTenant(t, tc.Server(0), base.TestTenantArgs{TenantID: roachpb.MakeTenantID(10), AllowSettingClusterSettings: false})
	defer db.Close()
	_, err := db.Exec(`SET CLUSTER SETTING sql.defaults.vectorize=off`)
	var pqErr *pq.Error
	ok := errors.As(err, &pqErr)
	require.True(t, ok, "expected err to be a *pq.Error but is of type %T. error is: %v", err)
	require.Equal(t, pq.ErrorCode(pgcode.InsufficientPrivilege.String()), pqErr.Code, "err %v has unexpected code", err)
}
