// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicationtestutils

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestGenerateLDRTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	server, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "CREATE DATABASE a")
	sqlDB.Exec(t, "CREATE DATABASE b")

	_, err := server.SystemLayer().SQLConn(t).Exec("SET CLUSTER SETTING kv.rangefeed.enabled = true")
	require.NoError(t, err)

	dbA := sqlutils.MakeSQLRunner(server.SQLConn(t, serverutils.DBName("a")))
	dbB := sqlutils.MakeSQLRunner(server.SQLConn(t, serverutils.DBName("b")))

	// Create a random table in database A
	rndSrc, _ := randutil.NewTestRand()
	rndSrc.Seed(time.Now().UnixNano())

	stmt := GenerateLDRTable(ctx, rndSrc, "test_writer", true)
	t.Logf("creating table: %s", stmt)
	dbA.Exec(t, stmt)

	dbAURL := GetExternalConnectionURI(t, server, server, serverutils.DBName("a"))
	dbBURL := GetExternalConnectionURI(t, server, server, serverutils.DBName("b"))
	dbB.Exec(t,
		"CREATE LOGICALLY REPLICATED TABLE b.test_writer FROM TABLE a.test_writer ON $1 WITH BIDIRECTIONAL ON $2",
		dbAURL.String(),
		dbBURL.String(),
	)
}
