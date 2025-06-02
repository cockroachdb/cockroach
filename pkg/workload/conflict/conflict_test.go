// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package conflict

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

func TestLDRCompatibility(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	server, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer server.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, "CREATE DATABASE a")
	sqlDB.Exec(t, "CREATE DATABASE b")

	replicationtestutils.ConfigureDefaultSettings(t, sqlDB)

	dbA := sqlutils.MakeSQLRunner(server.SQLConn(t, serverutils.DBName("a")))
	dbB := sqlutils.MakeSQLRunner(server.SQLConn(t, serverutils.DBName("b")))

	// Create a random table in database A
	rndSrc, _ := randutil.NewTestRand()
	stmt := tree.AsStringWithFlags(
		randgen.RandCreateTable(ctx, rndSrc, "test_writer", 1, RandTableOpt),
		tree.FmtParsable)
	dbA.Exec(t, stmt)

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, server, server, serverutils.DBName("a"))
	dbB.Exec(t,
		"CREATE LOGICALLY REPLICATED TABLE test_writer1 FROM TABLE test_writer1 ON $1 WITH UNIDIRECTIONAL",
		dbAURL.String(),
	)
}
