// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package conflict

import (
	"context"
	"testing"
	"time"

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
	rndSrc.Seed(time.Now().UnixNano())

	// TODO(jeffswenson): rand create table refactor
	// Extend options to use the go options pattern.
	//
	// Add an Index and ColumnType filter function that is used retry generation
	// of types that are incompatible with LDR.
	//
	// For other options, add a probility struct that is tuned by LDR.

	// TODO(jeffswenson): FLOAT8 in the primary key causes issues with LDR because it has a composite encoding.
	stmt := tree.AsStringWithFlags(
		randgen.RandCreateTable(ctx, rndSrc, "test_writer", 1, RandTableOpt),
		tree.FmtParsable)
	t.Logf("creating table: %s", stmt)
	dbA.Exec(t, stmt)

	dbAURL := replicationtestutils.GetExternalConnectionURI(t, server, server, serverutils.DBName("a"))
	dbBURL := replicationtestutils.GetExternalConnectionURI(t, server, server, serverutils.DBName("b"))
	dbB.Exec(t,
		"CREATE LOGICALLY REPLICATED TABLE b.test_writer1 FROM TABLE a.test_writer1 ON $1 WITH BIDIRECTIONAL ON $2",
		dbAURL.String(),
		dbBURL.String(),
	)
}
