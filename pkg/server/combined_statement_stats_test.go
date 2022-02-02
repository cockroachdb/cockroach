// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestCombinedStatementStats ensures that the Statements endpoint
// is accessible via gRPC and returns information reflecting
// both recently run SQL queries and flushed statement and txn
// statistics.
func TestCombinedStatementStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()
	testServer, db, _ := serverutils.StartServer(t, params)
	defer testServer.Stopper().Stop(ctx)

	conn, err := testServer.RPCContext().GRPCDialNode(
		testServer.RPCAddr(), testServer.NodeID(), rpc.DefaultClass,
	).Connect(ctx)
	require.NoError(t, err)

	client := serverpb.NewStatusClient(conn)
	sqlServer := testServer.(*TestServer).Server.sqlServer.pgServer.SQLServer

	expectedQueries := []string{`CREATE TABLE bar (id INT8)`, `CREATE TABLE foo (id INT8)`}

	_, err = db.Exec(expectedQueries[0])
	require.NoError(t, err)
	sqlServer.GetSQLStatsProvider().(*persistedsqlstats.PersistedSQLStats).Flush(ctx)
	_, err = db.Exec(expectedQueries[1])
	require.NoError(t, err)

	resp, err := client.CombinedStatementStats(ctx, &serverpb.CombinedStatementsStatsRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Statements)
	require.NotEmpty(t, resp.Transactions)

	var responseQueries []string
	for _, s := range resp.Statements {
		if strings.HasPrefix(s.Key.KeyData.App, catconstants.InternalAppNamePrefix) {
			t.Fatalf("unexpected internal query: %s", s.Key.KeyData.Query)
		}

		responseQueries = append(responseQueries, s.Key.KeyData.Query)
	}

	sort.Strings(responseQueries)
	sort.Strings(expectedQueries)
	require.ElementsMatch(t, expectedQueries, responseQueries)
}
