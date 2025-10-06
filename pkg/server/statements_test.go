// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestStatements ensures that the Statements endpoint is accessible
// via gRPC and returns information reflecting recently run SQL
// queries.
func TestStatements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testServer, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer testServer.Stopper().Stop(ctx)

	client := testServer.GetStatusClient(t)

	testQuery := "CREATE TABLE foo (id INT8)"
	_, err := db.Exec(testQuery)
	require.NoError(t, err)

	resp, err := client.Statements(ctx, &serverpb.StatementsRequest{NodeID: "local"})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Statements)
	require.NotEmpty(t, resp.Transactions)

	queries := make([]string, len(resp.Statements))
	for _, s := range resp.Statements {
		queries = append(queries, s.Key.KeyData.Query)
	}
	require.Contains(t, queries, testQuery)
}

func TestStatementsExcludeStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testServer, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer testServer.Stopper().Stop(ctx)

	client := testServer.GetStatusClient(t)

	testQuery := "CREATE TABLE foo (id INT8)"
	_, err := db.Exec(testQuery)
	require.NoError(t, err)

	t.Run("exclude-statements", func(t *testing.T) {
		resp, err := client.Statements(ctx, &serverpb.StatementsRequest{
			NodeID:    "local",
			FetchMode: serverpb.StatementsRequest_TxnStatsOnly,
		})
		require.NoError(t, err)
		require.NotEmpty(t, resp.Transactions)
		require.Empty(t, resp.Statements)
	})

	t.Run("exclude-transactions", func(t *testing.T) {
		resp, err := client.Statements(ctx, &serverpb.StatementsRequest{
			NodeID:    "local",
			FetchMode: serverpb.StatementsRequest_StmtStatsOnly,
		})
		require.NoError(t, err)
		require.Empty(t, resp.Transactions)
		require.NotEmpty(t, resp.Statements)
	})
}
