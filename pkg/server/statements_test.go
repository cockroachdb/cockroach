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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// TestStatements ensures that the Statements endpoint is accessible
// via gRPC and returns information reflecting recently run SQL
// queries.
func TestStatements(t *testing.T) {
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
	defer func(conn *grpc.ClientConn) {
		_ = conn.Close()
	}(conn)

	client := serverpb.NewStatusClient(conn)

	testQuery := "CREATE TABLE foo (id INT8)"
	_, err = db.Exec(testQuery)
	require.NoError(t, err)

	resp, err := client.Statements(ctx, &serverpb.StatementsRequest{NodeID: "local"})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Statements)

	queries := make([]string, len(resp.Statements))
	for _, s := range resp.Statements {
		queries = append(queries, s.Key.KeyData.Query)
	}
	require.Contains(t, queries, testQuery)
}
