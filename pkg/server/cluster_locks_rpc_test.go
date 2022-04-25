// Copyright 2022 The Cockroach Authors.
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
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestClusterLocksState ensures that the ClusterLocksState endpoint is
// accessible via gRPC and returns information reflecting the state of
// locks in the cluster.
//
// This test executes a txn that will acquire a lock and wait on one node.
// It then executes two txns in the remaining nodes that will block on
// the first txn.
func TestClusterLocksState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()
	//testServer, db, _ := serverutils.StartServer(t, params)
	testCluster := serverutils.StartNewTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: params,
	})
	defer testCluster.Stopper().Stop(ctx)

	firstServer := testCluster.Server(0)
	clientConn, err := firstServer.RPCContext().GRPCDialNode(
		firstServer.RPCAddr(), firstServer.NodeID(), rpc.DefaultClass,
	).Connect(ctx)
	require.NoError(t, err)

	applicationName := "test_cluster_locks_rpc"

	firstServerConn := testCluster.ServerConn(0)

	// Execute a lock holding txn in the first server.
	setupQuery := fmt.Sprintf(`
SET APPLICATION_NAME = '%s';
CREATE TABLE foo (k STRING PRIMARY KEY, v STRING, FAMILY (k, v));
INSERT INTO foo VALUES ('a', 'val1'), ('b', 'val2'), ('c', 'val3');`, applicationName)

	_, err = firstServerConn.Exec(setupQuery)
	require.NoError(t, err)

	resultCh := make(chan struct{})
	errorCh := make(chan error)
	defer close(errorCh)
	defer close(resultCh)

	tx, err := firstServerConn.Begin()
	require.NoError(t, err)

	go func() {
		if _, err := tx.Exec("UPDATE foo SET v = '_updated' WHERE k >= 'b'"); err != nil {
			errorCh <- err
		} else {
			resultCh <- struct{}{}
		}
	}()

	client := serverpb.NewStatusClient(clientConn)

	// Observe the running transaction.
	var txnLockHolder uuid.UUID
	require.Eventually(t, func() bool {
		resp, err := client.ListSessions(ctx, &serverpb.ListSessionsRequest{ExcludeClosedSessions: true})
		require.NoError(t, err)

		for _, session := range resp.Sessions {
			if session.ApplicationName == applicationName && session.ActiveTxn != nil {
				txnLockHolder = session.ActiveTxn.ID
				return true
			}
		}
		return false
	}, 10*time.Second, 100*time.Millisecond, "txn not found")
	require.NotEmpty(t, txnLockHolder)

	// Execute queries on other two servers that will wait to acquire lock.
	for i := 1; i < 3; i++ {
		go func(i int) {
			db := testCluster.ServerConn(i)
			if _, err := db.Exec("SELECT * FROM foo"); err != nil {
				errorCh <- err
			} else {
				resultCh <- struct{}{}
			}
		}(i)
	}

	// Observe the two blocked txns and the txn lock holder.
	var waitingTxnIDs []uuid.UUID
	require.Eventually(t, func() bool {
		resp, err := client.ListSessions(ctx, &serverpb.ListSessionsRequest{ExcludeClosedSessions: true})
		require.NoError(t, err)
		waitingTxnsCount := 0
		lockHolderFound := false
		for _, session := range resp.Sessions {
			if session.ActiveTxn != nil && session.ActiveTxn.ID == txnLockHolder {
				lockHolderFound = true
			}
			for _, query := range session.ActiveQueries {
				if query.Sql == "SELECT * FROM foo" {
					waitingTxnIDs = append(waitingTxnIDs, query.TxnID)
					waitingTxnsCount++
				}
			}
		}

		return waitingTxnsCount == 2 && lockHolderFound
	}, 10*time.Second, 100*time.Millisecond, "could not find txns")

	resp, err := client.ClusterLocksState(ctx, &serverpb.ClusterLocksStateRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, resp.Locks)

	// We should find the two expected txns in the waiting list.
	foundWaitingTxnIDs := make(map[uuid.UUID]bool)
	for _, lock := range resp.Locks {
		if len(lock.WaitingTxns) > 0 && lock.TxnLockHolder != nil && txnLockHolder == lock.TxnLockHolder.TxnID {
			for _, txn := range lock.WaitingTxns {
				foundWaitingTxnIDs[txn.TxnID] = true
			}
		}
	}

	for _, txnID := range waitingTxnIDs {
		require.True(t, foundWaitingTxnIDs[txnID])
	}

	// Commit the open transaction.
	err = tx.Commit()
	require.NoError(t, err)

	// Wait for all three txns to finish.
	for i := 0; i < 3; i++ {
		select {
		case <-resultCh:
			continue
		case err := <-errorCh:
			t.Fatalf("unexpected error while executing stmt: %s", err)
		}
	}

}
