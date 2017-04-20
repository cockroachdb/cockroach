// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package sql_test

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/lib/pq"
)

// TestAmbiguousCommitDueToLeadershipChange verifies that an ambiguous
// commit error is returned from sql.Exec in situations where an
// EndTransaction is part of a batch and the disposition of the batch
// request is unknown after a network failure or timeout. The goal
// here is to prevent spurious transaction retries after the initial
// transaction actually succeeded. In cases where there's an
// auto-generated primary key, this can result in silent
// duplications. In cases where the primary key is specified in
// advance, it can result in violated uniqueness constraints, or
// duplicate key violations. See #6053, #7604, and #10023.
func TestAmbiguousCommitDueToLeadershipChange(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, requireDistSenderRetry := range []bool{true, false} {
		t.Run(fmt.Sprintf("retry=%t", requireDistSenderRetry), func(t *testing.T) {
			// Create a command filter which prevents EndTransaction from
			// returning a response.
			params := base.TestServerArgs{}
			committed := make(chan struct{})
			wait := make(chan struct{})
			var tableStartKey atomic.Value
			var responseCount int32

			// Prevent the first conditional put on table 51 from returning to
			// waiting client in order to simulate a lost update or slow network
			// link.
			params.Knobs.Store = &storage.StoreTestingKnobs{
				TestingResponseFilter: func(ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
					req, ok := ba.GetArg(roachpb.ConditionalPut)
					tsk := tableStartKey.Load()
					if tsk == nil {
						return nil
					}
					if !ok || !bytes.HasPrefix(req.Header().Key, tsk.([]byte)) {
						return nil
					}
					// If this is the first write to the table, wait to respond to the
					// client in order to simulate a retry.
					if atomic.AddInt32(&responseCount, 1) == 1 {
						close(committed)
						<-wait
					}
					return nil
				},
			}
			params.SendNextTimeout = 50 * time.Millisecond
			testClusterArgs := base.TestClusterArgs{
				ReplicationMode: base.ReplicationAuto,
				ServerArgs:      params,
			}
			const numReplicas = 3
			tc := testcluster.StartTestCluster(t, numReplicas, testClusterArgs)
			defer tc.Stopper().Stop(context.TODO())

			sqlDB := sqlutils.MakeSQLRunner(t, tc.Conns[0])

			sqlDB.Exec(`CREATE DATABASE test`)
			sqlDB.Exec(`CREATE TABLE test.t (k SERIAL PRIMARY KEY, v INT)`)

			tableID, err := sqlutils.QueryTableID(tc.Conns[0], "test", "t")
			if err != nil {
				t.Fatal(err)
			}
			tableStartKey.Store(keys.MakeTablePrefix(tableID))

			// Wait for new table to split & replication.
			if err := tc.WaitForSplitAndReplication(tableStartKey.Load().([]byte)); err != nil {
				t.Fatal(err)
			}

			// Lookup the lease.
			tableRangeDesc, err := tc.LookupRange(keys.MakeRowSentinelKey(tableStartKey.Load().([]byte)))
			if err != nil {
				t.Fatal(err)
			}
			// Loop until we can get the lease holder. We query in the loop to
			// ensure that the lease is acquired.
			var leaseHolder roachpb.ReplicationTarget
			testutils.SucceedsSoon(t, func() error {
				rows := sqlDB.Query(`SELECT * FROM test.t`)
				rows.Close()
				leaseHolder, err = tc.FindRangeLeaseHolder(
					tableRangeDesc,
					&roachpb.ReplicationTarget{
						NodeID:  tc.Servers[0].GetNode().Descriptor.NodeID,
						StoreID: tc.Servers[0].GetFirstStoreID(),
					})
				return err
			})

			// In a goroutine, send an insert which will commit but not return
			// from the leader (due to the command filter we installed on node 0).
			sqlErrCh := make(chan error, 1)
			go func() {
				// Use a connection other than through the node which is the current
				// leaseholder to ensure that we use GRPC instead of the local server.
				// If we use a local server, the hanging response we simulate takes
				// up the dist sender thread of execution because local requests are
				// executed synchronously.
				sqlConn := tc.Conns[leaseHolder.NodeID%numReplicas]
				_, err := sqlConn.Exec(`INSERT INTO test.t (v) VALUES (1)`)
				sqlErrCh <- err
				close(wait)
			}()
			// Wait until the insert has committed.
			<-committed

			// If requested, wait for two further attempts from the distributed sender.
			if requireDistSenderRetry {
				// Wait for twice the send next timeout.
				time.Sleep(2 * params.SendNextTimeout)
			}

			// Find a node other than the current lease holder to transfer the lease to.
			for i, s := range tc.Servers {
				if leaseHolder.StoreID != s.GetFirstStoreID() {
					if err := tc.TransferRangeLease(tableRangeDesc, tc.Target(i)); err != nil {
						t.Fatal(err)
					}
					break
				}
			}

			// Wait for the error from the pending SQL insert.
			err = <-sqlErrCh
			if pqErr, ok := err.(*pq.Error); !ok {
				t.Errorf("expected ambiguous commit error with correct code; got %v", err)
			} else {
				if pqErr.Code != pgerror.CodeStatementCompletionUnknownError {
					t.Errorf("expected code %q, got %q (err: %s)",
						pgerror.CodeStatementCompletionUnknownError, pqErr.Code, err)
				}
			}

			// Verify a single row exists in the table.
			var rowCount int
			sqlDB.QueryRow(`SELECT count(*) FROM test.t`).Scan(&rowCount)
			if rowCount != 1 {
				t.Errorf("expected 1 row but found %d", rowCount)
			}
		})
	}
}
