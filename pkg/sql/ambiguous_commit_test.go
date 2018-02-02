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

package sql_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/lib/pq"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

type interceptingTransport struct {
	kv.Transport
	sendNext func(context.Context, chan<- kv.BatchCall)
}

func (t *interceptingTransport) SendNext(ctx context.Context, done chan<- kv.BatchCall) {
	if fn := t.sendNext; fn != nil {
		fn(ctx, done)
	} else {
		t.Transport.SendNext(ctx, done)
	}
}

// TestAmbiguousCommit verifies that an ambiguous commit error is returned
// from sql.Exec in situations where an EndTransaction is part of a batch and
// the disposition of the batch request is unknown after a network failure or
// timeout. The goal here is to prevent spurious transaction retries after the
// initial transaction actually succeeded. In cases where there's an auto-
// generated primary key, this can result in silent duplications. In cases
// where the primary key is specified in advance, it can result in violated
// uniqueness constraints, or duplicate key violations. See #6053, #7604, and
// #10023.
func TestAmbiguousCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "ambiguousSuccess", func(t *testing.T, ambiguousSuccess bool) {
		var params base.TestServerArgs
		var processed int32
		var tableStartKey atomic.Value

		translateToRPCError := roachpb.NewError(errors.Errorf("%s: RPC error: success=%t", t.Name(), ambiguousSuccess))

		maybeRPCError := func(req *roachpb.ConditionalPutRequest) *roachpb.Error {
			tsk, ok := tableStartKey.Load().([]byte)
			if !ok {
				return nil
			}
			if !bytes.HasPrefix(req.Header().Key, tsk) {
				return nil
			}
			if atomic.AddInt32(&processed, 1) == 1 {
				return translateToRPCError
			}
			return nil
		}

		params.Knobs.DistSender = &kv.DistSenderTestingKnobs{
			TransportFactory: func(opts kv.SendOptions, rpcContext *rpc.Context, replicas kv.ReplicaSlice, args roachpb.BatchRequest) (kv.Transport, error) {
				transport, err := kv.GRPCTransportFactory(opts, rpcContext, replicas, args)
				return &interceptingTransport{
					Transport: transport,
					sendNext: func(ctx context.Context, done chan<- kv.BatchCall) {
						if ambiguousSuccess {
							interceptDone := make(chan kv.BatchCall)
							go transport.SendNext(ctx, interceptDone)
							call := <-interceptDone
							// During shutdown, we may get responses that
							// have call.Err set and all we have to do is
							// not crash on those.
							//
							// For the rest, compare and perhaps inject an
							// RPC error ourselves.
							if call.Err == nil && call.Reply.Error.Equal(translateToRPCError) {
								// Translate the injected error into an RPC
								// error to simulate an ambiguous result.
								done <- kv.BatchCall{Err: call.Reply.Error.GoError()}
							} else {
								// Either the call succeeded or we got a non-
								// sentinel error; let normal machinery do its
								// thing.
								done <- call
							}
						} else {
							if req, ok := args.GetArg(roachpb.ConditionalPut); ok {
								if pErr := maybeRPCError(req.(*roachpb.ConditionalPutRequest)); pErr != nil {
									// Blackhole the RPC and return an
									// error to simulate an ambiguous
									// result.
									done <- kv.BatchCall{Err: pErr.GoError()}

									return
								}
							}
							transport.SendNext(ctx, done)
						}
					},
				}, err
			},
		}

		if ambiguousSuccess {
			params.Knobs.Store = &storage.StoreTestingKnobs{
				TestingResponseFilter: func(args roachpb.BatchRequest, _ *roachpb.BatchResponse) *roachpb.Error {
					if req, ok := args.GetArg(roachpb.ConditionalPut); ok {
						return maybeRPCError(req.(*roachpb.ConditionalPutRequest))
					}
					return nil
				},
			}
		}

		testClusterArgs := base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs:      params,
		}

		const numReplicas = 3
		tc := testcluster.StartTestCluster(t, numReplicas, testClusterArgs)
		defer tc.Stopper().Stop(context.TODO())

		// Avoid distSQL so we can reliably hydrate the intended dist
		// sender's cache below.
		for _, server := range tc.Servers {
			st := server.ClusterSettings()
			st.Manual.Store(true)
			sql.DistSQLClusterExecMode.Override(&st.SV, int64(sessiondata.DistSQLOff))
		}

		sqlDB := tc.Conns[0]

		if _, err := sqlDB.Exec(`CREATE DATABASE test`); err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec(`CREATE TABLE test.public.t (k SERIAL PRIMARY KEY, v INT)`); err != nil {
			t.Fatal(err)
		}

		tableID, err := sqlutils.QueryTableID(sqlDB, "test", "t")
		if err != nil {
			t.Fatal(err)
		}
		tableStartKey.Store(keys.MakeTablePrefix(tableID))

		// Wait for new table to split & replication.
		if err := tc.WaitForSplitAndReplication(tableStartKey.Load().([]byte)); err != nil {
			t.Fatal(err)
		}

		// Ensure that the dist sender's cache is up to date before
		// fault injection.
		if rows, err := sqlDB.Query(`SELECT * FROM test.public.t`); err != nil {
			t.Fatal(err)
		} else if err := rows.Close(); err != nil {
			t.Fatal(err)
		}

		if _, err := sqlDB.Exec(`INSERT INTO test.public.t (v) VALUES (1)`); ambiguousSuccess {
			if pqErr, ok := err.(*pq.Error); ok {
				if pqErr.Code != pgerror.CodeStatementCompletionUnknownError {
					t.Errorf("expected code %q, got %q (err: %s)",
						pgerror.CodeStatementCompletionUnknownError, pqErr.Code, err)
				}
			} else {
				t.Errorf("expected pq error; got %v", err)
			}
		} else {
			if err != nil {
				t.Error(err)
			}
		}

		// Verify a single row exists in the table.
		var rowCount int
		if err := sqlDB.QueryRow(`SELECT count(*) FROM test.public.t`).Scan(&rowCount); err != nil {
			t.Fatal(err)
		}
		if e := 1; rowCount != e {
			t.Errorf("expected %d row(s) but found %d", e, rowCount)
		}
	})
}
