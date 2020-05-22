// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

type interceptingTransport struct {
	kvcoord.Transport
	sendNext func(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, error)
}

func (t *interceptingTransport) SendNext(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	if fn := t.sendNext; fn != nil {
		return fn(ctx, ba)
	} else {
		return t.Transport.SendNext(ctx, ba)
	}
}

// TestAmbiguousCommit verifies that an ambiguous commit error is returned from
// sql.Exec in situations where an EndTxn is part of a batch and the disposition
// of the batch request is unknown after a network failure or timeout. The goal
// here is to prevent spurious transaction retries after the initial transaction
// actually succeeded. In cases where there's an auto- generated primary key,
// this can result in silent duplications. In cases where the primary key is
// specified in advance, it can result in violated uniqueness constraints, or
// duplicate key violations. See #6053, #7604, and #10023.
func TestAmbiguousCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testutils.RunTrueAndFalse(t, "ambiguousSuccess", func(t *testing.T, ambiguousSuccess bool) {
		var params base.TestServerArgs
		var processed int32
		var tableStartKey atomic.Value

		translateToRPCError := roachpb.NewError(errors.Errorf("%s: RPC error: success=%t", t.Name(), ambiguousSuccess))

		maybeRPCError := func(req *roachpb.ConditionalPutRequest) *roachpb.Error {
			tsk, ok := tableStartKey.Load().(roachpb.Key)
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

		params.Knobs.KVClient = &kvcoord.ClientTestingKnobs{
			TransportFactory: func(
				opts kvcoord.SendOptions, nodeDialer *nodedialer.Dialer, replicas kvcoord.ReplicaSlice,
			) (kvcoord.Transport, error) {
				transport, err := kvcoord.GRPCTransportFactory(opts, nodeDialer, replicas)
				return &interceptingTransport{
					Transport: transport,
					sendNext: func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, error) {
						if ambiguousSuccess {
							br, err := transport.SendNext(ctx, ba)
							// During shutdown, we may get responses that
							// have call.Err set and all we have to do is
							// not crash on those.
							//
							// For the rest, compare and perhaps inject an
							// RPC error ourselves.
							if err == nil && br.Error.Equal(translateToRPCError) {
								// Translate the injected error into an RPC
								// error to simulate an ambiguous result.
								return nil, br.Error.GoError()
							}
							return br, err
						} else {
							if req, ok := ba.GetArg(roachpb.ConditionalPut); ok {
								if pErr := maybeRPCError(req.(*roachpb.ConditionalPutRequest)); pErr != nil {
									// Blackhole the RPC and return an
									// error to simulate an ambiguous
									// result.
									return nil, pErr.GoError()
								}
							}
							return transport.SendNext(ctx, ba)
						}
					},
				}, err
			},
		}

		if ambiguousSuccess {
			params.Knobs.Store = &kvserver.StoreTestingKnobs{
				TestingResponseFilter: func(
					ctx context.Context, args roachpb.BatchRequest, _ *roachpb.BatchResponse,
				) *roachpb.Error {
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
		defer tc.Stopper().Stop(context.Background())

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
		if _, err := sqlDB.Exec(`CREATE TABLE test.t (k SERIAL PRIMARY KEY, v INT)`); err != nil {
			t.Fatal(err)
		}

		tableID := sqlutils.QueryTableID(t, sqlDB, "test", "public", "t")
		tableStartKey.Store(keys.SystemSQLCodec.TablePrefix(tableID))

		// Wait for new table to split & replication.
		if err := tc.WaitForSplitAndInitialization(tableStartKey.Load().(roachpb.Key)); err != nil {
			t.Fatal(err)
		}

		// Ensure that the dist sender's cache is up to date before
		// fault injection.
		if rows, err := sqlDB.Query(`SELECT * FROM test.t`); err != nil {
			t.Fatal(err)
		} else if err := rows.Close(); err != nil {
			t.Fatal(err)
		}

		if _, err := sqlDB.Exec(`INSERT INTO test.t (v) VALUES (1)`); ambiguousSuccess {
			if pqErr := (*pq.Error)(nil); errors.As(err, &pqErr) {
				if pqErr.Code != pgcode.StatementCompletionUnknown {
					t.Errorf("expected code %q, got %q (err: %s)",
						pgcode.StatementCompletionUnknown, pqErr.Code, err)
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
		if err := sqlDB.QueryRow(`SELECT count(*) FROM test.t`).Scan(&rowCount); err != nil {
			t.Fatal(err)
		}
		if e := 1; rowCount != e {
			t.Errorf("expected %d row(s) but found %d", e, rowCount)
		}
	})
}
