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
	"strings"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/mutations"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

type interceptingTransport struct {
	kvcoord.Transport
	sendNext func(context.Context, *kvpb.BatchRequest) (*kvpb.BatchResponse, error)
}

func (t *interceptingTransport) SendNext(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	if mutations.MaxBatchSize(false /* forceProductionMaxBatchSize */) == 1 {
		// This test relies on the fact that the mutation batch consisting of a
		// single row also contains an EndTxn which is the case only when the
		// max batch size is at least 2, so we'll skip it.
		skip.UnderMetamorphic(t)
	}

	testutils.RunTrueAndFalse(t, "ambiguousSuccess", func(t *testing.T, ambiguousSuccess bool) {
		var params base.TestServerArgs
		var processed int32
		var tableStartKey atomic.Value

		const errMarker = "boom"
		maybeRPCError := func(req *kvpb.ConditionalPutRequest) *kvpb.Error {
			tsk, ok := tableStartKey.Load().(roachpb.Key)
			if !ok {
				return nil
			}
			if !bytes.HasPrefix(req.Header().Key, tsk) {
				return nil
			}
			if atomic.AddInt32(&processed, 1) == 1 {
				return kvpb.NewError(errors.Errorf(errMarker))
			}
			return nil
		}

		params.Knobs.KVClient = &kvcoord.ClientTestingKnobs{
			TransportFactory: func(factory kvcoord.TransportFactory) kvcoord.TransportFactory {
				return func(options kvcoord.SendOptions, slice kvcoord.ReplicaSlice) (kvcoord.Transport, error) {
					transport, err := factory(options, slice)
					return &interceptingTransport{
						Transport: transport,
						sendNext: func(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, error) {
							if ambiguousSuccess {
								br, err := transport.SendNext(ctx, ba)
								// During shutdown, we may get responses that
								// have call.Err set and all we have to do is
								// not crash on those.
								//
								// For the rest, compare and perhaps inject an
								// RPC error ourselves.
								if err == nil && br.Error != nil && strings.Contains(br.Error.GoError().Error(), errMarker) {
									// Translate the injected error into an RPC
									// error to simulate an ambiguous result.
									return nil, br.Error.GoError()
								}
								return br, err
							} else {
								if req, ok := ba.GetArg(kvpb.ConditionalPut); ok {
									if pErr := maybeRPCError(req.(*kvpb.ConditionalPutRequest)); pErr != nil {
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
				}
			},
		}

		if ambiguousSuccess {
			params.Knobs.Store = &kvserver.StoreTestingKnobs{
				TestingResponseFilter: func(
					ctx context.Context, args *kvpb.BatchRequest, _ *kvpb.BatchResponse,
				) *kvpb.Error {
					if req, ok := args.GetArg(kvpb.ConditionalPut); ok {
						return maybeRPCError(req.(*kvpb.ConditionalPutRequest))
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
			st := server.ApplicationLayer().ClusterSettings()
			st.Manual.Store(true)
			sql.DistSQLClusterExecMode.Override(ctx, &st.SV, int64(sessiondatapb.DistSQLOff))
		}

		sqlDB := tc.Conns[0]

		if _, err := sqlDB.Exec(`CREATE DATABASE test`); err != nil {
			t.Fatal(err)
		}
		if _, err := sqlDB.Exec(`CREATE TABLE test.t (k SERIAL PRIMARY KEY, v INT)`); err != nil {
			t.Fatal(err)
		}

		tableID := sqlutils.QueryTableID(t, sqlDB, "test", "public", "t")
		tableStartKey.Store(tc.ApplicationLayer(0).Codec().TablePrefix(tableID))

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
				if pgcode.MakeCode(string(pqErr.Code)) != pgcode.StatementCompletionUnknown {
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
