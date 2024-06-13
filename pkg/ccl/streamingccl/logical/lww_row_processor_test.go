// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// BenchmarkLastWriteWinsInsert is a microbenchmark that targets overhead of the
// SQL layer when processing the INSERT query of the LWW handler. It mocks out
// the KV layer by injecting an error during KV request evaluation.
func BenchmarkLastWriteWinsInsert(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	var discardWrite atomic.Bool
	// key will be set later, before flipping discardWrite to true.
	var key roachpb.Key
	var numInjectedErrors atomic.Int64
	injectedErr := kvpb.NewError(errors.New("injected error"))

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(b, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
					// In order to focus the benchmark on the SQL overhead of
					// evaluating INSERT stmts via the internal executor, we'll
					// inject errors for them so that the processing of the KV
					// request stops once it reaches the KV client.
					if !discardWrite.Load() || !ba.IsWrite() || len(ba.Requests) != 1 {
						return nil
					}
					req, ok := ba.GetArg(kvpb.ConditionalPut)
					if !ok {
						return nil
					}
					if !req.(*kvpb.ConditionalPutRequest).Key.Equal(key) {
						return nil
					}
					numInjectedErrors.Add(1)
					return injectedErr
				},
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	runner := sqlutils.MakeSQLRunner(sqlDB)
	tableName := "tab"
	runner.Exec(b, "CREATE TABLE tab (pk int primary key, payload string)")
	runner.Exec(b, "ALTER TABLE tab ADD COLUMN crdb_internal_origin_timestamp DECIMAL NOT VISIBLE DEFAULT NULL ON UPDATE NULL")

	desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "defaultdb", tableName)
	tableDescs := make(map[string]descpb.TableDescriptor, 1)
	tableDescs["defaultdb.public."+tableName] = *desc.TableDesc()
	rp, err := makeSQLLastWriteWinsHandler(ctx, s.ClusterSettings(), tableDescs)
	require.NoError(b, err)

	// We'll be simulating processing the same INSERT over and over in the loop.
	keyValue := replicationtestutils.EncodeKV(b, s.Codec(), desc, 1, "hello")
	keyValue.Value.Timestamp = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	key = keyValue.Key

	defer discardWrite.Store(false)
	discardWrite.Store(true)
	b.ResetTimer()
	b.ReportAllocs()
	var lastTxnErr, lastRowErr error
	var i int
	for i = 0; i < b.N; i++ {
		// We need to start a fresh Txn since we're injecting an error.
		lastTxnErr = s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			// The error is expected here due to injected error.
			lastRowErr = rp.ProcessRow(ctx, txn, keyValue)
			keyValue.Value.Timestamp.WallTime += 1
			return nil
		})
	}
	require.Error(b, lastTxnErr)
	require.Error(b, lastRowErr)
	// Sanity check that an error was injected on each iteration.
	require.Equal(b, numInjectedErrors.Load(), int64(i))
}
