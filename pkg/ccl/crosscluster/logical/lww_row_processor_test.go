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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationtestutils"
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

func TestLWWInsertQueryQuoting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	runner := sqlutils.MakeSQLRunner(sqlDB)

	type testCase struct {
		name       string
		schemaTmpl string
		row        []interface{}
	}

	testCases := []testCase{
		{
			name:       "column with special characters",
			schemaTmpl: `CREATE TABLE %s (pk int primary key, "payload-col" string)`,
			row:        []interface{}{1, "hello"},
		},
		{
			name:       "primary constraint with special characters",
			schemaTmpl: `CREATE TABLE %s (pk int, payload string, CONSTRAINT "primary-idx" PRIMARY KEY (pk ASC))`,
			row:        []interface{}{1, "hello"},
		},
	}

	tableNumber := 0
	createTable := func(stmt string) string {
		tableName := fmt.Sprintf("tab%d", tableNumber)
		runner.Exec(t, fmt.Sprintf(stmt, tableName))
		runner.Exec(t, fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN crdb_internal_origin_timestamp DECIMAL NOT VISIBLE DEFAULT NULL ON UPDATE NULL",
			tableName))
		tableNumber++
		return tableName
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s/insert", tc.name), func(t *testing.T) {
			tableName := createTable(tc.schemaTmpl)
			desc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "defaultdb", tableName)
			rp, err := makeSQLLastWriteWinsHandler(ctx, s.ClusterSettings(), map[int32]descpb.TableDescriptor{
				int32(desc.GetID()): *desc.TableDesc(),
			})
			require.NoError(t, err)

			keyValue := replicationtestutils.EncodeKV(t, s.Codec(), desc, tc.row...)
			keyValue.Value.Timestamp = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				return rp.ProcessRow(ctx, txn, keyValue, roachpb.Value{})
			}))
		})
		t.Run(fmt.Sprintf("%s/delete", tc.name), func(t *testing.T) {
			tableName := createTable(tc.schemaTmpl)
			desc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "defaultdb", tableName)
			rp, err := makeSQLLastWriteWinsHandler(ctx, s.ClusterSettings(), map[int32]descpb.TableDescriptor{
				int32(desc.GetID()): *desc.TableDesc(),
			})
			require.NoError(t, err)

			keyValue := replicationtestutils.EncodeKV(t, s.Codec(), desc, tc.row...)
			keyValue.Value.RawBytes = nil
			keyValue.Value.Timestamp = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				return rp.ProcessRow(ctx, txn, keyValue, roachpb.Value{})
			}))
		})
	}
}

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
					switch req := ba.Requests[0].GetInner().(type) {
					case *kvpb.PutRequest:
						if !req.Key.Equal(key) {
							return nil
						}
						numInjectedErrors.Add(1)
						return injectedErr
					case *kvpb.ConditionalPutRequest:
						if !req.Key.Equal(key) {
							return nil
						}
						numInjectedErrors.Add(1)
						return injectedErr
					default:
						return nil
					}
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
	rp, err := makeSQLLastWriteWinsHandler(ctx, s.ClusterSettings(), map[int32]descpb.TableDescriptor{
		int32(desc.GetID()): *desc.TableDesc(),
	})
	require.NoError(b, err)

	// We'll be simulating processing the same INSERT over and over in the loop.
	keyValue := replicationtestutils.EncodeKV(b, s.Codec(), desc, 1, "hello")
	keyValue.Value.Timestamp = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	key = keyValue.Key

	for _, tc := range []struct {
		name      string
		prevValue roachpb.Value
	}{
		{name: "noPrevValue"},
		{name: "withPrevValue", prevValue: keyValue.Value},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			numInjectedErrors.Store(0)
			defer discardWrite.Store(false)
			discardWrite.Store(true)
			var lastTxnErr, lastRowErr error
			var i int
			for i = 0; i < b.N; i++ {
				// We need to start a fresh Txn since we're injecting an error.
				lastTxnErr = s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					// The error is expected here due to injected error.
					lastRowErr = rp.ProcessRow(ctx, txn, keyValue, tc.prevValue)
					keyValue.Value.Timestamp.WallTime += 1
					return nil
				})
			}
			require.Error(b, lastTxnErr)
			require.Error(b, lastRowErr)
			// Sanity check that an error was injected on each iteration.
			require.Equal(b, numInjectedErrors.Load(), int64(i))
		})
	}
}
