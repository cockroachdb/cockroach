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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestLWWInsertQueryGeneration(t *testing.T) {
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
		{
			name:       "multi-column primary key",
			schemaTmpl: `CREATE TABLE %s (pk1 int, pk2 int, payload string, CONSTRAINT "primary-idx" PRIMARY KEY (pk1 ASC, pk2 ASC))`,
			row:        []interface{}{1, 1, "hello"},
		},
	}

	tableNumber := 0
	createTable := func(t *testing.T, stmt string) string {
		tableName := fmt.Sprintf("tab%d", tableNumber)
		runner.Exec(t, fmt.Sprintf(stmt, tableName))
		runner.Exec(t, fmt.Sprintf(
			"ALTER TABLE %s "+lwwColumnAdd,
			tableName))
		tableNumber++
		return tableName
	}

	setup := func(t *testing.T, schemaTmpl string) (*sqlRowProcessor, func(...interface{}) roachpb.KeyValue) {
		tableNameSrc := createTable(t, schemaTmpl)
		tableNameDst := createTable(t, schemaTmpl)
		srcDesc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "defaultdb", tableNameSrc)
		dstDesc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "defaultdb", tableNameDst)
		rp, err := makeSQLLastWriteWinsHandler(ctx, s.ClusterSettings(), map[descpb.ID]sqlProcessorTableConfig{
			dstDesc.GetID(): {
				srcDesc: srcDesc,
			},
		}, s.InternalExecutor().(isql.Executor))
		require.NoError(t, err)
		return rp, func(datums ...interface{}) roachpb.KeyValue {
			kv := replicationtestutils.EncodeKV(t, s.Codec(), srcDesc, datums...)
			kv.Value.Timestamp = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			return kv
		}
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s/insert", tc.name), func(t *testing.T) {
			runner.Exec(t, "SET CLUSTER SETTING logical_replication.consumer.try_optimistic_insert.enabled=true")
			defer runner.Exec(t, "RESET CLUSTER SETTING logical_replication.consumer.try_optimistic_insert.enabled")
			rp, encoder := setup(t, tc.schemaTmpl)
			keyValue := encoder(tc.row...)
			require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				_, err := rp.ProcessRow(ctx, txn, keyValue, roachpb.Value{})
				return err
			}))
		})
		t.Run(fmt.Sprintf("%s/insert-without-optimistic-insert", tc.name), func(t *testing.T) {
			runner.Exec(t, "SET CLUSTER SETTING logical_replication.consumer.try_optimistic_insert.enabled=false")
			defer runner.Exec(t, "RESET CLUSTER SETTING logical_replication.consumer.try_optimistic_insert.enabled")
			rp, encoder := setup(t, tc.schemaTmpl)
			keyValue := encoder(tc.row...)
			require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				_, err := rp.ProcessRow(ctx, txn, keyValue, roachpb.Value{})
				return err
			}))
		})
		t.Run(fmt.Sprintf("%s/delete", tc.name), func(t *testing.T) {
			rp, encoder := setup(t, tc.schemaTmpl)
			keyValue := encoder(tc.row...)
			keyValue.Value.RawBytes = nil
			require.NoError(t, s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
				_, err := rp.ProcessRow(ctx, txn, keyValue, roachpb.Value{})
				return err
			}))
		})
	}
}

func BenchmarkLWWInsertBatch(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	srv, sqlDB, kvDB := serverutils.StartServer(b, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	// batchSize determines the number of INSERTs within a single iteration of
	// the benchmark.
	batchSize := int(flushBatchSize.Get(&s.ClusterSettings().SV))

	runner := sqlutils.MakeSQLRunner(sqlDB)
	tableName := "tab"
	runner.Exec(b, "CREATE TABLE tab (pk INT PRIMARY KEY, payload STRING)")
	runner.Exec(b, "ALTER TABLE tab "+lwwColumnAdd)

	desc := desctestutils.TestingGetPublicTableDescriptor(kvDB, s.Codec(), "defaultdb", tableName)
	// Simulate how we set up the row processor on the main code path.
	sd := sql.NewInternalSessionData(ctx, s.ClusterSettings(), "" /* opName */)
	rp, err := makeSQLLastWriteWinsHandler(ctx, s.ClusterSettings(), map[descpb.ID]sqlProcessorTableConfig{
		desc.GetID(): {
			srcDesc: desc,
		},
	}, s.InternalDB().(isql.DB).Executor(isql.WithSessionData(sd)))
	require.NoError(b, err)

	// In some configs, we'll be simulating processing the same INSERT over and
	// over in the loop.
	sameKV := replicationtestutils.EncodeKV(b, s.Codec(), desc, 1, "hello")
	sameKV.Value.Timestamp = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	advanceTS := func(keyValue roachpb.KeyValue) roachpb.KeyValue {
		keyValue.Value.Timestamp.WallTime += 1
		return keyValue
	}
	// In other configs, we'll be simulating an INSERT with a constantly
	// increasing PK. To make generation of the key easier, we start out with a
	// value that needs 4 bytes when encoded. As a result, we'll have about 2^24
	// values before getting into "5-bytes-encoded integers" land.
	getDifferentKV := func() roachpb.KeyValue {
		differentKV := replicationtestutils.EncodeKV(b, s.Codec(), desc, 0xffff+1, "hello")
		differentKV.Value.Timestamp = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		return differentKV
	}
	// advanceKeyAndTS assumes that it works on the KVs that started with the
	// one returned by getDifferentKV.
	advanceKeyAndTS := func(keyValue roachpb.KeyValue) roachpb.KeyValue {
		// Key is of the form
		//   []byte{240, 137, 248, 1, 0, 0, 136}
		// where:
		// - first two bytes are TableID / IndexID pair
		// - third byte is integer marker for using 4 bytes total
		// - fourth through sixth bytes indicate the integer value
		// - seventh byte is the column family marker.
		// In order to advance the key, we need to increment sixth byte with a
		// carryover into fifth and possibly fourth.
		keyValue.Key[5]++
		if keyValue.Key[5] == 0 {
			keyValue.Key[4]++
			if keyValue.Key[4] == 0 {
				keyValue.Key[3]++
			}
		}
		keyValue.Value.Timestamp.WallTime += 1
		return keyValue
	}
	// The contents of prevValue don't matter as long as RawBytes is non-nil.
	prevValue := roachpb.Value{RawBytes: make([]byte, 1)}
	for _, tc := range []struct {
		name        string
		implicitTxn bool
		// keyValue specifies the KV used on the first ProcessRow call.
		keyValue roachpb.KeyValue
		// afterEachRow will be invoked after each ProcessRow call. It should be
		// a quick function that takes the KV used on a previous call and
		// returns the KV for the next one.
		afterEachRow func(roachpb.KeyValue) roachpb.KeyValue
		// prevValue will be passed to ProcessRow.
		prevValue roachpb.Value
	}{
		// A set of configs that repeatedly processes the same KV resulting in
		// a conflict.
		{
			name:         "conflict/implicit/noPrevValue",
			implicitTxn:  true,
			keyValue:     sameKV,
			afterEachRow: advanceTS,
		},
		{
			name:         "conflict/implicit/withPrevValue",
			implicitTxn:  true,
			keyValue:     sameKV,
			afterEachRow: advanceTS,
			prevValue:    prevValue,
		},
		{
			name:         "conflict/explicit/noPrevValue",
			keyValue:     sameKV,
			afterEachRow: advanceTS,
		},
		{
			name:         "conflict/explicit/withPrevValue",
			keyValue:     sameKV,
			afterEachRow: advanceTS,
			prevValue:    prevValue,
		},
		// A set of configs that processes a new KV on each iteration resulting
		// in a non-conflicting write.
		{
			name:         "noConflict/implicit/noPrevValue",
			implicitTxn:  true,
			keyValue:     getDifferentKV(),
			afterEachRow: advanceKeyAndTS,
		},
		{
			name:         "noConflict/implicit/withPrevValue",
			implicitTxn:  true,
			keyValue:     getDifferentKV(),
			afterEachRow: advanceKeyAndTS,
			prevValue:    prevValue,
		},
		{
			name:         "noConflict/explicit/noPrevValue",
			keyValue:     getDifferentKV(),
			afterEachRow: advanceKeyAndTS,
		},
		{
			name:         "noConflict/explicit/withPrevValue",
			keyValue:     getDifferentKV(),
			afterEachRow: advanceKeyAndTS,
			prevValue:    prevValue,
		},
	} {
		b.Run(tc.name, func(b *testing.B) {
			// Ensure that any previous writes are deleted.
			runner.Exec(b, "DELETE FROM tab WHERE true")
			b.ResetTimer()
			b.ReportAllocs()
			var lastRowErr error
			keyValue := tc.keyValue
			if tc.implicitTxn {
			OUTER:
				for i := 0; i < b.N; i++ {
					for j := 0; j < batchSize; j++ {
						_, lastRowErr = rp.ProcessRow(ctx, nil /* txn */, keyValue, tc.prevValue)
						if lastRowErr != nil {
							break OUTER
						}
						keyValue = tc.afterEachRow(keyValue)
					}
				}
			} else {
				var lastTxnErr error
				for i := 0; i < b.N; i++ {
					lastTxnErr = s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
						for j := 0; j < batchSize; j++ {
							_, lastRowErr = rp.ProcessRow(ctx, txn, keyValue, tc.prevValue)
							if lastRowErr != nil {
								return lastRowErr
							}
							keyValue = tc.afterEachRow(keyValue)
						}
						return nil
					}, isql.WithSessionData(sd))
				}
				require.NoError(b, lastTxnErr)
			}
			require.NoError(b, lastRowErr)
		})
	}
}
