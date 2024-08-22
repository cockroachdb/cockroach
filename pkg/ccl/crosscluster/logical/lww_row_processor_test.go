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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
		rp, err := makeSQLProcessor(ctx, s.ClusterSettings(), map[descpb.ID]sqlProcessorTableConfig{
			dstDesc.GetID(): {
				srcDesc: srcDesc,
			},
		}, jobspb.JobID(1), s.InternalExecutor().(isql.Executor))
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
	rp, err := makeSQLProcessor(ctx, s.ClusterSettings(), map[descpb.ID]sqlProcessorTableConfig{
		desc.GetID(): {
			srcDesc: desc,
		},
	}, jobspb.JobID(1), s.InternalDB().(isql.DB).Executor(isql.WithSessionData(sd)))
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

// TestLWWConflictResolution tests how insert conflicts are handled under the default
// last write wins mode. The test cases are as follows:
// 1. The incoming row to the processor is older than the current row
// 2. The incoming row to the processor is newer than the current row
// 3. The current row is a local write (crdb_replication_origin_timestamp is NULL)
// 4. Both src and dest tables receive a write before one can be propagated to the other
// All of those combinations are tested with both optimistic inserts and standard inserts.
// The tests are also run with both the SQL row processor and the KV row processor. Note
// that currently, KV row proc write conflicts result in a fallback to the SQL row processor.
func TestLWWConflictResolution(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	runner := sqlutils.MakeSQLRunner(sqlDB)

	// Create new tables for each test to prevent conflicts between tests
	tableNumber := 0
	createTable := func(t *testing.T) string {
		tableName := fmt.Sprintf("tab%d", tableNumber)
		runner.Exec(t, fmt.Sprintf(`CREATE TABLE %s (pk int primary key, payload string)`, tableName))
		runner.Exec(t, fmt.Sprintf(
			"ALTER TABLE %s "+lwwColumnAdd,
			tableName))
		tableNumber++
		return tableName
	}

	setup := func(t *testing.T, useKVProc bool) (string, RowProcessor, func(hlc.Timestamp, ...interface{}) roachpb.KeyValue) {
		tableNameSrc := createTable(t)
		tableNameDst := createTable(t)
		srcDesc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "defaultdb", tableNameSrc)
		dstDesc := desctestutils.TestingGetPublicTableDescriptor(s.DB(), s.Codec(), "defaultdb", tableNameDst)

		// We need the SQL row processor even when testing the KW row processor since it's the fallback
		var rp RowProcessor
		rp, err := makeSQLProcessor(ctx, s.ClusterSettings(), map[descpb.ID]sqlProcessorTableConfig{
			dstDesc.GetID(): {
				srcDesc: srcDesc,
			},
		}, jobspb.JobID(1), s.InternalExecutor().(isql.Executor))
		require.NoError(t, err)

		if useKVProc {
			rp, err = newKVRowProcessor(ctx,
				&execinfra.ServerConfig{
					DB:           s.InternalDB().(descs.DB),
					LeaseManager: s.LeaseManager(),
				}, &eval.Context{
					Codec:    s.Codec(),
					Settings: s.ClusterSettings(),
				}, map[descpb.ID]sqlProcessorTableConfig{
					dstDesc.GetID(): {
						srcDesc: srcDesc,
					},
				},
				rp.(*sqlRowProcessor))
			require.NoError(t, err)
		}
		return tableNameDst, rp, func(timestamp hlc.Timestamp, datums ...interface{}) roachpb.KeyValue {
			kv := replicationtestutils.EncodeKV(t, s.Codec(), srcDesc, datums...)
			kv.Value.Timestamp = timestamp
			return kv
		}
	}

	insertRow := func(rp RowProcessor, keyValue roachpb.KeyValue, prevValue roachpb.Value) error {
		return s.InternalDB().(isql.DB).Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			_, err := rp.ProcessRow(ctx, txn, keyValue, prevValue)
			return err
		})
	}

	timeNow := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	timeOneDayForward := hlc.Timestamp{WallTime: timeutil.Now().Add(time.Hour * 24).UnixNano()}
	row1 := []interface{}{1, "row1"}
	row2 := []interface{}{1, "row2"}
	row3 := []interface{}{1, "row3"}

	testutils.RunTrueAndFalse(t, "useKVProc", func(t *testing.T, useKVProc bool) {
		testutils.RunTrueAndFalse(t, "optimistic_insert", func(t *testing.T, optimisticInsert bool) {

			t.Run("cross-cluster-write", func(t *testing.T) {
				runner.Exec(t, fmt.Sprintf("SET CLUSTER SETTING logical_replication.consumer.try_optimistic_insert.enabled=%t", optimisticInsert))
				defer runner.Exec(t, "RESET CLUSTER SETTING logical_replication.consumer.try_optimistic_insert.enabled")
				tableNameDst, rp, encoder := setup(t, useKVProc)

				runner.Exec(t, fmt.Sprintf("INSERT INTO %s VALUES ($1, $2)", tableNameDst), row1...)

				keyValue2 := encoder(timeOneDayForward, row2...)
				require.NoError(t, insertRow(rp, keyValue2, roachpb.Value{}))

				expectedRows := [][]string{
					{"1", "row2"},
				}
				runner.CheckQueryResults(t, fmt.Sprintf("SELECT * from %s", tableNameDst), expectedRows)
			})

			t.Run("sql-conflict-incoming-newer", func(t *testing.T) {
				runner.Exec(t, fmt.Sprintf("SET CLUSTER SETTING logical_replication.consumer.try_optimistic_insert.enabled=%t", optimisticInsert))
				defer runner.Exec(t, "RESET CLUSTER SETTING logical_replication.consumer.try_optimistic_insert.enabled")
				tableNameDst, rp, encoder := setup(t, useKVProc)

				keyValue1 := encoder(timeNow, row1...)
				require.NoError(t, insertRow(rp, keyValue1, roachpb.Value{}))

				keyValue2 := encoder(timeOneDayForward, row2...)
				require.NoError(t, insertRow(rp, keyValue2, roachpb.Value{}))

				expectedRows := [][]string{
					{"1", "row2"},
				}
				runner.CheckQueryResults(t, fmt.Sprintf("SELECT * from %s", tableNameDst), expectedRows)
			})

			t.Run("sql-conflict-incoming-older", func(t *testing.T) {
				runner.Exec(t, fmt.Sprintf("SET CLUSTER SETTING logical_replication.consumer.try_optimistic_insert.enabled=%t", optimisticInsert))
				defer runner.Exec(t, "RESET CLUSTER SETTING logical_replication.consumer.try_optimistic_insert.enabled")
				tableNameDst, rp, encoder := setup(t, useKVProc)

				keyValue1 := encoder(timeOneDayForward, row1...)
				require.NoError(t, insertRow(rp, keyValue1, roachpb.Value{}))

				keyValue2 := encoder(timeNow, row2...)
				require.NoError(t, insertRow(rp, keyValue2, roachpb.Value{}))

				expectedRows := [][]string{
					{"1", "row1"},
				}
				runner.CheckQueryResults(t, fmt.Sprintf("SELECT * from %s", tableNameDst), expectedRows)
			})

			// From the perspective of the row processor, once the first row is processed, the next incoming event from the
			// src rangefeed should have a "previous row" that matches the row currently in the cluster. If writes on the src
			// and dest occur too close together, both tables will attempt to propagate to the other, and the winner of the
			// conflict will depend on the MVCC timestamp just like the cross cluster write scenario
			t.Run("outdated-write-conflict", func(t *testing.T) {
				runner.Exec(t, fmt.Sprintf("SET CLUSTER SETTING logical_replication.consumer.try_optimistic_insert.enabled=%t", optimisticInsert))
				defer runner.Exec(t, "RESET CLUSTER SETTING logical_replication.consumer.try_optimistic_insert.enabled")
				tableNameDst, rp, encoder := setup(t, useKVProc)

				keyValue1 := encoder(timeNow, row1...)
				require.NoError(t, insertRow(rp, keyValue1, roachpb.Value{}))

				runner.Exec(t, fmt.Sprintf("UPSERT INTO %s VALUES ($1, $2)", tableNameDst), row2...)

				keyValue3 := encoder(timeOneDayForward, row3...)
				require.NoError(t, insertRow(rp, keyValue3, keyValue1.Value))

				expectedRows := [][]string{
					{"1", "row3"},
				}
				runner.CheckQueryResults(t, fmt.Sprintf("SELECT * from %s", tableNameDst), expectedRows)
			})
		})
	})
}
