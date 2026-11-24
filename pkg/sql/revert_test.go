// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestTableRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, kv := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "test"})
	defer s.Stopper().Stop(context.Background())
	tt := s.ApplicationLayer()
	codec := tt.Codec()
	execCfg := tt.ExecutorConfig().(sql.ExecutorConfig)

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, `CREATE DATABASE IF NOT EXISTS test`)
	db.Exec(t, `CREATE TABLE test (k INT PRIMARY KEY, rev INT DEFAULT 0, INDEX (rev))`)

	// Fill a table with some rows plus some revisions to those rows.
	const numRows = 1000
	db.Exec(t, `INSERT INTO test (k) SELECT generate_series(1, $1)`, numRows)
	db.Exec(t, `UPDATE test SET rev = 1 WHERE k % 3 = 0`)
	db.Exec(t, `DELETE FROM test WHERE k % 10 = 0`)
	db.Exec(t, `ALTER TABLE test SPLIT AT VALUES (30), (300), (501), (700)`)

	var ts string
	var before int
	db.QueryRow(t, `SELECT cluster_logical_timestamp(), xor_agg(k # rev) FROM test`).Scan(&ts, &before)
	targetTime, err := hlc.ParseHLC(ts)
	require.NoError(t, err)

	beforeNumRows := db.QueryStr(t, `SELECT count(*) FROM test`)

	// Make some more edits: delete some rows and edit others, insert into some of
	// the gaps made between previous rows, edit a large swath of rows and add a
	// large swath of new rows as well.
	db.Exec(t, `INSERT INTO test (k, rev) SELECT generate_series(10, $1, 10), 10`, numRows)
	db.Exec(t, `INSERT INTO test (k, rev) SELECT generate_series($1+1, $1+500, 1), 500`, numRows)

	// Delete all keys with values after the targetTime
	desc := desctestutils.TestingGetPublicTableDescriptor(kv, codec, "test", "test")

	predicates := kvpb.DeleteRangePredicates{StartTime: targetTime}
	require.NoError(t, sql.DeleteTableWithPredicate(
		ctx, kv, codec, tt.ClusterSettings(), execCfg.DistSender, desc.GetID(), predicates, 10))

	db.CheckQueryResults(t, `SELECT count(*) FROM test`, beforeNumRows)
}

func TestRollbackRandomTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kv := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "test"})
	defer s.Stopper().Stop(ctx)
	tt := s.ApplicationLayer()

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, `CREATE DATABASE IF NOT EXISTS test`)

	rng, _ := randutil.NewPseudoRand()
	tableName := "rand_table"
	// Filter out columns that can't be fingerprinted:
	// - Virtual columns can cause integer overflow in expressions like a+b.
	// - Non-key-encodable types (e.g. TSVECTOR, TSQUERY, PGVECTOR) can't be
	//   used with crdb_internal.datums_to_bytes() which SHOW FINGERPRINTS uses.
	fingerprintableColumns := randgen.WithColumnFilter(func(col *tree.ColumnTableDef) bool {
		return !col.IsVirtual() &&
			!colinfo.MustBeValueEncoded(col.Type.(*types.T))
	})
	createStmt := randgen.RandCreateTableWithName(
		ctx, rng, tableName, 1,
		[]randgen.TableOption{fingerprintableColumns})
	stmt := tree.SerializeForDisplay(createStmt)
	t.Log(stmt)

	db.Exec(t, stmt)

	_, err := randgen.PopulateTableWithRandData(rng, sqlDB, tableName, 100, nil)
	require.NoError(t, err)

	rollbackTs := tt.Clock().Now()
	fingerprint := db.QueryStr(t, fmt.Sprintf("SHOW FINGERPRINTS FROM TABLE test.%s", tableName))

	_, err = randgen.PopulateTableWithRandData(rng, sqlDB, tableName, 100, nil)
	require.NoError(t, err)

	codec := tt.Codec()
	desc := desctestutils.TestingGetPublicTableDescriptor(kv, codec, "test", tableName)
	predicates := kvpb.DeleteRangePredicates{StartTime: rollbackTs}

	execCfg := tt.ExecutorConfig().(sql.ExecutorConfig)
	require.NoError(t, sql.DeleteTableWithPredicate(
		ctx, kv, codec, tt.ClusterSettings(), execCfg.DistSender, desc.GetID(), predicates, 10))

	afterFingerprint := db.QueryStr(t, fmt.Sprintf("SHOW FINGERPRINTS FROM TABLE test.%s", tableName))
	require.Equal(t, fingerprint, afterFingerprint)
}

// TestDeleteTableWithPredicateNoHang covers two regressions in
// DeleteTableWithPredicate's producer/worker pipeline (#168754): when the
// parent ctx is canceled (e.g. PAUSE on a reverting IMPORT) and when a worker
// returns an error, the producer must observe the cancellation rather than
// block forever on send into a channel whose readers have exited. The cluster
// settings pin parallelism=1 and rangesPerBatch=1 so the producer must
// traverse multiple ranges before the lone worker drains its first request,
// keeping the producer mid-loop when the injected filter fires.
func TestDeleteTableWithPredicateNoHang(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	noopFilter := kvserverbase.ReplicaRequestFilter(
		func(context.Context, *kvpb.BatchRequest) *kvpb.Error { return nil })

	var filterFn atomic.Value
	filterFn.Store(noopFilter)

	s, sqlDB, kv := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "test",
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
					return filterFn.Load().(kvserverbase.ReplicaRequestFilter)(ctx, ba)
				},
			},
		},
	})
	defer s.Stopper().Stop(context.Background())
	tt := s.ApplicationLayer()

	db := sqlutils.MakeSQLRunner(sqlDB)
	db.Exec(t, `CREATE DATABASE IF NOT EXISTS test`)
	db.Exec(t, `SET CLUSTER SETTING bulkio.import.predicate_delete_range_batch_size = 1`)
	db.Exec(t, `SET CLUSTER SETTING bulkio.import.predicate_delete_range_parallelism = 1`)

	for _, tc := range []struct {
		name string
		// run installs the per-subtest filter behavior, drives
		// DeleteTableWithPredicate via invoke, and asserts the outcome.
		// tablePrefix scopes the filter to predicate DeleteRanges targeting
		// the test's table, ignoring any unrelated traffic.
		run func(t *testing.T, tablePrefix roachpb.Key, invoke func(context.Context) error)
	}{
		{
			name: "parent_context_cancellation",
			run: func(t *testing.T, tablePrefix roachpb.Key, invoke func(context.Context) error) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var triggered atomic.Bool
				filterFn.Store(kvserverbase.ReplicaRequestFilter(
					func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
						for _, ru := range ba.Requests {
							if dr, ok := ru.GetInner().(*kvpb.DeleteRangeRequest); ok &&
								dr.Predicates.StartTime.IsSet() &&
								bytes.HasPrefix(dr.Key, tablePrefix) {
								if triggered.CompareAndSwap(false, true) {
									cancel()
								}
								break
							}
						}
						return nil
					}))
				err := runWithHangWatchdog(t, ctx, invoke)
				require.True(t, triggered.Load(), "filter never saw a predicate DeleteRange")
				require.True(t, errors.Is(err, context.Canceled), "want context.Canceled, got: %v", err)
			},
		},
		{
			name: "worker_error_propagation",
			run: func(t *testing.T, tablePrefix roachpb.Key, invoke func(context.Context) error) {
				var triggered atomic.Bool
				filterFn.Store(kvserverbase.ReplicaRequestFilter(
					func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
						for _, ru := range ba.Requests {
							if dr, ok := ru.GetInner().(*kvpb.DeleteRangeRequest); ok &&
								dr.Predicates.StartTime.IsSet() &&
								bytes.HasPrefix(dr.Key, tablePrefix) {
								triggered.Store(true)
								return kvpb.NewError(errors.New("injected delete range error"))
							}
						}
						return nil
					}))
				// Background ctx so any hang can only stem from the worker error.
				err := runWithHangWatchdog(t, context.Background(), invoke)
				require.True(t, triggered.Load(), "filter never saw a predicate DeleteRange")
				require.ErrorContains(t, err, "injected delete range error")
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Reset filter in case a prior subtest left its closure installed
			// (matters under -shuffle, where source order isn't guaranteed).
			filterFn.Store(noopFilter)

			// Reseed: rows on both sides of a captured timestamp, with the
			// post-targetTime writes giving the predicate something to match.
			db.Exec(t, `DROP TABLE IF EXISTS test`)
			db.Exec(t, `CREATE TABLE test (k INT PRIMARY KEY)`)
			db.Exec(t, `INSERT INTO test SELECT generate_series(1, 200)`)
			db.Exec(t, `ALTER TABLE test SPLIT AT SELECT i*10 FROM generate_series(1, 19) AS g(i)`)

			var ts string
			db.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts)
			targetTime, err := hlc.ParseHLC(ts)
			require.NoError(t, err)

			db.Exec(t, `INSERT INTO test SELECT generate_series(201, 400)`)

			desc := desctestutils.TestingGetPublicTableDescriptor(kv, tt.Codec(), "test", "test")
			predicates := kvpb.DeleteRangePredicates{StartTime: targetTime}
			tablePrefix := tt.Codec().TablePrefix(uint32(desc.GetID()))

			tc.run(t, tablePrefix, func(ctx context.Context) error {
				return sql.DeleteTableWithPredicate(ctx, kv, tt.Codec(), tt.ClusterSettings(),
					tt.ExecutorConfig().(sql.ExecutorConfig).DistSender, desc.GetID(), predicates, 10)
			})
		})
	}
}

// runWithHangWatchdog runs do(ctx) in a goroutine and fails the test if it
// doesn't return within 15s. On timeout the ctx is canceled and the goroutine
// is given a brief grace period so a non-buggy call can unwind cleanly,
// avoiding a noisy secondary leaktest failure on top of the t.Fatal.
func runWithHangWatchdog(
	t *testing.T, parent context.Context, do func(context.Context) error,
) error {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- do(ctx) }()
	select {
	case err := <-done:
		return err
	case <-time.After(15 * time.Second):
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
		t.Fatal("DeleteTableWithPredicate hung; producer likely blocked on send")
	}
	panic("unreachable") // t.Fatal aborts the goroutine
}
