// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlccl_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestReadCommittedStmtRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params := base.TestServerArgs{}

	var trapReadCommittedWrites atomic.Bool
	var trappedReadCommittedWritesOnce sync.Once
	finishedReadCommittedScans := make(chan struct{})
	finishedExternalTxn := make(chan struct{})
	var sawWriteTooOldError atomic.Bool
	var codec keys.SQLCodec
	var kvTableId uint32

	filterFunc := func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
		if !trapReadCommittedWrites.Load() {
			return nil
		}
		if ba.Txn == nil || ba.Txn.IsoLevel != isolation.ReadCommitted {
			return nil
		}
		for _, arg := range ba.Requests {
			if req := arg.GetInner(); req.Method() == kvpb.Put {
				put := req.(*kvpb.PutRequest)
				// Only count writes to the kv table.
				_, tableID, err := codec.DecodeTablePrefix(put.Key)
				if err != nil || tableID != kvTableId {
					return nil
				}
				trappedReadCommittedWritesOnce.Do(func() {
					close(finishedReadCommittedScans)
					<-finishedExternalTxn
				})
			}
		}

		return nil
	}
	params.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingRequestFilter: filterFunc,
	}
	params.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
		OnReadCommittedStmtRetry: func(retryReason error) {
			if strings.Contains(retryReason.Error(), "WriteTooOldError") {
				sawWriteTooOldError.Store(true)
			}
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)
	codec = s.ApplicationLayer().Codec()

	// Create a table with three rows. Note that k is not the primary key,
	// so locking won't be pushed into the initial scan of the UPDATEs below.
	_, err := sqlDB.Exec(`CREATE TABLE kv (k TEXT, v INT) WITH (sql_stats_automatic_collection_enabled = false);`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO kv VALUES ('a', 1);`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO kv VALUES ('b', 2);`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO kv VALUES ('c', 3);`)
	require.NoError(t, err)
	err = sqlDB.QueryRow("SELECT 'kv'::regclass::oid").Scan(&kvTableId)
	require.NoError(t, err)

	g := ctxgroup.WithContext(ctx)

	// Create a read committed transaction that writes to key "a" in its first
	// statement before hitting a retryable error during the second statement.
	tx, err := sqlDB.BeginTx(ctx, &gosql.TxOptions{Isolation: gosql.LevelReadCommitted})
	require.NoError(t, err)
	// Write to "a" in the first statement.
	_, err = tx.Exec(`UPDATE kv SET v = v+10 WHERE k = 'a'`)
	require.NoError(t, err)

	// Start blocking writes in the read committed transaction.
	trapReadCommittedWrites.Store(true)

	// Perform a series of reads and writes in the second statement.
	// Read from "b" and "c" to establish refresh spans.
	// Write to "b" in the transaction, without issue.
	// Write to "c" in the transaction to hit the write-write conflict, which
	// causes the statement to need to retry.
	g.GoCtx(func(ctx context.Context) error {
		_, err = tx.Exec(`UPDATE kv SET v = v+10 WHERE k = 'b' OR k = 'c'`)
		return err
	})

	// Wait for the table to be scanned first.
	<-finishedReadCommittedScans

	// Write to "c" outside the transaction to create a write-write conflict.
	_, err = sqlDB.Exec(`UPDATE kv SET v = v+10 WHERE k = 'c'`)
	require.NoError(t, err)

	// Now let the READ COMMITTED write go through. It should encounter a
	// WriteTooOldError and retry.
	close(finishedExternalTxn)

	err = g.Wait()
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	sqlutils.MakeSQLRunner(sqlDB).CheckQueryResults(t, `SELECT k, v FROM kv ORDER BY k`, [][]string{
		{"a", "11"},
		{"b", "12"},
		{"c", "23"},
	})
	require.True(t, sawWriteTooOldError.Load())
}

// TestReadCommittedReadTimestampNotSteppedOnCommit verifies that the read
// timestamp of a read committed transaction is stepped between SQL statements,
// but not before commit.
func TestReadCommittedReadTimestampNotSteppedOnCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Keep track of the read timestamps of the read committed transaction during
	// each KV operation.
	var txnReadTimestamps []hlc.Timestamp
	var txnShouldParallelCommit bool
	filterFunc := func(ctx context.Context, ba *kvpb.BatchRequest, _ *kvpb.BatchResponse) *kvpb.Error {
		if ba.Txn == nil || ba.Txn.IsoLevel != isolation.ReadCommitted {
			return nil
		}
		req := ba.Requests[len(ba.Requests)-1]

		var recordRead bool
		switch req.GetInner().Method() {
		case kvpb.ConditionalPut:
			recordRead = true
			txnShouldParallelCommit = true
		case kvpb.Get:
			recordRead = req.GetGet().KeyLockingStrength == lock.Exclusive
		case kvpb.EndTxn:
			if txnShouldParallelCommit {
				recordRead = req.GetEndTxn().IsParallelCommit()
			} else {
				recordRead = true
			}
		default:
			recordRead = false
		}
		if recordRead {
			t.Logf("recording timestamp for %s", req)
			txnReadTimestamps = append(txnReadTimestamps, ba.Txn.ReadTimestamp)
		} else {
			t.Logf("not recording timestamp for %s", req)
		}
		return nil
	}

	ctx := context.Background()
	params := base.TestServerArgs{}
	params.Knobs.Store = &kvserver.StoreTestingKnobs{
		// NOTE: we use a TestingResponseFilter and not a TestingRequestFilter to
		// avoid potential flakiness from requests which are redirected or retried.
		TestingResponseFilter: filterFunc,
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec(`CREATE TABLE kv (k TEXT, v INT) WITH (sql_stats_automatic_collection_enabled = false);`)
	require.NoError(t, err)

	// Create a read committed transaction that writes to three rows in three
	// different statements and then commits.
	tx, err := sqlDB.BeginTx(ctx, &gosql.TxOptions{Isolation: gosql.LevelReadCommitted})
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO kv VALUES ('a', 1);`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO kv VALUES ('b', 2);`)
	require.NoError(t, err)
	_, err = tx.Exec(`INSERT INTO kv VALUES ('c', 3);`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit())

	// Verify that the transaction's read timestamp was not stepped on commit but
	// was stepped between every other statement.
	require.Len(t, txnReadTimestamps, 4)
	require.True(t, txnReadTimestamps[0].Less(txnReadTimestamps[1]))
	require.True(t, txnReadTimestamps[1].Less(txnReadTimestamps[2]))
	require.True(t, txnReadTimestamps[2].Equal(txnReadTimestamps[3]))
}

// TestReadCommittedVolatileUDF verifies that volatile UDFs running under
// READ COMMITTED do not have their external read timestamp incremented more
// than they should be.
func TestReadCommittedVolatileUDF(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var readCommittedRetryCount atomic.Int64
	var trapRCTxn atomic.Bool
	var tableID int
	rcTxnBlocked, unblockRCTxn := make(chan struct{}, 1), make(chan struct{})
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				OnReadCommittedStmtRetry: func(retryReason error) {
					// Track retries since we don't want them to happen in this
					// test, since they would change the read timestamp.
					readCommittedRetryCount.Add(1)
				},
			},
			Store: &kvserver.StoreTestingKnobs{
				TestingRequestFilter: func(_ context.Context, req *kvpb.BatchRequest) *kvpb.Error {
					if !trapRCTxn.Load() {
						return nil
					}
					if len(req.Requests) != 2 {
						return nil
					}
					for _, r := range req.Requests {
						get, ok := r.GetInner().(*kvpb.GetRequest)
						if !ok {
							return nil
						}
						if !strings.Contains(get.Key.String(), fmt.Sprintf("/Table/%d", tableID)) {
							return nil
						}
					}
					rcTxnBlocked <- struct{}{}
					<-unblockRCTxn
					return nil
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	_, err := sqlDB.Exec(`CREATE TABLE kv (k TEXT PRIMARY KEY, v INT) WITH (sql_stats_automatic_collection_enabled = false);`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO kv VALUES ('a', 10);`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`CREATE FUNCTION f() RETURNS INT AS $$ SELECT v FROM kv WHERE k = 'a' OR k = 'b' ORDER BY v LIMIT 1 FOR SHARE $$ LANGUAGE SQL VOLATILE;`)
	require.NoError(t, err)
	err = sqlDB.QueryRow("SELECT 'kv'::REGCLASS::OID").Scan(&tableID)
	require.NoError(t, err)

	g := ctxgroup.WithContext(ctx)

	// Create a transaction that takes a lock on key "a". This will end up getting
	// rolledback, but the important thing is that it makes txReadCommitted block.
	txSerializable, err := sqlDB.BeginTx(ctx, &gosql.TxOptions{Isolation: gosql.LevelSerializable})
	require.NoError(t, err)
	_, err = txSerializable.Exec(`UPDATE kv SET v = 5 WHERE k = 'a'`)
	require.NoError(t, err)

	// Start a READ COMMITTED transaction that is blocked on txSerializable, and
	// which tries to read a key that does not exist yet using a UDF.
	trapRCTxn.Store(true)
	txReadCommitted, err := sqlDB.BeginTx(ctx, &gosql.TxOptions{Isolation: gosql.LevelReadCommitted})
	require.NoError(t, err)
	var executedUDF atomic.Bool
	g.GoCtx(func(ctx context.Context) error {
		// The READ COMMITTED transaction invokes the function twice. Both
		// invocations should use a read timestamp that cannot see row 'b'.
		var udfResult1, udfResult2 int
		if err := txReadCommitted.QueryRow(`SELECT f(), f()`).Scan(&udfResult1, &udfResult2); err != nil {
			return err
		}
		if udfResult1 != 10 {
			return errors.Newf("expected first invocation result to be 10; got %d", udfResult1)
		}
		if udfResult2 != 10 {
			return errors.Newf("expected second invocation result to be 10; got %d", udfResult2)
		}
		executedUDF.Store(true)
		return nil
	})

	// In a third transaction, add another row to the table after confirming that
	// txReadCommitted is blocked. This row should not be visible to
	// txReadCommitted's UDF.
	<-rcTxnBlocked
	trapRCTxn.Store(false)
	close(unblockRCTxn)
	_, err = sqlDB.Exec(`INSERT INTO kv VALUES ('b', 2)`)
	require.NoError(t, err)

	require.False(t, executedUDF.Load())
	require.NoError(t, txSerializable.Rollback())
	require.NoError(t, g.Wait())
	require.NoError(t, txReadCommitted.Commit())
	require.Equal(t, int64(0), readCommittedRetryCount.Load())
}
