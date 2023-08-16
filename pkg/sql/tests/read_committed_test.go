// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	gosql "database/sql"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestReadCommittedStmtRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	params := base.TestServerArgs{}

	var readCommittedWriteCount atomic.Int64
	var finishedReadCommittedScans sync.WaitGroup
	finishedReadCommittedScans.Add(1)
	var finishedExternalTxn sync.WaitGroup
	finishedExternalTxn.Add(1)
	var sawWriteTooOldError atomic.Bool
	var codec keys.SQLCodec
	var kvTableId uint32

	filterFunc := func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
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
				// Because of the queries the test executes below, we know that before
				// the second read committed write begins, the read committed scans
				// will have finished.
				if newCount := readCommittedWriteCount.Add(1); newCount == 2 {
					finishedReadCommittedScans.Done()
					finishedExternalTxn.Wait()
				}
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
	_, err := sqlDB.Exec(`CREATE TABLE kv (k TEXT, v INT);`)
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
	finishedReadCommittedScans.Wait()

	// Write to "c" outside the transaction to create a write-write conflict.
	_, err = sqlDB.Exec(`UPDATE kv SET v = v+10 WHERE k = 'c'`)
	require.NoError(t, err)

	// Now let the READ COMMITTED write go through. It should encounter a
	// WriteTooOldError and retry.
	finishedExternalTxn.Done()

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
