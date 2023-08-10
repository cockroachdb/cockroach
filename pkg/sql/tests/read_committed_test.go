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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
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

	var readCommittedWriteCount int
	var waitBeforeSerializableWriteToC sync.WaitGroup
	waitBeforeSerializableWriteToC.Add(1)
	var waitBeforeSecondReadCommittedWrite sync.WaitGroup
	waitBeforeSecondReadCommittedWrite.Add(1)
	var sawWriteTooOldError bool

	filterFunc := func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
		if ba.Txn == nil || ba.Txn.IsoLevel != isolation.ReadCommitted {
			return nil
		}
		for _, arg := range ba.Requests {
			if req := arg.GetInner(); req.Method() == kvpb.Put {
				readCommittedWriteCount++
				if readCommittedWriteCount == 2 {
					waitBeforeSerializableWriteToC.Done()
					waitBeforeSecondReadCommittedWrite.Wait()
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
				sawWriteTooOldError = true
			}
		},
	}
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// Create a table with three rows.
	_, err := sqlDB.Exec(`CREATE TABLE kv (k TEXT, v INT);`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO kv VALUES ('a', 1);`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO kv VALUES ('b', 2);`)
	require.NoError(t, err)
	_, err = sqlDB.Exec(`INSERT INTO kv VALUES ('c', 3);`)
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
	waitBeforeSerializableWriteToC.Wait()

	// Write to "c" outside the transaction to create a write-write conflict.
	_, err = sqlDB.Exec(`UPDATE kv SET v = v+10 WHERE k = 'c'`)
	require.NoError(t, err)

	// Now let the READ COMMITTED write go through. It should encounter a
	// WriteTooOldError and retry.
	waitBeforeSecondReadCommittedWrite.Done()

	err = g.Wait()
	require.NoError(t, err)

	require.NoError(t, tx.Commit())

	sqlutils.MakeSQLRunner(sqlDB).CheckQueryResults(t, `SELECT k, v FROM kv ORDER BY k`, [][]string{
		{"a", "11"},
		{"b", "12"},
		{"c", "23"},
	})
	require.True(t, sawWriteTooOldError)
}
