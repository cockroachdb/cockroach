// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestTxnCoordSenderWriteBufferingDisablesPipelining verifies that enabling
// write buffering disables pipelining.
func TestTxnCoordSenderWriteBufferingDisablesPipelining(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	distSender := s.DistSenderI().(*DistSender)
	batchCount := 0
	var calls []kvpb.Method
	var senderFn kv.SenderFunc = func(
		ctx context.Context, ba *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error) {
		batchCount++
		calls = append(calls, ba.Methods()...)
		if et, ok := ba.GetArg(kvpb.EndTxn); ok {
			// Ensure that no transactions enter a STAGING state.
			et.(*kvpb.EndTxnRequest).InFlightWrites = nil
		}
		return distSender.Send(ctx, ba)
	}

	st := s.ClusterSettings()
	tsf := NewTxnCoordSenderFactory(TxnCoordSenderFactoryConfig{
		AmbientCtx: s.AmbientCtx(),
		Settings:   st,
		Clock:      s.Clock(),
		Stopper:    s.Stopper(),
		// Disable transaction heartbeats so that they don't disrupt our attempt to
		// track the requests issued by the transactions.
		HeartbeatInterval: -1,
	}, senderFn)
	db := kv.NewDB(s.AmbientCtx(), tsf, s.Clock(), s.Stopper())

	// Disable scan transforms so that we can force a write that _would have_ been
	// buffered.
	require.NoError(t, db.Put(ctx, "test-key-a", "hello"))

	bufferedWritesScanTransformEnabled.Override(ctx, &st.SV, false)
	BufferedWritesMaxBufferSize.Override(ctx, &st.SV, defaultBufferSize)

	// Without write buffering
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetBufferedWritesEnabled(false)
		if err := txn.Put(ctx, "test-key-c", "hello"); err != nil {
			return err
		}
		_, err := txn.ScanForUpdate(ctx, "test-key", "test-key-b", 10, kvpb.GuaranteedDurability)
		return err
	}))

	// With write buffering.
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetBufferedWritesEnabled(true)
		if err := txn.Put(ctx, "test-key-c", "hello"); err != nil {
			return err
		}
		_, err := txn.ScanForUpdate(ctx, "test-key", "test-key-b", 10, kvpb.GuaranteedDurability)
		return err
	}))

	require.Equal(t, 1+3+2, batchCount)
	require.Equal(t, []kvpb.Method{
		// The initial setup
		kvpb.Put,
		// The first transaction without write buffering
		kvpb.Put, kvpb.Scan, kvpb.QueryIntent, kvpb.QueryIntent, kvpb.EndTxn,
		// The second transaction with write buffering
		kvpb.Scan, kvpb.Put, kvpb.EndTxn,
	}, calls)
}

// TestTxnCoordSenderWriteBufferingReEnablesPipelining verifies that pipelining
// is re-enabled after a mid-transaction flush.
func TestTxnCoordSenderWriteBufferingReEnablesPipelining(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	distSender := s.DistSenderI().(*DistSender)
	batchCount := 0
	var calls []kvpb.Method
	var senderFn kv.SenderFunc = func(
		ctx context.Context, ba *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error) {
		batchCount++
		t.Logf("Batch: %#+v", ba.Methods())
		calls = append(calls, ba.Methods()...)
		if et, ok := ba.GetArg(kvpb.EndTxn); ok {
			// Ensure that no transactions enter a STAGING state.
			et.(*kvpb.EndTxnRequest).InFlightWrites = nil
		}
		return distSender.Send(ctx, ba)
	}

	st := s.ClusterSettings()
	BufferedWritesMaxBufferSize.Override(ctx, &st.SV, defaultBufferSize)

	tsf := NewTxnCoordSenderFactory(TxnCoordSenderFactoryConfig{
		AmbientCtx: s.AmbientCtx(),
		Settings:   st,
		Clock:      s.Clock(),
		Stopper:    s.Stopper(),
		// Disable transaction heartbeats so that they don't disrupt our attempt to
		// track the requests issued by the transactions.
		HeartbeatInterval: -1,
	}, senderFn)
	db := kv.NewDB(s.AmbientCtx(), tsf, s.Clock(), s.Stopper())

	require.NoError(t, db.Put(ctx, "test-key-a", "hello"))
	require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetBufferedWritesEnabled(true)
		if err := txn.Put(ctx, "test-key-c", "hello"); err != nil {
			return err
		}
		if _, err := txn.DelRange(ctx, "test-key", "test-key-d", true); err != nil {
			return err
		}
		if err := txn.Put(ctx, "test-key-a", "hello"); err != nil {
			return err
		}
		return nil
	}))

	require.Equal(t, 5, batchCount)
	require.Equal(t, []kvpb.Method{
		// The initial setup
		kvpb.Put,
		// The first (buffered) Put and the DeleteRange that flushes the buffer,
		// sent as separate batches.
		kvpb.Put,
		kvpb.DeleteRange,
		// The second (pipelined) Put
		kvpb.Put,
		// EndTxn with the QueryIntent because pipelining was turned back on.
		kvpb.QueryIntent, kvpb.EndTxn,
	}, calls)
}

// TestTxnWriteBufferMidTxnFlushFailurePoisonsTxn verifies that a transaction
// whose buffered writes are discarded by a failed mid-transaction flush cannot
// continue via a savepoint rollback.
//
// The flush discards the client-side buffer before sending the flushed writes
// to KV. If the flush then fails with an error that normally permits the
// transaction to continue (such as a WriteIntentError), the buffered writes no
// longer exist anywhere: not in the buffer and not (fully) at the server.
// Allowing a savepoint rollback to proceed would let the transaction commit
// without writes it acknowledged before the savepoint. Instead, the
// transaction must be moved to an error state that only permits a full
// rollback.
//
// Mid-transaction flush batches are stripped of statement-scoped header
// options such as lock timeouts (see clearBatchRequestOptions), so no known
// code path currently produces a WriteIntentError for such a flush. The test
// injects one underneath the TxnCoordSender to verify the poisoning
// regardless.
func TestTxnWriteBufferMidTxnFlushFailurePoisonsTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	st := s.ClusterSettings()
	// The metamorphic default for the max buffer size can be tiny, which would
	// flush the buffer before the test wants it to.
	BufferedWritesMaxBufferSize.Override(ctx, &st.SV, defaultBufferSize)

	// Fail any batch that carries a Put with a WriteIntentError. The only such
	// batch in this test is the mid-transaction buffer flush.
	distSender := s.DistSenderI().(*DistSender)
	injectedErrs := 0
	var senderFn kv.SenderFunc = func(
		ctx context.Context, ba *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, *kvpb.Error) {
		if _, ok := ba.GetArg(kvpb.Put); ok {
			injectedErrs++
			return nil, kvpb.NewError(&kvpb.WriteIntentError{
				Reason: kvpb.WriteIntentError_REASON_LOCK_TIMEOUT,
			})
		}
		return distSender.Send(ctx, ba)
	}
	tsf := NewTxnCoordSenderFactory(TxnCoordSenderFactoryConfig{
		AmbientCtx: s.AmbientCtx(),
		Settings:   st,
		Clock:      s.Clock(),
		Stopper:    s.Stopper(),
		// Disable transaction heartbeats. The heartbeat loop would start with
		// the flush batch, but the injected error means no txn record is ever
		// written for it to maintain.
		HeartbeatInterval: -1,
	}, senderFn)
	db := kv.NewDB(s.AmbientCtx(), tsf, s.Clock(), s.Stopper())

	keyA := "flush-fail-a"
	keyB := "flush-fail-b"

	txn := db.NewTxn(ctx, "flush-fail")
	txn.SetBufferedWritesEnabled(true)
	// Create an initial savepoint before the transaction performs any
	// operations, mirroring SAVEPOINT cockroach_restart.
	initSp, err := txn.CreateSavepoint(ctx)
	require.NoError(t, err)
	require.True(t, initSp.Initial())

	// This write is buffered on the client.
	require.NoError(t, txn.Put(ctx, keyA, "v1"))

	sp, err := txn.CreateSavepoint(ctx)
	require.NoError(t, err)

	// Disabling write buffering forces a flush on the next batch. The flushed
	// Put(keyA) fails with the injected WriteIntentError.
	txn.SetBufferedWritesEnabled(false)
	_, err = txn.Get(ctx, keyB)
	require.Error(t, err)
	require.Equal(t, 1, injectedErrs)
	// The error is wrapped so that it no longer appears recoverable, but the
	// cause remains in the chain for pgcode mapping.
	require.True(t, errors.HasType(err, (*kvpb.WriteIntentError)(nil)), "expected WriteIntentError, got %v", err)
	require.ErrorContains(t, err, "previously buffered write failed")

	// The failed flush discarded the buffered writes, so the transaction must
	// not be allowed to recover via a savepoint rollback.
	err = txn.RollbackToSavepoint(ctx, sp)
	require.ErrorContains(t, err, "cannot rollback to savepoint after error")

	// This includes initial savepoints (SAVEPOINT cockroach_restart), matching
	// the treatment of every other error that moves the transaction to an
	// error state.
	err = txn.RollbackToSavepoint(ctx, initSp)
	require.ErrorContains(t, err, "cannot rollback to savepoint after error")

	// Other requests are rejected as well.
	_, err = txn.Get(ctx, keyB)
	require.Error(t, err)

	// So is an attempt to commit.
	err = txn.Commit(ctx)
	require.Error(t, err)

	// A full rollback is still allowed.
	require.NoError(t, txn.Rollback(ctx))
}

// TestTxnWriteBufferFlushFailureRequiresRestart verifies that when a buffer
// flush fails with a retryable error, the retry requires the transaction to
// restart from the beginning, even under isolation levels that normally
// permit per-statement retries.
//
// Under Read Committed, a retryable error normally only advances the read
// timestamp: the client is expected to roll back to a statement savepoint and
// retry the current statement, keeping all prior writes. But a failed flush
// has already discarded the buffered writes, so a per-statement retry would
// let the transaction commit without them. The analogous situation for locked
// buffered writes is handled by making ExclusionViolationError always require
// a restart; this covers buffered writes that carry no lock.
func TestTxnWriteBufferFlushFailureRequiresRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	st := s.ClusterSettings()
	// See TestTxnWriteBufferMidTxnFlushFailurePoisonsTxn.
	BufferedWritesMaxBufferSize.Override(ctx, &st.SV, defaultBufferSize)

	db := s.DB()

	getStr := func(t *testing.T, key string) string {
		t.Helper()
		res, err := db.Get(ctx, key)
		require.NoError(t, err)
		resBytes, err := res.Value.GetBytes()
		require.NoError(t, err)
		return string(resBytes)
	}

	testutils.RunTrueAndFalse(t, "retry", func(t *testing.T, retry bool) {
		prefix := fmt.Sprintf("flush-wto-%t-", retry)
		keyR := prefix + "r"
		keyK := prefix + "k"
		keyB := prefix + "b"

		txn := db.NewTxn(ctx, "flush-wto")
		require.NoError(t, txn.SetIsoLevel(isolation.ReadCommitted))
		txn.SetBufferedWritesEnabled(true)
		// With stepping disabled, a Read Committed transaction advances its read
		// snapshot on every batch, which would let the flush below evaluate above
		// the conflicting writes and succeed. Enable stepping to hold the read
		// snapshot, like the SQL layer does.
		_ = txn.ConfigureStepping(ctx, kv.SteppingEnabled)

		// Read keyR to record a refresh span. The conflicting write on keyR below
		// makes the refresh that would otherwise transparently rescue the flush's
		// WriteTooOldError fail, so the error reaches the transaction coordinator.
		_, err := txn.Get(ctx, keyR)
		require.NoError(t, err)

		// This write is buffered on the client.
		require.NoError(t, txn.Put(ctx, keyK, "v1"))

		// Conflicting committed writes above the transaction's read timestamp.
		require.NoError(t, db.Put(ctx, keyR, "conflict"))
		require.NoError(t, db.Put(ctx, keyK, "conflict"))

		// Disabling write buffering forces a flush on the next batch. The flushed
		// Put(keyK) fails with a WriteTooOldError.
		txn.SetBufferedWritesEnabled(false)
		b := txn.NewBatch()
		b.Get(keyB)
		err = txn.Run(ctx, b)
		require.Error(t, err)

		retryErr := (*kvpb.TransactionRetryWithProtoRefreshError)(nil)
		require.True(t, errors.As(err, &retryErr), "expected retryable error, got %v", err)
		require.True(t, retryErr.TxnMustRestartFromBeginning(),
			"retry after a failed flush must restart from the beginning")

		if retry {
			// The transaction is still usable via the restart path. Prepare it
			// for retry and re-issue the write from the beginning, as a client
			// honoring TxnMustRestartFromBeginning would.
			require.NoError(t, txn.PrepareForRetry(ctx))
			require.NoError(t, txn.Put(ctx, keyK, "v2"))
			require.NoError(t, txn.Commit(ctx))
			require.Equal(t, "v2", getStr(t, keyK))
		} else {
			// Rolling back remains possible and discards the transaction's
			// writes.
			require.NoError(t, txn.Rollback(ctx))
			require.Equal(t, "conflict", getStr(t, keyK))
		}
	})
}
