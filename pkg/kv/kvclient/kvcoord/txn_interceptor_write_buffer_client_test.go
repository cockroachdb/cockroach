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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

	BufferedWritesScanTransformEnabled.Override(ctx, &st.SV, false)
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

// TestTxnWriteBufferFlushedWithMaxKeysOnBatch is a regression test for a bug
// related to flushing the write buffer in response to a batch with a MaxKeys or
// TargetBytes set.
//
// The bug requires that:
//
// 0. Buffered writes is enabled, either buffered writes for weak isolation is
// enabled or durable locks for serializable is enabled.
//
// 1. The user makes replicated, locking Get requests. This can occur via SELECT
// FOR UPDATE statements whose predicate contains all primary key columns and
// which isn't transformed to a Scan. These Get's will be transformed to unreplicated
// locking Get's and a replicated locking Get request will be buffered.
//
// 2. The user also writes some rows.
//
// 3. The Get's and writes are split over more than 1 range.
//
// 4. At least some of the buffered Get's are not replaced with later writes.
//
// 5. A batch with TargetBytes or MaxSpanRequestKeys set causes the buffer to flush.
//
// 6. The number of buffered Get's exceeds the MaxSpanRequestKeys.
func TestTxnWriteBufferFlushedWithMaxKeysOnBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	scratchStart, err := s.ScratchRange()
	require.NoError(t, err)

	scratchKey := func(idx int) roachpb.Key {
		key := scratchStart.Clone()
		key = append(key, []byte(fmt.Sprintf("key-%03d", idx))...)
		return key
	}

	// We split the scratch range at a known place so that we can arrange for bug
	// requirement (3).
	_, _, err = s.SplitRange(scratchKey(6))
	require.NoError(t, err)

	st := s.ClusterSettings()

	// The bug requires that we transform Gets. Here, we disable it to prove that
	// this tets passes.
	BufferedWritesGetTransformEnabled.Override(ctx, &st.SV, false)

	// The locks need to actually be taken, so let's write to every key we are
	// going to lock.
	for i := range []int{1, 2, 3, 7, 8, 9} {
		require.NoError(t, db.Put(ctx, scratchKey(i), "before-txn-value"))
	}

	txnCtx := ctx
	// To trace the transaction:
	//
	// tracer := s.TracerI().(*tracing.Tracer)
	// txnCtx, collectAndFinish := tracing.ContextWithRecordingSpan(context.Background(), tracer, "test")
	err = db.Txn(txnCtx, func(ctx context.Context, txn *kv.Txn) error {
		txn.SetBufferedWritesEnabled(true)
		// 1. Replicated locking Gets. We are putting 3 on both sides of the split
		// to ensure we satisfy (3)
		b := txn.NewBatch()
		b.GetForUpdate(scratchKey(1), kvpb.GuaranteedDurability)
		b.GetForUpdate(scratchKey(2), kvpb.GuaranteedDurability)
		b.GetForUpdate(scratchKey(3), kvpb.GuaranteedDurability)
		b.GetForUpdate(scratchKey(7), kvpb.GuaranteedDurability)
		b.GetForUpdate(scratchKey(8), kvpb.GuaranteedDurability)
		b.GetForUpdate(scratchKey(9), kvpb.GuaranteedDurability)
		if err := txn.Run(ctx, b); err != nil {
			return err
		}

		// 2. Our write that will be lost if we hit the bug.
		if err := txn.Put(ctx, scratchKey(10), "from-txn-value"); err != nil {
			return err
		}

		// 3. We force the flush of the buffer with a DeleteRange request that
		// has MaxSpanRequestKeys set
		b = txn.NewBatch()
		b.Header.MaxSpanRequestKeys = 2
		b.DelRange(scratchKey(21), scratchKey(24), true)
		if err := txn.Run(ctx, b); err != nil {
			return err
		}

		return nil
	})
	// To print the trace:
	// recording := collectAndFinish()
	// t.Logf("TRACE: %s", recording)
	require.NoError(t, err)
	actualKV, err := db.Get(ctx, scratchKey(10))
	require.NoError(t, err)
	require.NotNil(t, actualKV.Value)
	actualValue, err := actualKV.Value.GetBytes()
	require.NoError(t, err)
	require.Equal(t, []byte("from-txn-value"), actualValue)
}
