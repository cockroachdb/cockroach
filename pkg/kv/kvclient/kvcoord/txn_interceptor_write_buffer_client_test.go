// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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

	bufferedWritesScanTransformEnabled.Override(ctx, &st.SV, false)

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
