// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestHeartbeatTxnUpdateStatus verifies that a HeartbeatTxn on a REFRESHING
// record only transitions the status when UpdateStatus is set.
func TestHeartbeatTxnUpdateStatus(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()

	txnID := uuid.MakeV4()
	key := roachpb.Key("foo")
	txnKey := keys.TransactionKey(key, txnID)
	now := clock.Now()

	txn := roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			ID:             txnID,
			Key:            key,
			WriteTimestamp: now,
			MinTimestamp:   now,
		},
		LastHeartbeat: now,
		Status:        roachpb.REFRESHING,
	}

	evalCtx := (&batcheval.MockEvalCtx{
		Clock: clock,
	}).EvalContext()

	writeRecord := func() {
		txnRecord := txn.AsRecord()
		require.NoError(t, storage.MVCCPutProto(
			ctx, engine, txnKey, hlc.Timestamp{}, &txnRecord,
			storage.MVCCWriteOptions{Category: fs.BatchEvalReadCategory},
		))
	}

	t.Run("UpdateStatus=true", func(t *testing.T) {
		writeRecord()

		hbTxn := txn
		hbTxn.Status = roachpb.PENDING
		hbNow := clock.Now()
		resp := kvpb.HeartbeatTxnResponse{}
		_, err := batcheval.HeartbeatTxn(ctx, engine, batcheval.CommandArgs{
			EvalCtx: evalCtx,
			Header:  kvpb.Header{Txn: &hbTxn},
			Args: &kvpb.HeartbeatTxnRequest{
				RequestHeader: kvpb.RequestHeader{Key: key},
				Now:           hbNow,
				UpdateStatus:  true,
			},
		}, &resp)
		require.NoError(t, err)
		require.Equal(t, roachpb.PENDING, resp.Txn.Status)
		require.True(t, resp.Txn.LastHeartbeat.Equal(hbNow))
	})

	t.Run("UpdateStatus=false", func(t *testing.T) {
		writeRecord()

		hbTxn := txn
		hbTxn.Status = roachpb.REFRESHING
		hbNow := clock.Now()
		resp := kvpb.HeartbeatTxnResponse{}
		_, err := batcheval.HeartbeatTxn(ctx, engine, batcheval.CommandArgs{
			EvalCtx: evalCtx,
			Header:  kvpb.Header{Txn: &hbTxn},
			Args: &kvpb.HeartbeatTxnRequest{
				RequestHeader: kvpb.RequestHeader{Key: key},
				Now:           hbNow,
				UpdateStatus:  false,
			},
		}, &resp)
		require.NoError(t, err)
		require.Equal(t, roachpb.REFRESHING, resp.Txn.Status)
		require.True(t, resp.Txn.LastHeartbeat.Equal(hbNow))
	})
}
