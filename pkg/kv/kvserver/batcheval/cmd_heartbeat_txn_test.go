// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestHeartbeatTxnUpdateTSCacheOnly verifies that HeartbeatTxn with
// UpdateTSCacheOnly set returns immediately without reading or writing
// the transaction record.
func TestHeartbeatTxnUpdateTSCacheOnly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ts := hlc.Timestamp{WallTime: 1}
	txn := roachpb.MakeTransaction("test", roachpb.Key("a"), 0, 0, ts, 0, 1, 0, false /* omitInRangefeeds */)

	db := storage.NewDefaultInMemForTesting()
	defer db.Close()
	batch := db.NewBatch()
	defer batch.Close()

	// Do NOT write a transaction record. The UpdateTSCacheOnly path should
	// not attempt to read or create one.

	var resp kvpb.HeartbeatTxnResponse
	headerTxn := txn.Clone()
	_, err := HeartbeatTxn(ctx, batch, CommandArgs{
		Args: &kvpb.HeartbeatTxnRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: txn.Key,
			},
			UpdateTSCacheOnly: true,
		},
		Header: kvpb.Header{
			Timestamp: ts,
			Txn:       headerTxn,
		},
	}, &resp)
	require.NoError(t, err)

	// The response should echo back the header txn.
	require.Equal(t, headerTxn, resp.Txn)

	// No transaction record should have been written.
	txnKey := keys.TransactionKey(txn.Key, txn.ID)
	var record roachpb.TransactionRecord
	ok, err := storage.MVCCGetProto(ctx, batch, txnKey, hlc.Timestamp{}, &record, storage.MVCCGetOptions{})
	require.NoError(t, err)
	require.False(t, ok, "no transaction record should exist")
}
