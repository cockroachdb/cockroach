// Copyright 2024 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestPushTxnAmbiguousAbort tests PushTxn behavior when the transaction record
// is missing. In this case, the timestamp cache can tell us whether the
// transaction record may have existed in the past -- if we know it hasn't, then
// the transaction is still pending (e.g. before the record is written), but
// otherwise the transaction record is pessimistically assumed to have aborted.
// However, this state is ambiguous, as the transaction may in fact have
// committed already and GCed its transaction record. Make sure this is
// reflected in the AmbiguousAbort field.
//
// TODO(erikgrinaker): generalize this to test PushTxn more broadly.
func TestPushTxnAmbiguousAbort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))
	now := clock.Now()
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()

	testutils.RunTrueAndFalse(t, "CanCreateTxnRecord", func(t *testing.T, canCreateTxnRecord bool) {
		evalCtx := (&batcheval.MockEvalCtx{
			Clock: clock,
			CanCreateTxnRecordFn: func() (bool, kvpb.TransactionAbortedReason) {
				return canCreateTxnRecord, 0 // PushTxn doesn't care about the reason
			},
		}).EvalContext()

		key := roachpb.Key("foo")
		pusheeTxnMeta := enginepb.TxnMeta{
			ID:           uuid.MakeV4(),
			Key:          key,
			MinTimestamp: now,
		}

		resp := kvpb.PushTxnResponse{}
		res, err := batcheval.PushTxn(ctx, engine, batcheval.CommandArgs{
			EvalCtx: evalCtx,
			Header: kvpb.Header{
				Timestamp: clock.Now(),
			},
			Args: &kvpb.PushTxnRequest{
				RequestHeader: kvpb.RequestHeader{Key: key},
				PusheeTxn:     pusheeTxnMeta,
			},
		}, &resp)
		require.NoError(t, err)

		// There is no txn record (the engine is empty). If we can't create a txn
		// record, it's because the timestamp cache can't confirm that it didn't
		// exist in the past. This will return an ambiguous abort.
		var expectUpdatedTxns []*roachpb.Transaction
		expectTxn := roachpb.Transaction{
			TxnMeta:       pusheeTxnMeta,
			LastHeartbeat: pusheeTxnMeta.MinTimestamp,
		}
		if !canCreateTxnRecord {
			expectTxn.Status = roachpb.ABORTED
			expectUpdatedTxns = append(expectUpdatedTxns, &expectTxn)
		}

		require.Equal(t, result.Result{
			Local: result.LocalResult{
				UpdatedTxns: expectUpdatedTxns,
			},
		}, res)
		require.Equal(t, kvpb.PushTxnResponse{
			PusheeTxn:      expectTxn,
			AmbiguousAbort: !canCreateTxnRecord,
		}, resp)
	})
}

// TestPushTxnHigherEpochDoesNotLeakIgnoredSeqNums reproduces a lost write that
// happens when a transaction restarts at a higher epoch while its transaction
// record on disk still reflects the previous epoch.
//
// The scenario, as observed with a READ COMMITTED transaction:
//  1. At epoch 0, the transaction's statement is retried several times. Each
//     retry rolls back to a savepoint, so the transaction accumulates ignored
//     sequence numbers (say [1, 10]). A heartbeat persists these in the
//     transaction record.
//  2. After too many statement retries, the transaction restarts at epoch 1
//     (same transaction ID). Sequence numbers start over and the coordinator
//     clears its ignored sequence numbers. The epoch-1 attempt writes an
//     intent at sequence number 2. The transaction record is not rewritten, so
//     it still holds the epoch-0 state.
//  3. A conflicting request runs into the epoch-1 intent and pushes the
//     transaction's timestamp. PushTxn starts from the persisted (epoch-0)
//     record and, learning about the higher epoch from the intent, bumps only
//     the Epoch field. The returned pushee proto therefore claims epoch 1 but
//     carries epoch 0's ignored sequence numbers.
//  4. The pusher resolves the intent with that proto (PENDING status). Because
//     the epochs now match, intent resolution applies the ignored sequence
//     numbers, sees that the intent's sequence number (2) is "ignored", and
//     removes the intent.
//  5. The transaction then commits at epoch 1 without that write, even though
//     the write was evaluated successfully.
//
// The pushee proto returned by PushTxn must not combine a newer epoch with
// state scoped to an older epoch, and the epoch-1 intent must survive the
// pusher's resolution (only its timestamp should be moved).
func TestPushTxnHigherEpochDoesNotLeakIgnoredSeqNums(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	clock := hlc.NewClockForTesting(timeutil.NewManualTime(timeutil.Now()))
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()

	evalCtx := (&batcheval.MockEvalCtx{
		ClusterSettings: cluster.MakeTestingClusterSettings(),
		Clock:           clock,
		CanCreateTxnRecordFn: func() (bool, kvpb.TransactionAbortedReason) {
			return true, 0
		},
	}).EvalContext()

	anchorKey := roachpb.Key("a")
	intentKey := roachpb.Key("b")

	// Epoch 0: the transaction record, as persisted by a heartbeat after
	// several statement-level retries rolled back sequence numbers [1, 10].
	txn := roachpb.MakeTransaction(
		"test", anchorKey, isolation.ReadCommitted, roachpb.NormalUserPriority,
		clock.Now(), 0 /* maxOffsetNs */, 1 /* coordinatorNodeID */, 0, /* admissionPriority */
		false, /* omitInRangefeeds */
	)
	txn.Sequence = 10
	txn.IgnoredSeqNums = []enginepb.IgnoredSeqNumRange{{Start: 1, End: 10}}
	txn.LastHeartbeat = clock.Now()
	txnRecord := txn.AsRecord()
	require.NoError(t, storage.MVCCPutProto(
		ctx, engine, keys.TransactionKey(txn.Key, txn.ID), hlc.Timestamp{}, &txnRecord,
		storage.MVCCWriteOptions{},
	))

	// Epoch 1: the transaction restarts (sequence numbers start over and
	// ignored sequence numbers are cleared) and writes an intent at sequence
	// number 2. The transaction record is not updated.
	epoch1Txn := txn.Clone()
	epoch1Txn.Restart(roachpb.NormalUserPriority, 0 /* upgradePriority */, clock.Now())
	epoch1Txn.Sequence = 2
	require.Equal(t, enginepb.TxnEpoch(1), epoch1Txn.Epoch)
	require.Empty(t, epoch1Txn.IgnoredSeqNums)
	_, err := storage.MVCCPut(
		ctx, engine, intentKey, epoch1Txn.WriteTimestamp, roachpb.MakeValueFromString("v"),
		storage.MVCCWriteOptions{Txn: epoch1Txn},
	)
	require.NoError(t, err)

	// A conflicting request runs into the epoch-1 intent and pushes the
	// transaction's timestamp using the transaction metadata from the intent.
	intent, err := storage.GetIntent(ctx, engine, intentKey)
	require.NoError(t, err)
	require.NotNil(t, intent)
	pushTo := clock.Now()
	var pushResp kvpb.PushTxnResponse
	_, err = batcheval.PushTxn(ctx, engine, batcheval.CommandArgs{
		EvalCtx: evalCtx,
		Header:  kvpb.Header{Timestamp: pushTo},
		Args: &kvpb.PushTxnRequest{
			RequestHeader: kvpb.RequestHeader{Key: anchorKey},
			PusheeTxn:     intent.Txn,
			PushTo:        pushTo,
			PushType:      kvpb.PUSH_TIMESTAMP,
		},
	}, &pushResp)
	require.NoError(t, err)
	pushee := pushResp.PusheeTxn
	require.Equal(t, roachpb.PENDING, pushee.Status)
	require.Equal(t, enginepb.TxnEpoch(1), pushee.Epoch)
	// Ignored sequence numbers are scoped to an epoch; the epoch-0 ranges must
	// not be reported alongside epoch 1.
	require.Empty(t, pushee.IgnoredSeqNums,
		"PushTxn returned epoch %d with ignored seqnums from an earlier epoch", pushee.Epoch)

	// The pusher resolves the intent it ran into with the pushee proto from
	// the push. This must only move the intent's timestamp forward.
	_, _, _, _, err = storage.MVCCResolveWriteIntent(
		ctx, engine, nil /* ms */, roachpb.MakeLockUpdate(&pushee, roachpb.Span{Key: intentKey}),
		storage.MVCCResolveWriteIntentOptions{},
	)
	require.NoError(t, err)
	intent, err = storage.GetIntent(ctx, engine, intentKey)
	require.NoError(t, err)
	require.NotNil(t, intent, "epoch-1 intent was removed by a timestamp push")
	require.Equal(t, enginepb.TxnEpoch(1), intent.Txn.Epoch)
	require.Equal(t, enginepb.TxnSeq(2), intent.Txn.Sequence)
}
