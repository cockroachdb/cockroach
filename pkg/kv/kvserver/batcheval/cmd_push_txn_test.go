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

// TestPushTxnHigherEpochDoesNotLeakIgnoredSeqNums is a regression test for
// #175819.
//
// When a pusher learns from an intent that the pushee is running at a higher
// epoch than its persisted transaction record, PushTxn bumps the epoch on the
// transaction proto it returns. That proto is then used by the pusher to
// resolve the intent it was blocked on. If PushTxn were to bump the epoch while
// retaining the record's epoch-scoped state, the returned proto would pair a
// new epoch with the *old* epoch's IgnoredSeqNums. Intent resolution gates the
// application of ignored seqnum ranges on the epochs matching, so the bumped
// epoch defeats that guard and the stale ranges get applied to a new-epoch
// intent. Because sequence numbers restart on an epoch bump, the new intent's
// sequence number frequently falls inside an old ignored range, and the intent
// is removed even though the pushee never rolled it back. The pushee then
// commits without that write: silent data loss.
func TestPushTxnHigherEpochDoesNotLeakIgnoredSeqNums(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	manual := timeutil.NewManualTime(timeutil.Unix(0, 123))
	clock := hlc.NewClockForTesting(manual)
	st := cluster.MakeTestingClusterSettings()
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()

	evalCtx := (&batcheval.MockEvalCtx{
		ClusterSettings: st,
		Clock:           clock,
	}).EvalContext()

	key := roachpb.Key("a")
	now := clock.Now()

	// The pushee's persisted transaction record is at epoch 0 and, crucially,
	// carries ignored sequence numbers from a savepoint rollback performed in
	// that epoch. This is the precondition described in the issue: an explicit
	// READ COMMITTED transaction rolls back to a savepoint on each statement
	// retry (adding ignored seqnums), the first heartbeat writes the record with
	// them, and the record is not updated again until EndTxn.
	pusheeEpoch0 := roachpb.MakeTransaction(
		"pushee", key, isolation.ReadCommitted, roachpb.MinUserPriority, now,
		0 /* maxOffsetNs */, 0 /* coordinatorNodeID */, 0, /* admissionPriority */
		false, /* omitInRangefeeds */
	)
	pusheeEpoch0.Sequence = 10
	pusheeEpoch0.IgnoredSeqNums = []enginepb.IgnoredSeqNumRange{{Start: 1, End: 10}}
	pusheeEpoch0.LastHeartbeat = now
	record := pusheeEpoch0.AsRecord()
	txnKey := keys.TransactionKey(pusheeEpoch0.Key, pusheeEpoch0.ID)
	require.NoError(t, storage.MVCCPutProto(ctx, engine, txnKey, hlc.Timestamp{}, &record,
		storage.MVCCWriteOptions{}))

	// The pushee then hits a full transaction restart: same txn ID, epoch 1,
	// sequence numbers restart from 0, and the record is left untouched. It
	// writes an intent at sequence number 2 of the new epoch. Note that 2 falls
	// inside the epoch-0 ignored range [1, 10].
	pusheeEpoch1 := pusheeEpoch0
	pusheeEpoch1.Restart(roachpb.MinUserPriority, pusheeEpoch0.Priority, now)
	require.Equal(t, enginepb.TxnEpoch(1), pusheeEpoch1.Epoch)
	require.Empty(t, pusheeEpoch1.IgnoredSeqNums)
	pusheeEpoch1.Sequence = 2

	var v roachpb.Value
	v.SetString("new-epoch-write")
	_, err := storage.MVCCPut(ctx, engine, key, pusheeEpoch1.WriteTimestamp, v,
		storage.MVCCWriteOptions{Txn: &pusheeEpoch1})
	require.NoError(t, err)

	// A conflicting request encounters the epoch-1 intent and issues a
	// PUSH_TIMESTAMP against the pushee, passing along the intent's metadata.
	pusher := roachpb.MakeTransaction(
		"pusher", key, isolation.Serializable, roachpb.MaxUserPriority, now,
		0 /* maxOffsetNs */, 0 /* coordinatorNodeID */, 0, /* admissionPriority */
		false, /* omitInRangefeeds */
	)
	pushTo := pusheeEpoch1.WriteTimestamp.Add(1, 0)

	var resp kvpb.PushTxnResponse
	_, err = batcheval.PushTxn(ctx, engine, batcheval.CommandArgs{
		EvalCtx: evalCtx,
		// The request timestamp must be at least the timestamp we are pushing to.
		Header: kvpb.Header{Timestamp: pushTo},
		Args: &kvpb.PushTxnRequest{
			RequestHeader: kvpb.RequestHeader{Key: pusheeEpoch1.Key},
			PusherTxn:     pusher,
			PusheeTxn:     pusheeEpoch1.TxnMeta,
			PushTo:        pushTo,
			PushType:      kvpb.PUSH_TIMESTAMP,
		},
	}, &resp)
	require.NoError(t, err)

	// The push succeeded and learned about the higher epoch from the intent.
	require.Equal(t, roachpb.PENDING, resp.PusheeTxn.Status)
	require.Equal(t, enginepb.TxnEpoch(1), resp.PusheeTxn.Epoch)
	// The epoch-0 ignored seqnums must not ride along with the epoch-1 proto.
	require.Empty(t, resp.PusheeTxn.IgnoredSeqNums,
		"PushTxn returned epoch-%d state alongside epoch-%d ignored seqnums",
		pusheeEpoch0.Epoch, resp.PusheeTxn.Epoch)

	// Resolve the intent with the pushed (still PENDING) transaction, as the
	// pusher would. This must only move the intent's timestamp; it must never
	// remove an intent belonging to the pushee's current epoch.
	var resolveResp kvpb.ResolveIntentResponse
	_, err = batcheval.ResolveIntent(ctx, engine, batcheval.CommandArgs{
		EvalCtx: evalCtx,
		Header:  kvpb.Header{Timestamp: pushTo},
		Args: &kvpb.ResolveIntentRequest{
			RequestHeader:  kvpb.RequestHeader{Key: key},
			IntentTxn:      resp.PusheeTxn.TxnMeta,
			Status:         resp.PusheeTxn.Status,
			IgnoredSeqNums: resp.PusheeTxn.IgnoredSeqNums,
		},
	}, &resolveResp)
	require.NoError(t, err)

	// The pushee's epoch-1 intent must still be there. If it isn't, the pushee
	// will go on to commit without this write.
	res, err := storage.MVCCGet(ctx, engine, key, pushTo, storage.MVCCGetOptions{Inconsistent: true})
	require.NoError(t, err)
	require.NotNil(t, res.Intent, "the pushee's epoch-1 intent was removed by a timestamp push")
	require.Equal(t, enginepb.TxnEpoch(1), res.Intent.Txn.Epoch)
	require.Equal(t, pushTo, res.Intent.Txn.WriteTimestamp, "the intent should have been pushed")

	// And the pushee still sees its own write.
	ownRes, err := storage.MVCCGet(ctx, engine, key, pusheeEpoch1.ReadTimestamp,
		storage.MVCCGetOptions{Txn: &pusheeEpoch1})
	require.NoError(t, err)
	require.True(t, ownRes.Value.IsPresent())
	gotBytes, err := ownRes.Value.Value.GetBytes()
	require.NoError(t, err)
	require.Equal(t, "new-epoch-write", string(gotBytes))
}
