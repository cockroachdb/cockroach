// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

	clock := hlc.NewClock(timeutil.NewManualTime(timeutil.Now()), 0 /* maxOffset */)
	now := clock.Now()
	engine := storage.NewDefaultInMemForTesting()
	defer engine.Close()

	testutils.RunTrueAndFalse(t, "CanCreateTxnRecord", func(t *testing.T, canCreateTxnRecord bool) {
		evalCtx := (&batcheval.MockEvalCtx{
			Clock: clock,
			CanCreateTxn: func() (bool, hlc.Timestamp, roachpb.TransactionAbortedReason) {
				return canCreateTxnRecord, hlc.Timestamp{}, 0 // PushTxn doesn't care about the reason
			},
		}).EvalContext()

		key := roachpb.Key("foo")
		pusheeTxnMeta := enginepb.TxnMeta{
			ID:           uuid.MakeV4(),
			Key:          key,
			MinTimestamp: now,
		}

		resp := roachpb.PushTxnResponse{}
		res, err := batcheval.PushTxn(ctx, engine, batcheval.CommandArgs{
			EvalCtx: evalCtx,
			Header: roachpb.Header{
				Timestamp: clock.Now(),
			},
			Args: &roachpb.PushTxnRequest{
				RequestHeader: roachpb.RequestHeader{Key: key},
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
		require.Equal(t, roachpb.PushTxnResponse{
			PusheeTxn:      expectTxn,
			AmbiguousAbort: !canCreateTxnRecord,
		}, resp)
	})
}
