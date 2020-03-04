// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/abortspan"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestUpdateAbortSpan tests the different ways that request can set, update,
// and delete AbortSpan entries.
func TestUpdateAbortSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	startKey := roachpb.Key("0000")
	txnKey := roachpb.Key("1111")
	intentKey := roachpb.Key("2222")
	endKey := roachpb.Key("9999")
	desc := roachpb.RangeDescriptor{
		RangeID:  99,
		StartKey: roachpb.RKey(startKey),
		EndKey:   roachpb.RKey(endKey),
	}
	as := abortspan.New(desc.RangeID)

	txn := roachpb.MakeTransaction("test", txnKey, 0, hlc.Timestamp{WallTime: 1}, 0)
	newTxnAbortSpanEntry := roachpb.AbortSpanEntry{
		Key:       txn.Key,
		Timestamp: txn.WriteTimestamp,
		Priority:  txn.Priority,
	}
	// Used to detect updates to the AbortSpan entry. WriteTimestamp and
	// Priority don't matter other than that they allow us to detect changes
	// in the AbortSpanEntry.
	prevTxn := txn.Clone()
	prevTxn.WriteTimestamp.Add(-1, 0)
	prevTxn.Priority--
	prevTxnAbortSpanEntry := roachpb.AbortSpanEntry{
		Key:       prevTxn.Key,
		Timestamp: prevTxn.WriteTimestamp,
		Priority:  prevTxn.Priority,
	}

	// Setup helpers.
	type evalFn func(storage.ReadWriter, EvalContext) error
	addIntent := func(b storage.ReadWriter, _ EvalContext) error {
		val := roachpb.MakeValueFromString("val")
		return storage.MVCCPut(ctx, b, nil /* ms */, intentKey, txn.ReadTimestamp, val, &txn)
	}
	addPrevAbortSpanEntry := func(b storage.ReadWriter, rec EvalContext) error {
		return UpdateAbortSpan(ctx, rec, b, nil /* ms */, prevTxn.TxnMeta, true /* poison */)
	}
	compose := func(fns ...evalFn) evalFn {
		return func(b storage.ReadWriter, rec EvalContext) error {
			for _, fn := range fns {
				if err := fn(b, rec); err != nil {
					return err
				}
			}
			return nil
		}
	}

	// Request helpers.
	endTxn := func(b storage.ReadWriter, rec EvalContext, commit bool, poison bool) error {
		req := roachpb.EndTxnRequest{
			RequestHeader: roachpb.RequestHeader{Key: txnKey},
			Commit:        commit,
			Poison:        poison,
			LockSpans:     []roachpb.Span{{Key: intentKey}},
		}
		args := CommandArgs{
			EvalCtx: rec,
			Header: roachpb.Header{
				Timestamp: txn.ReadTimestamp,
				Txn:       &txn,
			},
			Args: &req,
		}

		var resp roachpb.EndTxnResponse
		_, err := EndTxn(ctx, b, args, &resp)
		return err
	}
	resolveIntent := func(
		b storage.ReadWriter, rec EvalContext, status roachpb.TransactionStatus, poison bool,
	) error {
		req := roachpb.ResolveIntentRequest{
			RequestHeader: roachpb.RequestHeader{Key: intentKey},
			IntentTxn:     txn.TxnMeta,
			Status:        status,
			Poison:        poison,
		}
		args := CommandArgs{
			EvalCtx: rec,
			Args:    &req,
		}

		var resp roachpb.ResolveIntentResponse
		_, err := ResolveIntent(ctx, b, args, &resp)
		return err
	}
	resolveIntentRange := func(
		b storage.ReadWriter, rec EvalContext, status roachpb.TransactionStatus, poison bool,
	) error {
		req := roachpb.ResolveIntentRangeRequest{
			RequestHeader: roachpb.RequestHeader{Key: startKey, EndKey: endKey},
			IntentTxn:     txn.TxnMeta,
			Status:        status,
			Poison:        poison,
		}
		args := CommandArgs{
			EvalCtx: rec,
			Args:    &req,
		}

		var resp roachpb.ResolveIntentRangeResponse
		_, err := ResolveIntentRange(ctx, b, args, &resp)
		return err
	}

	testCases := []struct {
		name   string
		before evalFn
		run    evalFn                  // nil if invalid test case
		exp    *roachpb.AbortSpanEntry // nil if no entry expected
		expErr string                  // empty if no error expected
	}{
		///////////////////////////////////////////////////////////////////////
		//                       EndTxnRequest                               //
		///////////////////////////////////////////////////////////////////////
		{
			name: "end txn, rollback, no poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return endTxn(b, rec, false /* commit */, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "end txn, rollback, no poison, intent missing, abort span present",
			before: addPrevAbortSpanEntry,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return endTxn(b, rec, false /* commit */, false /* poison */)
			},
			// Not poisoning, should clean up abort span entry.
			exp: nil,
		},
		{
			name: "end txn, rollback, poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return endTxn(b, rec, false /* commit */, true /* poison */)
			},
			// Poisoning, but no intents found, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "end txn, rollback, poison, intent missing, abort span present",
			before: addPrevAbortSpanEntry,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return endTxn(b, rec, false /* commit */, true /* poison */)
			},
			// Poisoning, but no intents found, don't touch abort span.
			exp: &prevTxnAbortSpanEntry,
		},
		{
			name: "end txn, commit, no poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return endTxn(b, rec, true /* commit */, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			// NOTE: this request doesn't make sense. An abort span shouldn't be
			// present if the transaction is still committable.
			name: "end txn, commit, no poison, intent missing, abort span present",
		},
		{
			// It is an error for EndTxn to pass Commit = true and Poison = true.
			name: "end txn, commit, poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return endTxn(b, rec, true /* commit */, true /* poison */)
			},
			expErr: "cannot poison during a committing EndTxn request",
		},
		{
			// It is an error for EndTxn to pass Commit = true and Poison = true.
			name:   "end txn, commit, poison, intent missing, abort span present",
			before: addPrevAbortSpanEntry,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return endTxn(b, rec, true /* commit */, true /* poison */)
			},
			expErr: "cannot poison during a committing EndTxn request",
		},
		{
			name:   "end txn, rollback, no poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return endTxn(b, rec, false /* commit */, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "end txn, rollback, no poison, intent present, abort span present",
			before: compose(addIntent, addPrevAbortSpanEntry),
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return endTxn(b, rec, false /* commit */, false /* poison */)
			},
			// Not poisoning, should clean up abort span entry.
			exp: nil,
		},
		{
			name:   "end txn, rollback, poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return endTxn(b, rec, false /* commit */, true /* poison */)
			},
			// Poisoning, should add an abort span entry.
			exp: &newTxnAbortSpanEntry,
		},
		{
			name:   "end txn, rollback, poison, intent present, abort span present",
			before: compose(addIntent, addPrevAbortSpanEntry),
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return endTxn(b, rec, false /* commit */, true /* poison */)
			},
			// Poisoning, should update abort span entry.
			exp: &newTxnAbortSpanEntry,
		},
		{
			name:   "end txn, commit, no poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return endTxn(b, rec, true /* commit */, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			// NOTE: this request doesn't make sense. An abort span shouldn't be
			// present if the transaction is still committable.
			name: "end txn, commit, no poison, intent present, abort span present",
		},
		{
			// It is an error for EndTxn to pass Commit = true and Poison = true.
			name:   "end txn, commit, poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return endTxn(b, rec, true /* commit */, true /* poison */)
			},
			expErr: "cannot poison during a committing EndTxn request",
		},
		{
			// It is an error for EndTxn to pass Commit = true and Poison = true.
			name:   "end txn, commit, poison, intent present, abort span present",
			before: compose(addIntent, addPrevAbortSpanEntry),
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return endTxn(b, rec, true /* commit */, true /* poison */)
			},
			expErr: "cannot poison during a committing EndTxn request",
		},
		///////////////////////////////////////////////////////////////////////
		//                       ResolveIntentRequest                        //
		///////////////////////////////////////////////////////////////////////
		{
			name: "resolve intent, txn pending, no poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.PENDING, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent, txn pending, no poison, intent missing, abort span present",
			before: addPrevAbortSpanEntry,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.PENDING, false /* poison */)
			},
			// Not aborted, don't touch abort span.
			exp: &prevTxnAbortSpanEntry,
		},
		{
			name:   "resolve intent, txn pending, no poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.PENDING, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent, txn pending, no poison, intent present, abort span present",
			before: compose(addIntent, addPrevAbortSpanEntry),
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.PENDING, false /* poison */)
			},
			// Not aborted, don't touch abort span.
			exp: &prevTxnAbortSpanEntry,
		},
		{
			name: "resolve intent, txn pending, poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.PENDING, true /* poison */)
			},
			// Poisoning but not aborted, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent, txn pending, poison, intent missing, abort span present",
			before: addPrevAbortSpanEntry,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.PENDING, true /* poison */)
			},
			// Not aborted, don't touch abort span.
			exp: &prevTxnAbortSpanEntry,
		},
		{
			name:   "resolve intent, txn pending, poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.PENDING, true /* poison */)
			},
			// Poisoning but not aborted, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent, txn pending, poison, intent present, abort span present",
			before: compose(addIntent, addPrevAbortSpanEntry),
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.PENDING, true /* poison */)
			},
			// Not aborted, don't touch abort span.
			exp: &prevTxnAbortSpanEntry,
		},
		{
			name: "resolve intent, txn aborted, no poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.ABORTED, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent, txn aborted, no poison, intent missing, abort span present",
			before: addPrevAbortSpanEntry,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.ABORTED, false /* poison */)
			},
			// Not poisoning, should clean up abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent, txn aborted, no poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.ABORTED, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent, txn aborted, no poison, intent present, abort span present",
			before: compose(addIntent, addPrevAbortSpanEntry),
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.ABORTED, false /* poison */)
			},
			// Not poisoning, should clean up abort span entry.
			exp: nil,
		},
		{
			name: "resolve intent, txn aborted, poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.ABORTED, true /* poison */)
			},
			// Poisoning, but no intents found, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent, txn aborted, poison, intent missing, abort span present",
			before: addPrevAbortSpanEntry,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.ABORTED, true /* poison */)
			},
			// Poisoning, but no intents found, don't touch abort span.
			exp: &prevTxnAbortSpanEntry,
		},
		{
			name:   "resolve intent, txn aborted, poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.ABORTED, true /* poison */)
			},
			// Poisoning, should add an abort span entry.
			exp: &newTxnAbortSpanEntry,
		},
		{
			name:   "resolve intent, txn aborted, poison, intent present, abort span present",
			before: compose(addIntent, addPrevAbortSpanEntry),
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.ABORTED, true /* poison */)
			},
			// Poisoning, should update abort span entry.
			exp: &newTxnAbortSpanEntry,
		},
		{
			name: "resolve intent, txn committed, no poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.COMMITTED, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			// NOTE: this case doesn't make sense. It shouldn't be possible for a committed
			// txn to have an abort span before its intents are cleaned up.
			name: "resolve intent, txn committed, no poison, intent missing, abort span present",
		},
		{
			name:   "resolve intent, txn committed, no poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.COMMITTED, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			// NOTE: this case doesn't make sense. It shouldn't be possible for a committed
			// txn to have an abort span before its intents are cleaned up.
			name: "resolve intent, txn committed, no poison, intent present, abort span present",
		},
		{
			name: "resolve intent, txn committed, poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.COMMITTED, true /* poison */)
			},
			// Poisoning but not aborted, should not add an abort span entry.
			exp: nil,
		},
		{
			// NOTE: this case doesn't make sense. It shouldn't be possible for a committed
			// txn to have an abort span before its intents are cleaned up.
			name: "resolve intent, txn committed, poison, intent missing, abort span present",
		},
		{
			name:   "resolve intent, txn committed, poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntent(b, rec, roachpb.COMMITTED, true /* poison */)
			},
			// Poisoning but not aborted, should not add an abort span entry.
			exp: nil,
		},
		{
			// NOTE: this case doesn't make sense. It shouldn't be possible for a committed
			// txn to have an abort span before its intents are cleaned up.
			name: "resolve intent, txn committed, poison, intent present, abort span present",
		},
		///////////////////////////////////////////////////////////////////////
		//                     ResolveIntentRangeRequest                     //
		///////////////////////////////////////////////////////////////////////
		{
			name: "resolve intent range, txn pending, no poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.PENDING, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent range, txn pending, no poison, intent missing, abort span present",
			before: addPrevAbortSpanEntry,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.PENDING, false /* poison */)
			},
			// Not aborted, don't touch abort span.
			exp: &prevTxnAbortSpanEntry,
		},
		{
			name:   "resolve intent range, txn pending, no poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.PENDING, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent range, txn pending, no poison, intent present, abort span present",
			before: compose(addIntent, addPrevAbortSpanEntry),
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.PENDING, false /* poison */)
			},
			// Not aborted, don't touch abort span.
			exp: &prevTxnAbortSpanEntry,
		},
		{
			name: "resolve intent range, txn pending, poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.PENDING, true /* poison */)
			},
			// Poisoning but not aborted, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent range, txn pending, poison, intent missing, abort span present",
			before: addPrevAbortSpanEntry,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.PENDING, true /* poison */)
			},
			// Not aborted, don't touch abort span.
			exp: &prevTxnAbortSpanEntry,
		},
		{
			name:   "resolve intent range, txn pending, poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.PENDING, true /* poison */)
			},
			// Poisoning but not aborted, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent range, txn pending, poison, intent present, abort span present",
			before: compose(addIntent, addPrevAbortSpanEntry),
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.PENDING, true /* poison */)
			},
			// Not aborted, don't touch abort span.
			exp: &prevTxnAbortSpanEntry,
		},
		{
			name: "resolve intent range, txn aborted, no poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.ABORTED, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent range, txn aborted, no poison, intent missing, abort span present",
			before: addPrevAbortSpanEntry,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.ABORTED, false /* poison */)
			},
			// Not poisoning, should clean up abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent range, txn aborted, no poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.ABORTED, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent range, txn aborted, no poison, intent present, abort span present",
			before: compose(addIntent, addPrevAbortSpanEntry),
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.ABORTED, false /* poison */)
			},
			// Not poisoning, should clean up abort span entry.
			exp: nil,
		},
		{
			name: "resolve intent range, txn aborted, poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.ABORTED, true /* poison */)
			},
			// Poisoning, but no intents found, should not add an abort span entry.
			exp: nil,
		},
		{
			name:   "resolve intent range, txn aborted, poison, intent missing, abort span present",
			before: addPrevAbortSpanEntry,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.ABORTED, true /* poison */)
			},
			// Poisoning, but no intents found, don't touch abort span.
			exp: &prevTxnAbortSpanEntry,
		},
		{
			name:   "resolve intent range, txn aborted, poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.ABORTED, true /* poison */)
			},
			// Poisoning, should add an abort span entry.
			exp: &newTxnAbortSpanEntry,
		},
		{
			name:   "resolve intent range, txn aborted, poison, intent present, abort span present",
			before: compose(addIntent, addPrevAbortSpanEntry),
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.ABORTED, true /* poison */)
			},
			// Poisoning, should update abort span entry.
			exp: &newTxnAbortSpanEntry,
		},
		{
			name: "resolve intent range, txn committed, no poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.COMMITTED, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			// NOTE: this case doesn't make sense. It shouldn't be possible for a committed
			// txn to have an abort span before its intents are cleaned up.
			name: "resolve intent range, txn committed, no poison, intent missing, abort span present",
		},
		{
			name:   "resolve intent range, txn committed, no poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.COMMITTED, false /* poison */)
			},
			// Not poisoning, should not add an abort span entry.
			exp: nil,
		},
		{
			// NOTE: this case doesn't make sense. It shouldn't be possible for a committed
			// txn to have an abort span before its intents are cleaned up.
			name: "resolve intent range, txn committed, no poison, intent present, abort span present",
		},
		{
			name: "resolve intent range, txn committed, poison, intent missing, abort span missing",
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.COMMITTED, true /* poison */)
			},
			// Poisoning but not aborted, should not add an abort span entry.
			exp: nil,
		},
		{
			// NOTE: this case doesn't make sense. It shouldn't be possible for a committed
			// txn to have an abort span before its intents are cleaned up.
			name: "resolve intent range, txn committed, poison, intent missing, abort span present",
		},
		{
			name:   "resolve intent range, txn committed, poison, intent present, abort span missing",
			before: addIntent,
			run: func(b storage.ReadWriter, rec EvalContext) error {
				return resolveIntentRange(b, rec, roachpb.COMMITTED, true /* poison */)
			},
			// Poisoning but not aborted, should not add an abort span entry.
			exp: nil,
		},
		{
			// NOTE: this case doesn't make sense. It shouldn't be possible for a committed
			// txn to have an abort span before its intents are cleaned up.
			name: "resolve intent range, txn committed, poison, intent present, abort span present",
		},
	}
	for _, c := range testCases {
		t.Run(c.name, func(t *testing.T) {
			if c.run == nil {
				t.Skip("invalid test case")
			}

			db := storage.NewDefaultInMem()
			defer db.Close()
			batch := db.NewBatch()
			defer batch.Close()
			evalCtx := &MockEvalCtx{
				Desc:      &desc,
				AbortSpan: as,
				CanCreateTxn: func() (bool, hlc.Timestamp, roachpb.TransactionAbortedReason) {
					return true, hlc.Timestamp{}, 0
				},
			}

			if c.before != nil {
				require.NoError(t, c.before(batch, evalCtx.EvalContext()))
			}

			err := c.run(batch, evalCtx.EvalContext())
			if c.expErr != "" {
				require.Regexp(t, c.expErr, err)
			} else {
				require.NoError(t, err)

				var curEntry roachpb.AbortSpanEntry
				exists, err := as.Get(ctx, batch, txn.ID, &curEntry)
				require.NoError(t, err)
				require.Equal(t, c.exp != nil, exists)
				if exists {
					require.Equal(t, *c.exp, curEntry)
				}
			}
		})
	}
}
