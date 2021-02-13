// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadOnlyCommand(roachpb.QueryIntent, declareKeysQueryIntent, QueryIntent)
}

func declareKeysQueryIntent(
	_ ImmutableRangeState, _ roachpb.Header, req roachpb.Request, latchSpans, _ *spanset.SpanSet,
) {
	// QueryIntent requests read the specified keys at the maximum timestamp in
	// order to read any intent present, if one exists, regardless of the
	// timestamp it was written at.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, req.Header().Span())
}

// QueryIntent checks if an intent exists for the specified transaction at the
// given key. If the intent is missing, the request prevents the intent from
// ever being written at the specified timestamp (but the actual prevention
// happens during the timestamp cache update).
//
// QueryIntent returns an error if the intent is missing and its ErrorIfMissing
// field is set to true. This error is typically an IntentMissingError, but the
// request is special-cased to return a SERIALIZABLE retry error if a transaction
// queries its own intent and finds it has been pushed.
func QueryIntent(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.QueryIntentRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.QueryIntentResponse)

	ownTxn := false
	if h.Txn != nil {
		// Determine if the request is querying an intent in its own
		// transaction. If not, the request is rejected as querying one
		// transaction's intent from within another transaction is unsupported.
		if h.Txn.ID == args.Txn.ID {
			ownTxn = true
		} else {
			return result.Result{}, ErrTransactionUnsupported
		}
	}
	if h.WriteTimestamp().Less(args.Txn.WriteTimestamp) {
		// This condition must hold for the timestamp cache update in
		// Replica.updateTimestampCache to be safe.
		return result.Result{}, errors.AssertionFailedf("QueryIntent request timestamp %s less than txn WriteTimestamp %s",
			h.Timestamp, args.Txn.WriteTimestamp)
	}

	// Read at the specified key at the maximum timestamp. This ensures that we
	// see an intent if one exists, regardless of what timestamp it is written
	// at.
	_, intent, err := storage.MVCCGet(ctx, reader, args.Key, hlc.MaxTimestamp, storage.MVCCGetOptions{
		// Perform an inconsistent read so that intents are returned instead of
		// causing WriteIntentErrors.
		Inconsistent: true,
		// Even if the request header contains a txn, perform the engine lookup
		// without a transaction so that intents for a matching transaction are
		// not returned as values (i.e. we don't want to see our own writes).
		Txn: nil,
	})
	if err != nil {
		return result.Result{}, err
	}

	var curIntentPushed bool
	if intent != nil {
		// See comment on QueryIntentRequest.Txn for an explanation of this
		// comparison.
		// TODO(nvanbenschoten): Now that we have a full intent history,
		// we can look at the exact sequence! That won't serve as much more
		// than an assertion that QueryIntent is being used correctly.
		reply.FoundIntent = (args.Txn.ID == intent.Txn.ID) &&
			(args.Txn.Epoch == intent.Txn.Epoch) &&
			(args.Txn.Sequence <= intent.Txn.Sequence)

		// If we found a matching intent, check whether the intent was pushed
		// past its expected timestamp.
		if reply.FoundIntent {
			// If the request is querying an intent for its own transaction, forward
			// the timestamp we compare against to the provisional commit timestamp
			// in the batch header.
			cmpTS := args.Txn.WriteTimestamp
			if ownTxn {
				cmpTS.Forward(h.Txn.WriteTimestamp)
			}
			if cmpTS.Less(intent.Txn.WriteTimestamp) {
				// The intent matched but was pushed to a later timestamp. Consider a
				// pushed intent a missing intent.
				curIntentPushed = true
				log.VEventf(ctx, 2, "found pushed intent")
				reply.FoundIntent = false

				// If the request was querying an intent in its own transaction, update
				// the response transaction.
				if ownTxn {
					reply.Txn = h.Txn.Clone()
					reply.Txn.WriteTimestamp.Forward(intent.Txn.WriteTimestamp)
				}
			}
		}
	}

	if !reply.FoundIntent && args.ErrorIfMissing {
		if ownTxn && curIntentPushed {
			// If the transaction's own intent was pushed, go ahead and
			// return a TransactionRetryError immediately with an updated
			// transaction proto. This is an optimization that can help
			// the txn use refresh spans more effectively.
			return result.Result{}, roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE, "intent pushed")
		}
		return result.Result{}, roachpb.NewIntentMissingError(args.Key, intent)
	}
	return result.Result{}, nil
}
