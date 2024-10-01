// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadOnlyCommand(kvpb.QueryIntent, declareKeysQueryIntent, QueryIntent)
}

func declareKeysQueryIntent(
	rs ImmutableRangeState,
	_ *kvpb.Header,
	req kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	// QueryIntent requests acquire a non-MVCC latch in order to read the queried
	// lock, if one exists, regardless of the time it was written at. This
	// isolates them from in-flight intent writes and exclusive lock acquisitions
	// they're trying to query.
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, req.Header().Span())
	// They also acquire a read latch on the per-transaction local key that all
	// replicated shared lock acquisitions acquire latches on. This isolates them
	// from the in-flight shared lock acquisition they're trying to query.
	//
	// TODO(arul): add a test.
	txnID := req.(*kvpb.QueryIntentRequest).Txn.ID
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{
		Key: keys.ReplicatedSharedLocksTransactionLatchingKey(rs.GetRangeID(), txnID),
	})
	return nil
}

// QueryIntent checks if an intent exists for the specified transaction at the
// given key. If the intent is missing, the request prevents the intent from
// ever being written at the specified timestamp (but the actual prevention
// happens during the timestamp cache update).
//
// QueryIntent returns an error if the intent is missing and its ErrorIfMissing
// field is set to true.
func QueryIntent(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.QueryIntentRequest)
	h := cArgs.Header
	reply := resp.(*kvpb.QueryIntentResponse)

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

	if enginepb.TxnSeqIsIgnored(args.Txn.Sequence, args.IgnoredSeqNums) {
		return result.Result{}, errors.AssertionFailedf(
			"QueryIntent request for lock at sequence number %d but sequence number is ignored %v",
			args.Txn.Sequence, args.IgnoredSeqNums,
		)
	}

	var intent *roachpb.Intent

	// Intents have special handling because there's an associated timestamp
	// component with them.
	if args.StrengthOrDefault() == lock.Intent {
		// Read from the lock table to see if an intent exists.
		var err error
		intent, err = storage.GetIntent(ctx, reader, args.Key)
		if err != nil {
			return result.Result{}, err
		}

		reply.FoundIntent = false
		reply.FoundUnpushedIntent = false
		if intent != nil {
			// See comment on QueryIntentRequest.Txn for an explanation of this
			// comparison.
			// TODO(nvanbenschoten): Now that we have a full intent history,
			// we can look at the exact sequence! That won't serve as much more
			// than an assertion that QueryIntent is being used correctly.
			reply.FoundIntent = (args.Txn.ID == intent.Txn.ID) &&
				(args.Txn.Epoch == intent.Txn.Epoch) &&
				(args.Txn.Sequence <= intent.Txn.Sequence)

			if !reply.FoundIntent {
				log.VEventf(ctx, 2, "intent mismatch requires - %v == %v and %v == %v and %v <= %v",
					args.Txn.ID, intent.Txn.ID, args.Txn.Epoch, intent.Txn.Epoch, args.Txn.Sequence, intent.Txn.Sequence)
			} else {
				// If we found a matching intent, check whether the intent was pushed past
				// its expected timestamp.
				cmpTS := args.Txn.WriteTimestamp
				if ownTxn {
					// If the request is querying an intent for its own transaction, forward
					// the timestamp we compare against to the provisional commit timestamp
					// in the batch header.
					cmpTS.Forward(h.Txn.WriteTimestamp)
				}
				reply.FoundUnpushedIntent = intent.Txn.WriteTimestamp.LessEq(cmpTS)

				if !reply.FoundUnpushedIntent {
					log.VEventf(ctx, 2, "found pushed intent")
					// If the request was querying an intent in its own transaction, update
					// the response transaction.
					// TODO(nvanbenschoten): if this is necessary for correctness, say so.
					// And then add a test to demonstrate that.
					if ownTxn {
						reply.Txn = h.Txn.Clone()
						reply.Txn.WriteTimestamp.Forward(intent.Txn.WriteTimestamp)
					}
				}
			}
		} else {
			log.VEventf(ctx, 2, "found no intent")
		}
	} else {
		found, err := storage.MVCCVerifyLock(
			ctx, reader, &args.Txn, args.Strength, args.Key, args.IgnoredSeqNums,
		)
		if err != nil {
			return result.Result{}, err
		}
		if found {
			reply.FoundIntent = true
			reply.FoundUnpushedIntent = true
		}
	}

	if !reply.FoundIntent && args.ErrorIfMissing {
		return result.Result{}, kvpb.NewIntentMissingError(args.Key, intent)
	}
	return result.Result{}, nil
}
