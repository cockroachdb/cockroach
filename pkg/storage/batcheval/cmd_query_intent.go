// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

func init() {
	RegisterCommand(roachpb.QueryIntent, DefaultDeclareKeys, QueryIntent)
}

// QueryIntent checks if an intent exists for the specified
// transaction at the given key. If the intent is missing,
// the request reacts according to its IfMissing field.
func QueryIntent(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.QueryIntentRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.QueryIntentResponse)

	// Snapshot transactions cannot be prevented using a QueryIntent command.
	// This is because we use the timestamp cache to prevent a transaction from
	// committing, but a SNAPSHOT transaction does not need to restart/abort if
	// it runs into the timestamp cache and its timestamp is pushed forwards.
	if args.Txn.Isolation == enginepb.SNAPSHOT &&
		args.IfMissing == roachpb.QueryIntentRequest_PREVENT {
		return result.Result{}, errors.Errorf("cannot prevent SNAPSHOT transaction with QueryIntent")
	}

	// Read at the specified key at the maximum timestamp. This ensures that we
	// see an intent if one exists, regardless of what timestamp it is written
	// at.
	ts := hlc.MaxTimestamp
	// Perform an inconsistent read so that intents are returned instead of
	// causing WriteIntentErrors.
	consistent := false
	// Even if the request header contains a txn, perform the engine lookup
	// without a transaction so that intents for a matching transaction are
	// not returned as values (i.e. we don't want to see our own writes).
	txn := (*roachpb.Transaction)(nil)
	_, intents, err := engine.MVCCGet(ctx, batch, args.Key, ts, consistent, txn)
	if err != nil {
		return result.Result{}, err
	}

	// Determine if the request is querying an intent in its own transaction.
	ownTxn := h.Txn != nil && h.Txn.ID == args.Txn.ID

	var curIntent *roachpb.Intent
	var curIntentPushed bool
	switch len(intents) {
	case 0:
		reply.FoundIntent = false
	case 1:
		curIntent = &intents[0]
		// See comment on QueryIntentRequest.Txn for an explanation of this
		// comparison.
		reply.FoundIntent = (args.Txn.ID == curIntent.Txn.ID) &&
			(args.Txn.Epoch == curIntent.Txn.Epoch) &&
			(args.Txn.Sequence <= curIntent.Txn.Sequence)

		// Check whether the intent was pushed past its expected timestamp.
		if reply.FoundIntent && args.Txn.Timestamp.Less(curIntent.Txn.Timestamp) {
			// The intent matched but was pushed to a later timestamp.
			curIntentPushed = true

			// If the transaction is SERIALIZABLE, consider a pushed intent as a
			// missing intent. If the transaction is SNAPSHOT, don't.
			if args.Txn.Isolation == enginepb.SERIALIZABLE {
				reply.FoundIntent = false
			}

			// If the request was querying an intent in its own transaction, update
			// the response transaction.
			if ownTxn {
				clonedTxn := h.Txn.Clone()
				reply.Txn = &clonedTxn
				reply.Txn.Timestamp.Forward(curIntent.Txn.Timestamp)
			}
		}
	default:
		log.Fatalf(ctx, "more than 1 intent on single key: %v", intents)
	}

	if !reply.FoundIntent {
		switch args.IfMissing {
		case roachpb.QueryIntentRequest_DO_NOTHING:
			// Do nothing.
		case roachpb.QueryIntentRequest_RETURN_ERROR:
			if ownTxn && curIntentPushed {
				// If the transaction's own intent was pushed, go ahead and
				// return a TransactionRetryError immediately with an updated
				// transaction proto. This is an optimization that can help
				// the txn use refresh spans more effectively.
				return result.Result{}, roachpb.NewTransactionRetryError(roachpb.RETRY_SERIALIZABLE)
			}
			return result.Result{}, roachpb.NewIntentMissingError(curIntent)
		case roachpb.QueryIntentRequest_PREVENT:
			// The intent will be prevented by bumping the timestamp cache for
			// the key to the txn timestamp in Replica.updateTimestampCache.
		default:
			return result.Result{},
				errors.Errorf("unexpected QueryIntent IfMissing behavior %v", args.IfMissing)
		}
	}
	return result.Result{}, nil
}
