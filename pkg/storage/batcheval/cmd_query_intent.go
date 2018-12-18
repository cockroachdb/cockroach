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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

	// Read at the specified key at the maximum timestamp. This ensures that we
	// see an intent if one exists, regardless of what timestamp it is written
	// at.
	_, intent, err := engine.MVCCGet(ctx, batch, args.Key, hlc.MaxTimestamp, engine.MVCCGetOptions{
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

	// Determine if the request is querying an intent in its own transaction.
	ownTxn := h.Txn != nil && h.Txn.ID == args.Txn.ID

	var curIntentPushed bool
	if intent != nil {
		// See comment on QueryIntentRequest.Txn for an explanation of this
		// comparison.
		reply.FoundIntent = (args.Txn.ID == intent.Txn.ID) &&
			(args.Txn.Epoch == intent.Txn.Epoch) &&
			(args.Txn.Sequence <= intent.Txn.Sequence)

		// Check whether the intent was pushed past its expected timestamp.
		if reply.FoundIntent && args.Txn.Timestamp.Less(intent.Txn.Timestamp) {
			// The intent matched but was pushed to a later timestamp. Consider a
			// pushed intent a missing intent.
			curIntentPushed = true
			reply.FoundIntent = false

			// If the request was querying an intent in its own transaction, update
			// the response transaction.
			if ownTxn {
				clonedTxn := h.Txn.Clone()
				reply.Txn = &clonedTxn
				reply.Txn.Timestamp.Forward(intent.Txn.Timestamp)
			}
		}
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
			return result.Result{}, roachpb.NewIntentMissingError(intent)
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
