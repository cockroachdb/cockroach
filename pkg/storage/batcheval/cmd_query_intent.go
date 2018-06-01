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

	// Perform an inconsistent read so that intents are returned instead of
	// causing WriteIntentErrors.
	consistent := false
	// Even if the request header contains a txn, perform the engine lookup
	// without a transaction so that intents for a matching transaction are
	// not returned as values (i.e. we don't want to see our own writes).
	txn := (*roachpb.Transaction)(nil)
	_, intents, err := engine.MVCCGet(ctx, batch, args.Key, h.Timestamp, consistent, txn)
	if err != nil {
		return result.Result{}, err
	}

	var curIntent *roachpb.Intent
	switch len(intents) {
	case 0:
		reply.FoundIntent = false
	case 1:
		curIntent = &intents[0]
		// See comment on QueryIntentRequest.Txn for an explanation of this
		// comparison.
		reply.FoundIntent = (curIntent.Txn.ID == args.Txn.ID) &&
			(curIntent.Txn.Epoch == args.Txn.Epoch) &&
			(curIntent.Txn.Timestamp == args.Txn.Timestamp) &&
			(curIntent.Txn.Sequence >= args.Txn.Sequence)
	default:
		log.Fatalf(ctx, "more than 1 intent on single key: %v", intents)
	}

	if !reply.FoundIntent {
		switch args.IfMissing {
		case roachpb.QueryIntentRequest_DO_NOTHING:
			// Do nothing.
		case roachpb.QueryIntentRequest_THROW_ERROR:
			return result.Result{}, roachpb.NewIntentMissingError(args.Txn, args.Key, curIntent)
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
