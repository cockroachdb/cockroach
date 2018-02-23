// Copyright 2017 The Cockroach Authors.
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
	RegisterCommand(roachpb.Refresh, DefaultDeclareKeys, Refresh)
}

// Refresh checks the key for more recently written values than the
// txn's original timestamp and less recently than the txn's current
// timestamp.
func Refresh(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.RefreshRequest)
	h := cArgs.Header

	if h.Txn == nil {
		return result.Result{}, errors.Errorf("no transaction specified to %s", args.Method())
	}

	// Get the most recent committed value and return any intent by
	// specifying consistent=false. Note that we include tombstones,
	// which must be considered as updates on refresh.
	log.VEventf(ctx, 2, "refresh %s @[%s-%s]", args.Header(), h.Txn.OrigTimestamp, h.Txn.Timestamp)
	val, intents, err := engine.MVCCGetWithTombstone(
		ctx, batch, args.Key, h.Txn.Timestamp, false /* consistent */, nil, /* txn */
	)

	if err != nil {
		return result.Result{}, err
	} else if val != nil {
		if ts := val.Timestamp; !ts.Less(h.Txn.OrigTimestamp) {
			return result.Result{}, errors.Errorf("encountered recently written key %s @%s", args.Key, ts)
		}
	}

	// Now, check intents slice for any which were written earlier than
	// the command's timestamp, not owned by this transaction.
	for _, i := range intents {
		// Ignore our own intents.
		if i.Txn.ID == h.Txn.ID {
			continue
		}
		// Return an error if an intent was written to the span.
		return result.Result{}, errors.Errorf("encountered recently written intent %s @%s",
			i.Span.Key, i.Txn.Timestamp)
	}

	return result.Result{}, nil
}
