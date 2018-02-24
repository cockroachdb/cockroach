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
	RegisterCommand(roachpb.RefreshRange, DefaultDeclareKeys, RefreshRange)
}

// RefreshRange scans the key range specified by start key through end
// key, and returns an error on any keys written more recently than
// the txn's original timestamp and less recently than the txn's
// current timestamp.
func RefreshRange(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.RefreshRangeRequest)
	h := cArgs.Header

	if h.Txn == nil {
		return result.Result{}, errors.Errorf("no transaction specified to %s", args.Method())
	}

	// Use a time-bounded iterator to avoid unnecessarily iterating over
	// older data.
	iter := batch.NewTimeBoundIterator(h.Txn.OrigTimestamp, h.Txn.Timestamp)
	defer iter.Close()
	// Iterate over values until we discover any value written at or
	// after the original timestamp, but before or at the current
	// timestamp. Note that we do not iterate using the txn and the
	// iteration is done with consistent=false. This reads only
	// committed values and returns all intents, including those from
	// the txn itself. Note that we include tombstones, which must be
	// considered as updates on refresh.
	log.VEventf(ctx, 2, "refresh %s @[%s-%s]", args.Header(), h.Txn.OrigTimestamp, h.Txn.Timestamp)
	intents, err := engine.MVCCIterateUsingIter(
		ctx,
		batch,
		args.Key,
		args.EndKey,
		h.Txn.Timestamp,
		false, /* consistent */
		true,  /* tombstones */
		nil,   /* txn */
		false, /* reverse */
		iter,
		func(kv roachpb.KeyValue) (bool, error) {
			if ts := kv.Value.Timestamp; !ts.Less(h.Txn.OrigTimestamp) {
				return true, errors.Errorf("encountered recently written key %s @%s", kv.Key, ts)
			}
			return false, nil
		},
	)
	if err != nil {
		return result.Result{}, err
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
