// Copyright 2017 The Cockroach Authors.
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
	log.VEventf(ctx, 2, "refresh %s @[%s-%s]", args.Span(), h.Txn.OrigTimestamp, h.Txn.Timestamp)
	val, intent, err := engine.MVCCGet(ctx, batch, args.Key, h.Txn.Timestamp, engine.MVCCGetOptions{
		Inconsistent: true,
		Tombstones:   true,
	})

	if err != nil {
		return result.Result{}, err
	} else if val != nil {
		// TODO(nvanbenschoten): This is pessimistic. We only need to check
		//   !ts.Less(h.Txn.PrevRefreshTimestamp)
		// This could avoid failed refreshes due to requests performed after
		// earlier refreshes (which read at the refresh ts) that already
		// observed writes between the orig ts and the refresh ts.
		if ts := val.Timestamp; !ts.Less(h.Txn.OrigTimestamp) {
			return result.Result{}, errors.Errorf("encountered recently written key %s @%s", args.Key, ts)
		}
	}

	// Check if an intent which is not owned by this transaction was written
	// at or beneath the refresh timestamp.
	if intent != nil && intent.Txn.ID != h.Txn.ID {
		return result.Result{}, errors.Errorf("encountered recently written intent %s @%s",
			intent.Span.Key, intent.Txn.Timestamp)
	}

	return result.Result{}, nil
}
