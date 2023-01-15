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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadOnlyCommand(roachpb.Refresh, DefaultDeclareKeys, Refresh)
}

// Refresh checks whether the key has any values written in the interval
// (args.RefreshFrom, header.Timestamp].
func Refresh(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.RefreshRequest)
	h := cArgs.Header

	if h.Txn == nil {
		return result.Result{}, errors.AssertionFailedf("no transaction specified to %s", args.Method())
	}

	// We're going to refresh up to the transaction's read timestamp.
	if h.Timestamp != h.Txn.WriteTimestamp {
		// We're expecting the read and write timestamp to have converged before the
		// Refresh request was sent.
		log.Fatalf(ctx, "expected provisional commit ts %s == read ts %s. txn: %s", h.Timestamp,
			h.Txn.WriteTimestamp, h.Txn)
	}
	refreshTo := h.Timestamp

	refreshFrom := args.RefreshFrom
	if refreshFrom.IsEmpty() {
		return result.Result{}, errors.AssertionFailedf("empty RefreshFrom: %s", args)
	}

	// Get the most recent committed value and return any intent by
	// specifying consistent=false. Note that we include tombstones,
	// which must be considered as updates on refresh.
	log.VEventf(ctx, 2, "refresh %s @[%s-%s]", args.Span(), refreshFrom, refreshTo)
	res, err := storage.MVCCGet(ctx, reader, args.Key, refreshTo, storage.MVCCGetOptions{
		Inconsistent: true,
		Tombstones:   true,
	})

	if err != nil {
		return result.Result{}, err
	} else if res.Value != nil {
		if ts := res.Value.Timestamp; refreshFrom.Less(ts) {
			return result.Result{},
				roachpb.NewRefreshFailedError(roachpb.RefreshFailedError_REASON_COMMITTED_VALUE, args.Key, ts)
		}
	}

	// Check if an intent which is not owned by this transaction was written
	// at or beneath the refresh timestamp.
	if res.Intent != nil && res.Intent.Txn.ID != h.Txn.ID {
		return result.Result{}, roachpb.NewRefreshFailedError(roachpb.RefreshFailedError_REASON_INTENT,
			res.Intent.Key, res.Intent.Txn.WriteTimestamp)
	}

	return result.Result{}, nil
}
