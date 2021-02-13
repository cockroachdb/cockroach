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
	RegisterReadOnlyCommand(roachpb.RefreshRange, DefaultDeclareKeys, RefreshRange)
}

// RefreshRange checks whether the key range specified has any values written in
// the interval [args.RefreshFrom, header.Timestamp].
func RefreshRange(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.RefreshRangeRequest)
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

	// Iterate over values until we discover any value written at or after the
	// original timestamp, but before or at the current timestamp. Note that we
	// iterate inconsistently, meaning that intents - including our own - are
	// collected separately and the callback is only invoked on the latest
	// committed version. Note also that we include tombstones, which must be
	// considered as updates on refresh.
	log.VEventf(ctx, 2, "refresh %s @[%s-%s]", args.Span(), refreshFrom, refreshTo)
	intents, err := storage.MVCCIterate(
		ctx, reader, args.Key, args.EndKey, refreshTo,
		storage.MVCCScanOptions{
			Inconsistent: true,
			Tombstones:   true,
		},
		func(kv roachpb.KeyValue) error {
			if ts := kv.Value.Timestamp; refreshFrom.LessEq(ts) {
				return errors.Errorf("encountered recently written key %s @%s", kv.Key, ts)
			}
			return nil
		})
	if err != nil {
		return result.Result{}, err
	}

	// Check if any intents which are not owned by this transaction were written
	// at or beneath the refresh timestamp.
	for _, i := range intents {
		// Ignore our own intents.
		if i.Txn.ID == h.Txn.ID {
			continue
		}
		// Return an error if an intent was written to the span.
		return result.Result{}, errors.Errorf("encountered recently written intent %s @%s",
			i.Key, i.Txn.WriteTimestamp)
	}

	return result.Result{}, nil
}
