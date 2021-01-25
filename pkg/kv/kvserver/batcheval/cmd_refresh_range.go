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
		return result.Result{}, errors.Errorf("no transaction specified to %s", args.Method())
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
	//
	// TODO(ajwerner): The mechanics here related to intents are a bummer. Ideally
	// we'd be able to process intents during an inconsistent iteration as they
	// arise from inside the callback. Not being able to do so means that a
	// conflict due to a descendant transaction may be hidden because of a
	// different, committed, invalidating write.
	log.VEventf(ctx, 2, "refresh %s @[%s-%s]", args.Span(), refreshFrom, refreshTo)
	intents, err := storage.MVCCIterate(
		ctx, reader, args.Key, args.EndKey, refreshTo,
		storage.MVCCScanOptions{
			Inconsistent: true,
			Tombstones:   true,
		},
		func(kv roachpb.KeyValue) error {
			if ts := kv.Value.Timestamp; refreshFrom.LessEq(ts) {
				return &roachpb.RefreshFailedError{
					Key:       kv.Key,
					Timestamp: kv.Value.Timestamp,
				}
			}
			return nil
		})
	if err != nil {
		return result.Result{}, err
	}

	// Check if any intents which are not owned by this transaction were written
	// at or beneath the refresh timestamp. If there are descendant txn IDs, then
	// all of the intents need to be checked.
	err = nil
	for _, i := range intents {
		// Ignore our own intents.
		if i.Txn.ID == h.Txn.ID {
			continue
		}

		// Ensure that the intent was not due to a descendant transaction.
		// Such an intent would imply that a descendant invalidated a read of one of
		// its ancestors. That is not allowed.
		for j := range args.DescendantTxns {
			if i.Txn.ID == args.DescendantTxns[j] {
				return result.Result{}, errors.AssertionFailedf(
					"found illegal intent due to descendant %v at key %v", i.Txn.ID, args.Key)
			}
		}

		// If an error has already been created, don't create another.
		if err != nil {
			continue
		}
		err = &roachpb.RefreshFailedError{
			Key:       i.Key,
			Timestamp: i.Txn.WriteTimestamp,
			Intent:    true,
		}
		if len(args.DescendantTxns) == 0 {
			break
		}
	}

	return result.Result{}, err
}
