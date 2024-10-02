// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func init() {
	// Depending on the cluster version, Refresh requests  may or may not declare locks.
	// See DeclareKeysForRefresh for details.
	RegisterReadOnlyCommand(kvpb.Refresh, DeclareKeysForRefresh, Refresh)
}

// Refresh checks whether the key has any values written in the interval
// (args.RefreshFrom, header.Timestamp].
func Refresh(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.RefreshRequest)
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
		ReadCategory: fs.BatchEvalReadCategory,
	})

	if err != nil {
		return result.Result{}, err
	} else if res.Value != nil {
		if ts := res.Value.Timestamp; refreshFrom.Less(ts) {
			return result.Result{},
				kvpb.NewRefreshFailedError(ctx, kvpb.RefreshFailedError_REASON_COMMITTED_VALUE, args.Key, ts)
		}
	}

	// Check if an intent which is not owned by this transaction was written
	// at or beneath the refresh timestamp.
	if res.Intent != nil && res.Intent.Txn.ID != h.Txn.ID {
		// TODO(mira): Remove after V23_2_RemoveLockTableWaiterTouchPush is deleted.
		if h.WaitPolicy == lock.WaitPolicy_Error {
			// Return a LockConflictError, which will be handled by
			// the concurrency manager's HandleLockConflictError.
			return result.Result{}, &kvpb.LockConflictError{Locks: []roachpb.Lock{res.Intent.AsLock()}}
		} else {
			return result.Result{}, kvpb.NewRefreshFailedError(ctx, kvpb.RefreshFailedError_REASON_INTENT, res.Intent.Key, res.Intent.Txn.WriteTimestamp, kvpb.WithConflictingTxn(&res.Intent.Txn))
		}
	}

	return result.Result{}, nil
}
