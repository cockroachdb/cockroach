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
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

func init() {
	// Depending on the cluster version, Refresh requests  may or may not declare locks.
	// See DeclareKeysForRefresh for details.
	RegisterReadOnlyCommand(kvpb.RefreshRange, DeclareKeysForRefresh, RefreshRange)
}

// RefreshRange checks whether the key range specified has any values written in
// the interval (args.RefreshFrom, header.Timestamp].
func RefreshRange(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.RefreshRangeRequest)
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

	log.VEventf(ctx, 2, "refresh %s @[%s-%s]", args.Span(), refreshFrom, refreshTo)
	return result.Result{}, refreshRange(ctx, reader, args.Span(), refreshFrom, refreshTo, h.Txn.ID, h.WaitPolicy)
}

// refreshRange iterates over the specified key span until it discovers a value
// written after the refreshFrom timestamp but before or at the refreshTo
// timestamp. The iteration observes MVCC tombstones, which must be considered
// as conflicts during a refresh. The iteration also observes intents, and any
// intent that is not owned by the specified txn ID is considered a conflict.
//
// If such a conflict is found, the function returns an error. Otherwise, no
// error is returned.
func refreshRange(
	ctx context.Context,
	reader storage.Reader,
	span roachpb.Span,
	refreshFrom, refreshTo hlc.Timestamp,
	txnID uuid.UUID,
	wp lock.WaitPolicy,
) error {
	// Construct an incremental iterator with the desired time bounds. Incremental
	// iterators will emit MVCC tombstones by default and will emit intents when
	// configured to do so (see IntentPolicy).
	iter, err := storage.NewMVCCIncrementalIterator(ctx, reader, storage.MVCCIncrementalIterOptions{
		KeyTypes:     storage.IterKeyTypePointsAndRanges,
		StartKey:     span.Key,
		EndKey:       span.EndKey,
		StartTime:    refreshFrom, // exclusive
		EndTime:      refreshTo,   // inclusive
		IntentPolicy: storage.MVCCIncrementalIterIntentPolicyEmit,
		ReadCategory: fs.BatchEvalReadCategory,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	var meta enginepb.MVCCMetadata
	for iter.SeekGE(storage.MVCCKey{Key: span.Key}); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}

		key := iter.UnsafeKey().Clone()

		if _, hasRange := iter.HasPointAndRange(); hasRange {
			return kvpb.NewRefreshFailedError(ctx, kvpb.RefreshFailedError_REASON_COMMITTED_VALUE, key.Key, iter.RangeKeys().Versions[0].Timestamp)
		}

		if !key.IsValue() {
			// Found an intent. Check whether it is owned by this transaction.
			// If so, proceed with iteration. Otherwise, return an error.
			v, err := iter.UnsafeValue()
			if err != nil {
				return err
			}
			if err := protoutil.Unmarshal(v, &meta); err != nil {
				return errors.Wrapf(err, "unmarshaling mvcc meta: %v", key)
			}
			if meta.IsInline() {
				// Ignore inline MVCC metadata. We don't expect to see this in practice
				// when performing a refresh of an MVCC keyspace.
				continue
			}
			if meta.Txn.ID == txnID {
				// Ignore the transaction's own intent and skip past the corresponding
				// provisional key-value. To do this, iterate to the provisional
				// key-value, validate its timestamp, then iterate again.
				iter.Next()
				if ok, err := iter.Valid(); err != nil {
					return errors.Wrap(err, "iterating to provisional value for intent")
				} else if !ok {
					return errors.Errorf("expected provisional value for intent")
				}
				if meta.Timestamp.ToTimestamp() != iter.UnsafeKey().Timestamp {
					return errors.Errorf("expected provisional value for intent with ts %s, found %s",
						meta.Timestamp, iter.UnsafeKey().Timestamp)
				}
				continue
			}
			// TODO(mira): Remove after V23_2_RemoveLockTableWaiterTouchPush is deleted.
			if wp == lock.WaitPolicy_Error {
				// Return a LockConflictError, which will be handled by
				// the concurrency manager's HandleLockConflictError.
				return &kvpb.LockConflictError{Locks: []roachpb.Lock{roachpb.MakeLock(meta.Txn, key.Key, lock.Intent)}}
			} else {
				return kvpb.NewRefreshFailedError(ctx, kvpb.RefreshFailedError_REASON_INTENT, key.Key, meta.Txn.WriteTimestamp, kvpb.WithConflictingTxn(meta.Txn))
			}
		}

		// If a committed value is found, return an error.
		return kvpb.NewRefreshFailedError(ctx, kvpb.RefreshFailedError_REASON_COMMITTED_VALUE, key.Key, key.Timestamp)
	}
	return nil
}
