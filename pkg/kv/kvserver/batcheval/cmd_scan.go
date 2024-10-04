// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(kvpb.Scan, DefaultDeclareIsolatedKeys, Scan)
}

// Scan scans the key range specified by start key through end key
// in ascending order up to some maximum number of results. maxKeys
// stores the number of scan results remaining for this batch
// (MaxInt64 for no limit).
func Scan(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.ScanRequest)
	h := cArgs.Header
	reply := resp.(*kvpb.ScanResponse)

	if err := maybeDisallowSkipLockedRequest(h, args.KeyLockingStrength); err != nil {
		return result.Result{}, err
	}

	var res result.Result
	var scanRes storage.MVCCScanResult
	var err error

	opts := storage.MVCCScanOptions{
		Inconsistent:          h.ReadConsistency != kvpb.CONSISTENT,
		SkipLocked:            h.WaitPolicy == lock.WaitPolicy_SkipLocked,
		Txn:                   h.Txn,
		ScanStats:             cArgs.ScanStats,
		Uncertainty:           cArgs.Uncertainty,
		MaxKeys:               h.MaxSpanRequestKeys,
		MaxLockConflicts:      storage.MaxConflictsPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV),
		TargetBytes:           h.TargetBytes,
		AllowEmpty:            h.AllowEmpty,
		WholeRowsOfSize:       h.WholeRowsOfSize,
		FailOnMoreRecent:      args.KeyLockingStrength != lock.None,
		Reverse:               false,
		MemoryAccount:         cArgs.EvalCtx.GetResponseMemoryAccount(),
		LockTable:             cArgs.Concurrency,
		DontInterleaveIntents: cArgs.DontInterleaveIntents,
	}

	switch args.ScanFormat {
	case kvpb.BATCH_RESPONSE:
		scanRes, err = storage.MVCCScanToBytes(
			ctx, readWriter, args.Key, args.EndKey, h.Timestamp, opts)
		if err != nil {
			return result.Result{}, err
		}
		reply.BatchResponses = scanRes.KVData
	case kvpb.COL_BATCH_RESPONSE:
		scanRes, err = storage.MVCCScanToCols(
			ctx, readWriter, cArgs.Header.IndexFetchSpec, args.Key, args.EndKey,
			h.Timestamp, opts, cArgs.EvalCtx.ClusterSettings(),
		)
		if err != nil {
			return result.Result{}, err
		}
		if len(scanRes.ColBatches) > 0 {
			reply.ColBatches.ColBatches = scanRes.ColBatches
		} else {
			reply.BatchResponses = scanRes.KVData
		}
	case kvpb.KEY_VALUES:
		scanRes, err = storage.MVCCScan(
			ctx, readWriter, args.Key, args.EndKey, h.Timestamp, opts)
		if err != nil {
			return result.Result{}, err
		}
		reply.Rows = scanRes.KVs
	default:
		panic(fmt.Sprintf("Unknown scanFormat %d", args.ScanFormat))
	}

	reply.NumKeys = scanRes.NumKeys
	reply.NumBytes = scanRes.NumBytes

	if scanRes.ResumeSpan != nil {
		reply.ResumeSpan = scanRes.ResumeSpan
		reply.ResumeReason = scanRes.ResumeReason
		reply.ResumeNextBytes = scanRes.ResumeNextBytes
	}

	if h.ReadConsistency == kvpb.READ_UNCOMMITTED {
		// NOTE: MVCCScan doesn't use a Prefix iterator, so we don't want to use
		// one in CollectIntentRows either so that we're guaranteed to use the
		// same cached iterator and observe a consistent snapshot of the engine.
		const usePrefixIter = false
		reply.IntentRows, err = CollectIntentRows(ctx, readWriter, usePrefixIter, scanRes.Intents)
		if err != nil {
			return result.Result{}, err
		}
	}

	if args.KeyLockingStrength != lock.None && h.Txn != nil {
		acquiredLocks, err := acquireLocksOnKeys(
			ctx, readWriter, h.Txn, args.KeyLockingStrength, args.KeyLockingDurability,
			args.ScanFormat, &scanRes, cArgs.Stats, cArgs.EvalCtx.ClusterSettings())
		if err != nil {
			return result.Result{}, maybeInterceptDisallowedSkipLockedUsage(h, err)
		}
		res.Local.AcquiredLocks = acquiredLocks
	}

	res.Local.EncounteredIntents = scanRes.Intents
	return res, nil
}

// maybeInterceptDisallowedSkipLockedUsage checks if read evaluation for a skip
// locked request encountered a replicated lock by checking the supplier error
// type. It transforms the error into an unimplemented error if that's the case;
// otherwise, the error is passed through.
//
// TODO(arul): this won't be needed once
// https://github.com/cockroachdb/cockroach/issues/115057 is addressed.
func maybeInterceptDisallowedSkipLockedUsage(h kvpb.Header, err error) error {
	if h.WaitPolicy == lock.WaitPolicy_SkipLocked && errors.HasType(err, (*kvpb.LockConflictError)(nil)) {
		return MarkSkipLockedUnsupportedError(errors.UnimplementedError(
			errors.IssueLink{IssueURL: build.MakeIssueURL(115057)},
			"usage of replicated locks in conjunction with skip locked wait policy is currently unsupported",
		))
	}
	return err
}

// maybeDisallowSkipLockedRequest returns an error if the skip locked wait
// policy is used in conjunction with shared locks.
//
// TODO(arul): this won't be needed once
// https://github.com/cockroachdb/cockroach/issues/110743 is addressed. Until
// then, we return unimplemented errors.
func maybeDisallowSkipLockedRequest(h kvpb.Header, str lock.Strength) error {
	if h.WaitPolicy == lock.WaitPolicy_SkipLocked && str == lock.Shared {
		return MarkSkipLockedUnsupportedError(errors.UnimplementedError(
			errors.IssueLink{IssueURL: build.MakeIssueURL(110743)},
			"usage of shared locks in conjunction with skip locked wait policy is currently unsupported",
		))
	}
	return nil
}

// SkipLockedUnsupportedError is used to mark errors resulting from unsupported
// (currently unimplemented) uses of the skip locked wait policy.
type SkipLockedUnsupportedError struct{}

func (e *SkipLockedUnsupportedError) Error() string {
	return "unsupported skip locked use error"
}

// MarkSkipLockedUnsupportedError wraps the given error, if not nil, as a skip
// locked unsupported error.
func MarkSkipLockedUnsupportedError(cause error) error {
	if cause == nil {
		return nil
	}
	return errors.Mark(cause, &SkipLockedUnsupportedError{})
}
