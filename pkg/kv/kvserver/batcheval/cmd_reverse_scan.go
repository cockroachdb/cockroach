// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadWriteCommand(kvpb.ReverseScan, DefaultDeclareIsolatedKeys, ReverseScan)
}

// ReverseScan scans the key range specified by start key through
// end key in descending order up to some maximum number of results.
// maxKeys stores the number of scan results remaining for this batch
// (MaxInt64 for no limit).
func ReverseScan(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.ReverseScanRequest)
	h := cArgs.Header
	reply := resp.(*kvpb.ReverseScanResponse)

	var lockTableForSkipLocked storage.LockTableView
	if h.WaitPolicy == lock.WaitPolicy_SkipLocked {
		lockTableForSkipLocked = newRequestBoundLockTableView(
			readWriter, cArgs.Concurrency, h.Txn, args.KeyLockingStrength,
		)
		defer lockTableForSkipLocked.Close()
	}

	var res result.Result
	var scanRes storage.MVCCScanResult
	var err error

	readCategory := ScanReadCategory(cArgs.EvalCtx.AdmissionHeader())
	opts := storage.MVCCScanOptions{
		Inconsistent:            h.ReadConsistency != kvpb.CONSISTENT,
		SkipLocked:              h.WaitPolicy == lock.WaitPolicy_SkipLocked,
		Txn:                     h.Txn,
		ScanStats:               cArgs.ScanStats,
		Uncertainty:             cArgs.Uncertainty,
		MaxKeys:                 h.MaxSpanRequestKeys,
		MaxLockConflicts:        storage.MaxConflictsPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV),
		TargetLockConflictBytes: storage.TargetBytesPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV),
		TargetBytes:             h.TargetBytes,
		AllowEmpty:              h.AllowEmpty,
		WholeRowsOfSize:         h.WholeRowsOfSize,
		FailOnMoreRecent:        args.KeyLockingStrength != lock.None,
		Reverse:                 true,
		MemoryAccount:           cArgs.EvalCtx.GetResponseMemoryAccount(),
		LockTable:               lockTableForSkipLocked,
		DontInterleaveIntents:   cArgs.DontInterleaveIntents,
		ReadCategory:            readCategory,
		ReturnRawMVCCValues:     args.ReturnRawMVCCValues,
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

	if args.KeyLockingStrength != lock.None {
		acquiredLocks, err := acquireLocksOnKeys(
			ctx, readWriter, h.Txn, args.KeyLockingStrength, args.KeyLockingDurability,
			args.ScanFormat, &scanRes, cArgs.Stats, cArgs.EvalCtx.ClusterSettings())
		if err != nil {
			return result.Result{}, err
		}
		res.Local.AcquiredLocks = acquiredLocks
	}

	res.Local.EncounteredIntents = scanRes.Intents
	return res, nil
}
