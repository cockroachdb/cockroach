// Copyright 2014 The Cockroach Authors.
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
)

func init() {
	RegisterReadWriteCommand(kvpb.Get, DefaultDeclareIsolatedKeys, Get)
}

// Get returns the value for a specified key.
func Get(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.GetRequest)
	h := cArgs.Header
	reply := resp.(*kvpb.GetResponse)

	var lockTableForSkipLocked storage.LockTableView
	if h.WaitPolicy == lock.WaitPolicy_SkipLocked {
		lockTableForSkipLocked = newRequestBoundLockTableView(
			readWriter, cArgs.Concurrency, h.Txn, args.KeyLockingStrength,
		)
		defer lockTableForSkipLocked.Close()
	}

	getRes, err := storage.MVCCGet(ctx, readWriter, args.Key, h.Timestamp, storage.MVCCGetOptions{
		Inconsistent:          h.ReadConsistency != kvpb.CONSISTENT,
		SkipLocked:            h.WaitPolicy == lock.WaitPolicy_SkipLocked,
		Txn:                   h.Txn,
		FailOnMoreRecent:      args.KeyLockingStrength != lock.None,
		ScanStats:             cArgs.ScanStats,
		Uncertainty:           cArgs.Uncertainty,
		MemoryAccount:         cArgs.EvalCtx.GetResponseMemoryAccount(),
		LockTable:             lockTableForSkipLocked,
		DontInterleaveIntents: cArgs.DontInterleaveIntents,
		MaxKeys:               cArgs.Header.MaxSpanRequestKeys,
		TargetBytes:           cArgs.Header.TargetBytes,
		AllowEmpty:            cArgs.Header.AllowEmpty,
		ReadCategory:          fs.BatchEvalReadCategory,
		ReturnRawMVCCValues:   args.ReturnRawMVCCValues,
	})
	if err != nil {
		return result.Result{}, err
	}
	reply.ResumeSpan = getRes.ResumeSpan
	reply.ResumeReason = getRes.ResumeReason
	reply.ResumeNextBytes = getRes.ResumeNextBytes
	reply.NumKeys = getRes.NumKeys
	reply.NumBytes = getRes.NumBytes
	if reply.ResumeSpan != nil {
		return result.Result{}, nil
	}
	var intents []roachpb.Intent
	if getRes.Intent != nil {
		intents = append(intents, *getRes.Intent)
	}

	reply.Value = getRes.Value
	if h.ReadConsistency == kvpb.READ_UNCOMMITTED {
		var intentVals []roachpb.KeyValue
		// NOTE: MVCCGet uses a Prefix iterator, so we want to use one in
		// CollectIntentRows as well so that we're guaranteed to use the same
		// cached iterator and observe a consistent snapshot of the engine.
		const usePrefixIter = true
		intentVals, err = CollectIntentRows(ctx, readWriter, usePrefixIter, intents)
		if err == nil {
			switch len(intentVals) {
			case 0:
			case 1:
				reply.IntentValue = &intentVals[0].Value
			default:
				log.Fatalf(ctx, "more than 1 intent on single key: %v", intentVals)
			}
		}
	}

	var res result.Result
	if args.KeyLockingStrength != lock.None && getRes.Value != nil {
		acq, err := acquireLockOnKey(ctx, readWriter, h.Txn, args.KeyLockingStrength,
			args.KeyLockingDurability, args.Key, cArgs.Stats, cArgs.EvalCtx.ClusterSettings())
		if err != nil {
			return result.Result{}, err
		}
		if !acq.Empty() {
			res.Local.AcquiredLocks = []roachpb.LockAcquisition{acq}
		}
	}
	res.Local.EncounteredIntents = intents
	return res, err
}
