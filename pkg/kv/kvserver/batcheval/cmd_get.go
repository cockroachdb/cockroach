// Copyright 2014 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func init() {
	RegisterReadOnlyCommand(roachpb.Get, DefaultDeclareIsolatedKeys, Get)
}

// Get returns the value for a specified key.
func Get(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.GetRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.GetResponse)

	getRes, err := storage.MVCCGet(ctx, reader, args.Key, h.Timestamp, storage.MVCCGetOptions{
		Inconsistent:          h.ReadConsistency != roachpb.CONSISTENT,
		SkipLocked:            h.WaitPolicy == lock.WaitPolicy_SkipLocked,
		Txn:                   h.Txn,
		FailOnMoreRecent:      args.KeyLocking != lock.None,
		Uncertainty:           cArgs.Uncertainty,
		MemoryAccount:         cArgs.EvalCtx.GetResponseMemoryAccount(),
		LockTable:             cArgs.Concurrency,
		DontInterleaveIntents: cArgs.DontInterleaveIntents,
		MaxKeys:               cArgs.Header.MaxSpanRequestKeys,
		TargetBytes:           cArgs.Header.TargetBytes,
		AllowEmpty:            cArgs.Header.AllowEmpty,
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
	if h.ReadConsistency == roachpb.READ_UNCOMMITTED {
		var intentVals []roachpb.KeyValue
		// NOTE: MVCCGet uses a Prefix iterator, so we want to use one in
		// CollectIntentRows as well so that we're guaranteed to use the same
		// cached iterator and observe a consistent snapshot of the engine.
		const usePrefixIter = true
		intentVals, err = CollectIntentRows(ctx, reader, usePrefixIter, intents)
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
	if args.KeyLocking != lock.None && h.Txn != nil && getRes.Value != nil {
		acq := roachpb.MakeLockAcquisition(h.Txn, args.Key, lock.Unreplicated)
		res.Local.AcquiredLocks = []roachpb.LockAcquisition{acq}
	}
	res.Local.EncounteredIntents = intents
	return res, err
}
