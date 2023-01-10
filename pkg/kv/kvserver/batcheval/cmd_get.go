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

	if h.MaxSpanRequestKeys < 0 || h.TargetBytes < 0 {
		// Receipt of a GetRequest with negative MaxSpanRequestKeys or TargetBytes
		// indicates that the request was part of a batch that has already exhausted
		// its limit, which means that we should *not* serve the request and return
		// a ResumeSpan for this GetRequest.
		//
		// This mirrors the logic in MVCCScan, though the logic in MVCCScan is
		// slightly lower in the stack.
		reply.ResumeSpan = &roachpb.Span{Key: args.Key}
		if h.MaxSpanRequestKeys < 0 {
			reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
		} else if h.TargetBytes < 0 {
			reply.ResumeReason = roachpb.RESUME_BYTE_LIMIT
		}
		return result.Result{}, nil
	}

	getRes, err := storage.MVCCGet(ctx, reader, args.Key, h.Timestamp, storage.MVCCGetOptions{
		Inconsistent:          h.ReadConsistency != roachpb.CONSISTENT,
		SkipLocked:            h.WaitPolicy == lock.WaitPolicy_SkipLocked,
		Txn:                   h.Txn,
		FailOnMoreRecent:      args.KeyLocking != lock.None,
		Uncertainty:           cArgs.Uncertainty,
		MemoryAccount:         cArgs.EvalCtx.GetResponseMemoryAccount(),
		LockTable:             cArgs.Concurrency,
		DontInterleaveIntents: cArgs.DontInterleaveIntents,
	})
	if err != nil {
		return result.Result{}, err
	}
	if getRes.Value != nil {
		// NB: This calculation is different from Scan, since Scan responses include
		// the key/value pair while Get only includes the value.
		numBytes := int64(len(getRes.Value.RawBytes))
		if h.TargetBytes > 0 && h.AllowEmpty && numBytes > h.TargetBytes {
			reply.ResumeSpan = &roachpb.Span{Key: args.Key}
			reply.ResumeReason = roachpb.RESUME_BYTE_LIMIT
			reply.ResumeNextBytes = numBytes
			return result.Result{}, nil
		}
		reply.NumKeys = 1
		reply.NumBytes = numBytes
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
