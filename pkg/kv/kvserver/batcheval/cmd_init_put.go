// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
)

func init() {
	RegisterReadWriteCommand(kvpb.InitPut, DefaultDeclareIsolatedKeys, InitPut)
}

// InitPut sets the value for a specified key only if it doesn't exist. It
// returns a ConditionFailedError if the key exists with an existing value that
// is different from the value provided. If FailOnTombstone is set to true,
// tombstones count as mismatched values and will cause a ConditionFailedError.
func InitPut(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.InitPutRequest)
	h := cArgs.Header

	failOnTombstones := args.FailOnTombstones && !cArgs.EvalCtx.EvalKnobs().DisableInitPutFailOnTombstones
	opts := storage.MVCCWriteOptions{
		Txn:                            h.Txn,
		LocalTimestamp:                 cArgs.Now,
		Stats:                          cArgs.Stats,
		ReplayWriteTimestampProtection: h.AmbiguousReplayProtection,
		OmitInRangefeeds:               cArgs.OmitInRangefeeds,
		OriginID:                       h.WriteOptions.GetOriginID(),
		OriginTimestamp:                h.WriteOptions.GetOriginTimestamp(),
		MaxLockConflicts:               storage.MaxConflictsPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV),
		TargetLockConflictBytes:        storage.TargetBytesPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV),
		Category:                       fs.BatchEvalReadCategory,
	}

	var err error
	var acq roachpb.LockAcquisition
	if args.Blind {
		acq, err = storage.MVCCBlindInitPut(
			ctx, readWriter, args.Key, h.Timestamp, args.Value, failOnTombstones, opts)
	} else {
		acq, err = storage.MVCCInitPut(
			ctx, readWriter, args.Key, h.Timestamp, args.Value, failOnTombstones, opts)
	}
	if err != nil {
		return result.Result{}, err
	}
	return result.WithAcquiredLocks(acq), nil
}
