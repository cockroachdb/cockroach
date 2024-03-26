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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
)

func init() {
	RegisterReadWriteCommand(kvpb.Delete, DefaultDeclareIsolatedKeys, Delete)
}

// Delete deletes the key and value specified by key.
func Delete(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.DeleteRequest)
	h := cArgs.Header
	reply := resp.(*kvpb.DeleteResponse)

	opts := storage.MVCCWriteOptions{
		Txn:                            h.Txn,
		LocalTimestamp:                 cArgs.Now,
		Stats:                          cArgs.Stats,
		ReplayWriteTimestampProtection: h.AmbiguousReplayProtection,
		OmitInRangefeeds:               cArgs.OmitInRangefeeds,
		MaxLockConflicts:               storage.MaxConflictsPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV),
		TargetLockConflictBytes:        storage.TargetBytesPerLockConflictError.Get(&cArgs.EvalCtx.ClusterSettings().SV),
		Category:                       fs.BatchEvalReadCategory,
	}

	var err error
	var acq roachpb.LockAcquisition
	reply.FoundKey, acq, err = storage.MVCCDelete(
		ctx, readWriter, args.Key, h.Timestamp, opts,
	)
	if err != nil {
		return result.Result{}, err
	}

	// If requested, replace point tombstones with range tombstones.
	if cArgs.EvalCtx.EvalKnobs().UseRangeTombstonesForPointDeletes && h.Txn == nil {
		if err := storage.ReplacePointTombstonesWithRangeTombstones(
			ctx, spanset.DisableReadWriterAssertions(readWriter),
			cArgs.Stats, args.Key, args.EndKey); err != nil {
			return result.Result{}, err
		}
	}

	return result.WithAcquiredLocks(acq), nil
}
