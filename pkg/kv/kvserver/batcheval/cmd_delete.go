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
	"github.com/cockroachdb/cockroach/pkg/storage"
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

	var err error
	reply.FoundKey, err = storage.MVCCDelete(
		ctx, readWriter, cArgs.Stats, args.Key, h.Timestamp, cArgs.Now, h.Txn,
	)

	// If requested, replace point tombstones with range tombstones.
	if cArgs.EvalCtx.EvalKnobs().UseRangeTombstonesForPointDeletes && err == nil && h.Txn == nil {
		if err := storage.ReplacePointTombstonesWithRangeTombstones(
			ctx, spanset.DisableReadWriterAssertions(readWriter),
			cArgs.Stats, args.Key, args.EndKey); err != nil {
			return result.Result{}, err
		}
	}

	// NB: even if MVCC returns an error, it may still have written an intent
	// into the batch. This allows callers to consume errors like WriteTooOld
	// without re-evaluating the batch. This behavior isn't particularly
	// desirable, but while it remains, we need to assume that an intent could
	// have been written even when an error is returned. This is harmless if the
	// error is not consumed by the caller because the result will be discarded.
	return result.FromAcquiredLocks(h.Txn, args.Key), err
}
