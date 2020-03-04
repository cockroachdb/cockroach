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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
)

func init() {
	RegisterReadWriteCommand(roachpb.Merge, DefaultDeclareKeys, Merge)
}

// Merge is used to merge a value into an existing key. Merge is an
// efficient accumulation operation which is exposed by RocksDB, used
// by CockroachDB for the efficient accumulation of certain
// values. Due to the difficulty of making these operations
// transactional, merges are not currently exposed directly to
// clients. Merged values are explicitly not MVCC data.
func Merge(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.MergeRequest)
	h := cArgs.Header

	return result.Result{}, storage.MVCCMerge(ctx, readWriter, cArgs.Stats, args.Key, h.Timestamp, args.Value)
}
