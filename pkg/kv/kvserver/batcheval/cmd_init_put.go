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
	"github.com/cockroachdb/cockroach/pkg/storage"
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

	if args.FailOnTombstones && cArgs.EvalCtx.EvalKnobs().DisableInitPutFailOnTombstones {
		args.FailOnTombstones = false
	}

	opts := storage.MVCCWriteOptions{
		Txn:                            h.Txn,
		LocalTimestamp:                 cArgs.Now,
		Stats:                          cArgs.Stats,
		ReplayWriteTimestampProtection: h.AmbiguousReplayProtection,
	}

	var err error
	if args.Blind {
		err = storage.MVCCBlindInitPut(
			ctx, readWriter, args.Key, h.Timestamp, args.Value, args.FailOnTombstones, opts)
	} else {
		err = storage.MVCCInitPut(
			ctx, readWriter, args.Key, h.Timestamp, args.Value, args.FailOnTombstones, opts)
	}
	if err != nil {
		return result.Result{}, err
	}
	return result.FromAcquiredLocks(h.Txn, args.Key), nil
}
