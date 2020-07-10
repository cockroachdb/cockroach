// Copyright 2020 The Cockroach Authors.
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
	RegisterReadOnlyCommand(roachpb.CheckExists, DefaultDeclareIsolatedKeys, CheckExists)
}

// CheckExists scans the key range specified by start key
// through end key and returns whether the key range contained
// any keys.
func CheckExists(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.CheckExistsRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.CheckExistsResponse)

	scanRes, err := storage.MVCCScanToBytes(ctx, reader, args.Key, args.EndKey, h.Timestamp, storage.MVCCScanOptions{
		Inconsistent: h.ReadConsistency != roachpb.CONSISTENT,
		Txn:          h.Txn,
		MaxKeys:      1,
	})
	if err != nil {
		return result.Result{}, err
	}

	reply.Exists = scanRes.NumKeys > 0
	return result.FromEncounteredIntents(scanRes.Intents), nil
}
