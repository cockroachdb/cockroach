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
	RegisterReadWriteCommand(kvpb.Increment, DefaultDeclareIsolatedKeys, Increment)
}

// Increment increments the value (interpreted as varint64 encoded) and
// returns the newly incremented value (encoded as varint64). If no value
// exists for the key, zero is incremented.
func Increment(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.IncrementRequest)
	h := cArgs.Header
	reply := resp.(*kvpb.IncrementResponse)

	var err error
	reply.NewValue, err = storage.MVCCIncrement(
		ctx, readWriter, cArgs.Stats, args.Key, h.Timestamp, cArgs.Now, h.Txn, args.Increment)
	if err != nil {
		return result.Result{}, err
	}
	return result.FromAcquiredLocks(h.Txn, args.Key), nil
}
