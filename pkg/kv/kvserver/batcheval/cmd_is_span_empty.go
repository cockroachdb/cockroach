// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadOnlyCommand(kvpb.IsSpanEmpty, DefaultDeclareKeys, IsSpanEmpty)
}

// IsSpanEmpty determines whether there are any keys in the key span requested
// at any time. If there are any keys, the response header will have a NumKeys
// value of 1.
func IsSpanEmpty(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.IsSpanEmptyRequest)
	reply := resp.(*kvpb.IsSpanEmptyResponse)
	isEmpty, err := storage.MVCCIsSpanEmpty(ctx, reader, storage.MVCCIsSpanEmptyOptions{
		StartKey: args.Key,
		EndKey:   args.EndKey,
	})
	if err != nil {
		return result.Result{}, errors.Wrap(err, "IsSpanEmpty")
	}
	if !isEmpty {
		reply.NumKeys++
	}
	return result.Result{}, nil
}
