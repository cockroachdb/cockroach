// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadOnlyCommand(roachpb.IsSpanEmpty, DefaultDeclareKeys, IsSpanEmpty)
}

// IsSpanEmpty determines whether there are any keys in the key span requested
// at any time. If there are any keys, the response header will have a NumKeys
// value of 1.
func IsSpanEmpty(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.IsSpanEmptyRequest)
	reply := resp.(*roachpb.IsSpanEmptyResponse)
	isEmpty, err := storage.MVCCIsSpanEmpty(ctx, reader, storage.MVCCIsSpanEmptyOptions{
		StartKey: args.Key,
		EndKey:   args.EndKey,
		StartTS:  hlc.MinTimestamp, // beginning of time
		EndTS:    hlc.MaxTimestamp, // end of time
	})
	if err != nil {
		return result.Result{}, errors.Wrap(err, "IsSpanEmpty")
	}
	if !isEmpty {
		reply.NumKeys++
	}
	return result.Result{}, nil
}
