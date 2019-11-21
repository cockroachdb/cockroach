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
	"errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func init() {
	RegisterCommand(roachpb.FastGC, declareFastGCKey, FastGC)
}

func declareFastGCKey(
	desc *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	// Intentionally don't call DefaultDeclareKeys: the key range in the header
	// is usually the whole range (pending resolution of #7880).
	fgr := req.(*roachpb.FastGCRequest)
	if keys.IsLocal(fgr.GcKey.Key) {
		spans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: fgr.GcKey.Key})
	} else {
		spans.AddMVCC(spanset.SpanReadWrite, roachpb.Span{Key: fgr.GcKey.Key}, header.Timestamp)
	}
	spans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
}

// FastGC wipes all MVCC versions of keys covered by the specified
// span, adjusting the MVCC stats accordingly.
//
// Note that "correct" use of this command is only possible for key
// spans consisting of user data that we know is not being written to
// or queried any more, such as after a DROP or TRUNCATE table, or
// DROP index.
func FastGC(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	log.VEventf(ctx, 2, "FastGC %+v", cArgs.Args)

	// Encode MVCCKey values for start and end of clear span.
	args := cArgs.Args.(*roachpb.FastGCRequest)
	desc := cArgs.EvalCtx.Desc()
	// TODO check GC span
	if !desc.ContainsKey(roachpb.RKey(args.GcKey.Key)) {
		return result.Result{}, errors.New("range not contains key")
	}
	from := engine.MVCCKey{Key: args.GcKey.Key, Timestamp: args.GcKey.Timestamp}
	to := engine.MVCCKey{Key: from.Key.PrefixEnd()}
	var pd result.Result

	statsDelta := args.Delta
	cArgs.Stats.Add(statsDelta)
	if err := batch.ClearRange(from, to); err != nil {
		return result.Result{}, err
	}
	return pd, nil
}
