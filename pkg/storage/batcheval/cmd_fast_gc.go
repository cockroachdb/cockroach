// Copyright 2018 The ChuBao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/keys"
)

func init() {
	RegisterCommand(roachpb.FastGC, declareFastGCKey, FastGC)
}

func declareFastGCKey(
	desc roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	// Intentionally don't call DefaultDeclareKeys: the key range in the header
	// is usually the whole range (pending resolution of #7880).
	fgr := req.(*roachpb.FastGCRequest)
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: fgr.GcKey.Key})
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
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
	from := engine.MVCCKey{Key:args.GcKey.Key, Timestamp: args.GcKey.Timestamp}
	to := engine.MVCCKey{Key: from.Key.PrefixEnd()}
	var pd result.Result

	statsDelta := args.Delta
	cArgs.Stats.Add(statsDelta)
  log.Infof(ctx, "clear range [%s %s]", from, to)
	if err := batch.ClearRange(from, to); err != nil {
		return result.Result{}, err
	}
	return pd, nil
}
