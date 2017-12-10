// Copyright 2017 The Cockroach Authors.
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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func init() {
	RegisterCommand(roachpb.ClearRange, declareKeysClearRange, ClearRange)
}

func declareKeysClearRange(
	desc roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	DefaultDeclareKeys(desc, header, req, spans)
	// We look up the range descriptor key to check whether the span
	// is equal to the entire range for fast stats updating.
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
	// Add the GC threshold key, as this is updated as part of clear a
	// range of data.
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: keys.RangeLastGCKey(header.RangeID)})
}

// ClearRange wipes all MVCC versions of keys covered by the specified
// span, adjusting the MVCC stats accordingly.
//
// Note that "correct" use of this command is only possible for key
// spans consisting of user data that we know is not being written to
// or queried any more, such as after a DROP or TRUNCATE table.
func ClearRange(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	log.VEventf(ctx, 2, "ClearRange %+v", cArgs.Args)

	// Encode MVCCKey values for start and end of clear span.
	args := cArgs.Args.(*roachpb.ClearRangeRequest)
	from := engine.MVCCKey{Key: args.Key}
	to := engine.MVCCKey{Key: args.EndKey}

	// Before clearing, compute the delta in MVCCStats. If the cleared
	// span is the entire range, computing MVCCStats is easy. We just
	// negate all fields except sys bytes and count.
	desc := cArgs.EvalCtx.Desc()
	if desc.StartKey.Equal(args.Key) && desc.EndKey.Equal(args.EndKey) {
		// Note this it is valid to use the full range MVCC stats, as
		// opposed to the usual method of computing only a localizied
		// stats delta, because a full-range clear prevents any concurrent
		// access to the stats.
		statsCurrent := cArgs.EvalCtx.GetMVCCStats()
		cArgs.Stats.Subtract(statsCurrent)
		cArgs.Stats.SysCount = 0 // no change
		cArgs.Stats.SysBytes = 0 // no change
	} else {
		// Otherwise, we determine the delta by computing stats across
		// the key span to be cleared.
		iter := batch.NewIterator(false)
		defer iter.Close()
		statsDelta, err := iter.ComputeStats(from, to, 0 /* nowNanos */)
		if err != nil {
			return result.Result{}, err
		}
		cArgs.Stats.Subtract(statsDelta)
	}

	// Protect against multiple ClearRange or GC requests arriving out
	// of order; we track the maximum timestamps.
	var pd result.Result
	gcThreshold := cArgs.EvalCtx.GetGCThreshold()
	gcThreshold.Forward(args.GCThreshold)
	pd.Replicated.State = &storagebase.ReplicaState{
		GCThreshold: &gcThreshold,
	}
	stateLoader := MakeStateLoader(cArgs.EvalCtx)
	if err := stateLoader.SetGCThreshold(ctx, batch, cArgs.Stats, &gcThreshold); err != nil {
		return result.Result{}, err
	}

	// Clear the key span.
	return pd, batch.ClearRange(from, to)
}
