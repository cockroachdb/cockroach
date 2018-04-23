// Copyright 2018 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/storage/rditer"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/abortspan"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
)

func init() {
	RegisterCommand(roachpb.Subsume, declareKeysSubsume, Subsume)
}

func declareKeysSubsume(
	desc roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	// TODO(benesch): verify that this really does cover everything.
	spans.Add(spanset.SpanReadWrite, roachpb.Span{
		Key:    desc.StartKey.AsRawKey(),
		EndKey: desc.EndKey.AsRawKey(),
	})
	spans.Add(spanset.SpanReadWrite, roachpb.Span{
		Key:    keys.MakeRangeKeyPrefix(desc.StartKey),
		EndKey: keys.MakeRangeKeyPrefix(desc.EndKey).PrefixEnd(),
	})
	rangeIDPrefix := keys.MakeRangeIDReplicatedPrefix(desc.RangeID)
	spans.Add(spanset.SpanReadWrite, roachpb.Span{
		Key:    rangeIDPrefix,
		EndKey: rangeIDPrefix.PrefixEnd(),
	})
	rangeIDUnreplicatedPrefix := keys.MakeRangeIDUnreplicatedPrefix(desc.RangeID)
	spans.Add(spanset.SpanReadWrite, roachpb.Span{
		Key:    rangeIDUnreplicatedPrefix,
		EndKey: rangeIDUnreplicatedPrefix.PrefixEnd(),
	})
	spans.Add(spanset.SpanReadOnly, roachpb.Span{
		Key:    abortspan.MinKey(desc.RangeID),
		EndKey: abortspan.MaxKey(desc.RangeID),
	})
}

// Subsume notifies a range that its left-hand neighbor has initiated a merge.
// It is the means by which the merging ranges ensure there is no moment in time
// where they could both process commands for the same keys, which would lead to
// stale reads and lost writes.
//
// Specifically, the receiving range guarantees that:
//   - when it responds, there are no in-flight commands,
//   - the snapshot in the response has the latest writes,
//   - it will not process future commands until it refreshes its range
//     descriptor with a consistent read from meta2, and
//   - if it finds that its range descriptor has been deleted, it self
//     destructs.
//
// Provided the merge transaction lays down an intent on the right-hand side's
// meta2 descriptor before sending a SubsumeRequest, these four guarantees
// combine to ensure a correct merge. If the merge transaction commits, the
// right-hand side never serves another request. If the merge transaction
// aborst, the right-hand side resumes processing of requests.
func Subsume(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.SubsumeRequest)
	reply := resp.(*roachpb.SubsumeResponse)
	desc := cArgs.EvalCtx.Desc()

	rangeIDPrefix := keys.MakeRangeIDReplicatedPrefix(desc.RangeID)
	rangeIDStart := engine.MakeMVCCMetadataKey(rangeIDPrefix)
	rangeIDEnd := engine.MakeMVCCMetadataKey(rangeIDPrefix.PrefixEnd())

	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer eng.Close()

	// XXX: This command reads the whole replica into memory. We'll need to be
	// more careful if merging large ranges.
	snapBatch := eng.NewBatch()
	defer snapBatch.Close()

	var ignoredMS enginepb.MVCCStats
	_, err := cArgs.EvalCtx.AbortSpan().CopyInto(snapBatch, &ignoredMS, args.LeftRangeID)
	if err != nil {
		return result.Result{}, err
	}

	iter := rditer.NewReplicaDataIterator(desc, batch, true /* replicatedOnly */)
	defer iter.Close()
	for ; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return result.Result{}, err
		} else if !ok {
			break
		}
		key, val := iter.Key(), iter.Value()
		// TODO(benesch): is there some way to get ReplicaDataIterator to skip over
		// the Range ID span entirely? Should we not be using a ReplicaDataIterator
		// here at all?
		if !key.Less(rangeIDStart) && key.Less(rangeIDEnd) {
			// Range ID keys are never applicable to the subsuming range.
			continue
		}
		snapBatch.Put(key, val)
	}
	reply.Data = snapBatch.Repr()

	ms, err := rditer.ComputeStatsForRange(desc, snapBatch, 0 /* nowNanos */)
	if err != nil {
		return result.Result{}, err
	}
	reply.MVCCStats = ms

	return result.Result{
		Local: result.LocalResult{SetMerging: true},
	}, nil
}
