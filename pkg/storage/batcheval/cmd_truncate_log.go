// Copyright 2014 The Cockroach Authors.
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
	"math"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

func init() {
	RegisterCommand(roachpb.TruncateLog, declareKeysTruncateLog, TruncateLog)
}

func declareKeysTruncateLog(
	_ roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: keys.RaftTruncatedStateKey(header.RangeID)})
	prefix := keys.RaftLogPrefix(header.RangeID)
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()})
}

// TruncateLog discards a prefix of the raft log. Truncating part of a log that
// has already been truncated has no effect. If this range is not the one
// specified within the request body, the request will also be ignored.
func TruncateLog(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.TruncateLogRequest)

	// After a merge, it's possible that this request was sent to the wrong
	// range based on the start key. This will cancel the request if this is not
	// the range specified in the request body.
	rangeID := cArgs.EvalCtx.GetRangeID()
	if rangeID != args.RangeID {
		log.Infof(ctx, "attempting to truncate raft logs for another range: r%d. Normally this is due to a merge and can be ignored.",
			args.RangeID)
		return result.Result{}, nil
	}

	// Have we already truncated this log? If so, just return without an error.
	firstIndex, err := cArgs.EvalCtx.GetFirstIndex()
	if err != nil {
		return result.Result{}, err
	}

	if firstIndex >= args.Index {
		if log.V(3) {
			log.Infof(ctx, "attempting to truncate previously truncated raft log. FirstIndex:%d, TruncateFrom:%d",
				firstIndex, args.Index)
		}
		return result.Result{}, nil
	}

	// args.Index is the first index to keep.
	term, err := cArgs.EvalCtx.GetTerm(args.Index - 1)
	if err != nil {
		return result.Result{}, err
	}

	start := engine.MakeMVCCMetadataKey(keys.RaftLogKey(rangeID, firstIndex))
	end := engine.MakeMVCCMetadataKey(keys.RaftLogKey(rangeID, args.Index).PrefixEnd())

	var ms enginepb.MVCCStats
	if cArgs.EvalCtx.ClusterSettings().Version.IsActive(cluster.VersionRaftLogTruncationBelowRaft) {
		// Compute the stats delta that were to occur should the log entries be
		// purged. We do this as a side effect of seeing a new TruncatedState,
		// downstream of Raft.
		//
		// Note that any sideloaded payloads that may be removed by this truncation
		// don't matter; they're not tracked in the raft log delta.
		iter := batch.NewIterator(engine.IterOptions{UpperBound: end.Key})
		defer iter.Close()
		// We can pass zero as nowNanos because we're only interested in SysBytes.
		var err error
		ms, err = iter.ComputeStats(start, end, 0 /* nowNanos */)
		if err != nil {
			return result.Result{}, errors.Wrap(err, "while computing stats of Raft log freed by truncation")
		}
		ms.SysBytes = -ms.SysBytes // simulate the deletion

	} else {
		if _, _, _, err := engine.MVCCDeleteRange(ctx, batch, &ms, start.Key, end.Key, math.MaxInt64, /* max */
			hlc.Timestamp{}, nil /* txn */, false /* returnKeys */); err != nil {
			return result.Result{}, err
		}
	}

	tState := &roachpb.RaftTruncatedState{
		Index: args.Index - 1,
		Term:  term,
	}

	var pd result.Result
	pd.Replicated.State = &storagepb.ReplicaState{
		TruncatedState: tState,
	}
	pd.Replicated.RaftLogDelta = ms.SysBytes

	return pd, MakeStateLoader(cArgs.EvalCtx).SetTruncatedState(ctx, batch, cArgs.Stats, tState)
}
