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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
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
	_ *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	spans.Add(spanset.SpanReadWrite, roachpb.Span{Key: keys.RaftTruncatedStateLegacyKey(header.RangeID)})
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

	var legacyTruncatedState roachpb.RaftTruncatedState
	legacyKeyFound, err := engine.MVCCGetProto(
		ctx, batch, keys.RaftTruncatedStateLegacyKey(cArgs.EvalCtx.GetRangeID()),
		hlc.Timestamp{}, &legacyTruncatedState, engine.MVCCGetOptions{},
	)
	if err != nil {
		return result.Result{}, err
	}

	// See the comment on the cluster version for all the moving parts involved
	// in migrating into this cluster version. Note that if the legacy key is
	// missing, the cluster version has been bumped (though we may not know it
	// yet) and we keep using the unreplicated key.
	useNewUnreplicatedTruncatedStateKey := cArgs.EvalCtx.ClusterSettings().Version.IsActive(cluster.VersionUnreplicatedRaftTruncatedState) || !legacyKeyFound

	firstIndex, err := cArgs.EvalCtx.GetFirstIndex()
	if err != nil {
		return result.Result{}, errors.Wrap(err, "getting first index")
	}
	// Have we already truncated this log? If so, just return without an error.
	// Note that there may in principle be followers whose Raft log is longer
	// than this node's, but to issue a truncation we also need the *term* for
	// the new truncated state, which we can't obtain if we don't have the log
	// entry ourselves.
	//
	// TODO(tbg): think about synthesizing a valid term. Can we use the next
	// existing entry's term?
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
		return result.Result{}, errors.Wrap(err, "getting term")
	}

	// Compute the number of bytes freed by this truncation. Note that this will
	// only make sense for the leaseholder as we base this off its own first
	// index (other replicas may have other first indexes assuming we're not
	// still using the legacy truncated state key). In principle, this could be
	// off either way, though in practice we don't expect followers to have
	// a first index smaller than the leaseholder's (see #34287), and most of
	// the time everyone's first index should be the same.
	start := engine.MakeMVCCMetadataKey(keys.RaftLogKey(rangeID, firstIndex))
	end := engine.MakeMVCCMetadataKey(keys.RaftLogKey(rangeID, args.Index))

	// Compute the stats delta that were to occur should the log entries be
	// purged. We do this as a side effect of seeing a new TruncatedState,
	// downstream of Raft.
	//
	// Note that any sideloaded payloads that may be removed by this truncation
	// don't matter; they're not tracked in the raft log delta.
	//
	// TODO(tbg): it's difficult to prove that this computation doesn't have
	// bugs that let it diverge. It might be easier to compute the stats
	// from scratch, stopping when 4mb (defaultRaftLogTruncationThreshold)
	// is reached as at that point we'll truncate aggressively anyway.
	iter := batch.NewIterator(engine.IterOptions{UpperBound: end.Key})
	defer iter.Close()
	// We can pass zero as nowNanos because we're only interested in SysBytes.
	ms, err := iter.ComputeStats(start, end, 0 /* nowNanos */)
	if err != nil {
		return result.Result{}, errors.Wrap(err, "while computing stats of Raft log freed by truncation")
	}
	ms.SysBytes = -ms.SysBytes // simulate the deletion

	tState := &roachpb.RaftTruncatedState{
		Index: args.Index - 1,
		Term:  term,
	}

	var pd result.Result
	pd.Replicated.State = &storagepb.ReplicaState{
		TruncatedState: tState,
	}

	pd.Replicated.RaftLogDelta = ms.SysBytes

	if !useNewUnreplicatedTruncatedStateKey {
		return pd, MakeStateLoader(cArgs.EvalCtx).SetLegacyRaftTruncatedState(ctx, batch, cArgs.Stats, tState)
	}
	if legacyKeyFound {
		// Time to migrate by deleting the legacy key. The downstream-of-Raft
		// code will atomically rewrite the truncated state (supplied via the
		// side effect) into the new unreplicated key.
		if err := engine.MVCCDelete(
			ctx, batch, cArgs.Stats, keys.RaftTruncatedStateLegacyKey(cArgs.EvalCtx.GetRangeID()),
			hlc.Timestamp{}, nil, /* txn */
		); err != nil {
			return result.Result{}, err
		}
	}

	return pd, nil
}
