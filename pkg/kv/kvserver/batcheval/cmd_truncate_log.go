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
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(roachpb.TruncateLog, declareKeysTruncateLog, TruncateLog)
}

func declareKeysTruncateLog(
	rs ImmutableRangeState,
	_ *roachpb.Header,
	_ roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
	_ time.Duration,
) {
	prefix := keys.RaftLogPrefix(rs.GetRangeID())
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()})
}

// TruncateLog discards a prefix of the raft log. Truncating part of a log that
// has already been truncated has no effect. If this range is not the one
// specified within the request body, the request will also be ignored.
func TruncateLog(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
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

	// Compute the number of bytes freed by this truncation. Note that using
	// firstIndex only make sense for the leaseholder as we base this off its
	// own first index (other replicas may have other first indexes). In
	// principle, this could be off either way, though in practice we don't
	// expect followers to have a first index smaller than the leaseholder's
	// (see #34287), and most of the time everyone's first index should be the
	// same.
	// Additionally, it is possible that a write-heavy range has multiple in
	// flight TruncateLogRequests, and using the firstIndex will result in
	// duplicate accounting. The ExpectedFirstIndex, populated for clusters at
	// LooselyCoupledRaftLogTruncation, allows us to avoid this problem.
	//
	// We have an additional source of error not mitigated by
	// ExpectedFirstIndex. There is nothing synchronizing firstIndex with the
	// state visible in readWriter. The former uses the in-memory state or
	// fetches directly from the Engine. The latter uses Engine state from some
	// point in time which can fall anywhere in the time interval starting from
	// when the readWriter was created up to where we create an MVCCIterator
	// below.
	// TODO(sumeer): we can eliminate this error as part of addressing
	// https://github.com/cockroachdb/cockroach/issues/55461 and
	// https://github.com/cockroachdb/cockroach/issues/70974 that discuss taking
	// a consistent snapshot of some Replica state and the engine.
	if args.ExpectedFirstIndex > firstIndex {
		firstIndex = args.ExpectedFirstIndex
	}
	start := keys.RaftLogKey(rangeID, firstIndex)
	end := keys.RaftLogKey(rangeID, args.Index)

	// Compute the stats delta that were to occur should the log entries be
	// purged. We do this as a side effect of seeing a new TruncatedState,
	// downstream of Raft.
	//
	// Note that any sideloaded payloads that may be removed by this truncation
	// are not tracked in the raft log delta. The delta will be adjusted below
	// raft.
	iter := readWriter.NewMVCCIterator(storage.MVCCKeyIterKind, storage.IterOptions{UpperBound: end})
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
	pd.Replicated.State = &kvserverpb.ReplicaState{
		TruncatedState: tState,
	}
	pd.Replicated.RaftLogDelta = ms.SysBytes
	if cArgs.EvalCtx.ClusterSettings().Version.ActiveVersionOrEmpty(ctx).IsActive(
		clusterversion.LooselyCoupledRaftLogTruncation) {
		pd.Replicated.RaftExpectedFirstIndex = firstIndex
	}
	return pd, nil
}
