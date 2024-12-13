// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"bytes"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(kvpb.Subsume, declareKeysSubsume, Subsume)
}

func declareKeysSubsume(
	_ ImmutableRangeState,
	_ *kvpb.Header,
	_ kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	// Subsume must not run concurrently with any other command. It declares a
	// non-MVCC write over every addressable key in the range; this guarantees
	// that it conflicts with any other command because every command must
	// declare at least one addressable key. It does not, in fact, write any
	// keys.
	declareAllKeys(latchSpans)
	return nil
}

// Subsume freezes a range for merging with its left-hand neighbor. When called
// correctly, it provides important guarantees that ensure there is no moment in
// time where the ranges involved in the merge could both process commands for
// the same keys.
//
// Specifically, the receiving replica guarantees that:
//
//  1. it is the leaseholder at the time the request executes,
//  2. when it responds, there are no commands in flight with a timestamp
//     greater than the FreezeStart timestamp provided in the response,
//  3. the MVCC statistics in the response reflect the latest writes,
//  4. it, and all future leaseholders for the range, will not process another
//     command until they refresh their range descriptor with a consistent read
//     from meta2, and
//  5. if it or any future leaseholder for the range finds that its range
//     descriptor has been deleted, it self destructs.
//
// To achieve guarantees four and five, when issuing a Subsume request, the
// caller must have a merge transaction open that has already placed deletion
// intents on both the local and meta2 copy of the right-hand range descriptor.
// The intent on the meta2 allows the leaseholder to block until the merge
// transaction completes by performing a consistent read for its meta2
// descriptor. The intent on the local descriptor allows future leaseholders to
// efficiently check whether a merge is in progress by performing a read of its
// local descriptor after acquiring the lease.
//
// The period of time after intents have been placed but before the merge
// transaction is complete is called the merge's "critical phase".
//
// SubsumeRequest does a write on newer cluster versions. This is accomplished
// by (a) marking SubsumeRequest.flags() with isWrite, and (b) returning a
// non-empty Result.Replicated. Both are necessary for SubsumeRequest to be
// replicated via Raft. The write is limited to updating the
// RangeForceFlushKey with a value corresponding to the index of the raft
// entry of the Subsume. Since this write is to a replicated Range-ID local
// key (and because it is done non-transactionally -- see
// kvserver.AdminMerge), it will not be rolled back if the distributed
// transaction doing the merge aborts. This is ok since the RangeForceFlushKey
// only provides a directive to the replication flow control v2 (RACv2)
// machinery to force-flush up to a particular Raft index. Additionally,
// safety relies on SubsumeRequest only writing to a Range-ID local key:
//
//   - Neither the SubsumeResponse.{MVCCStats,RangeIDLocalMVCCStats} includes
//     the effect of this write, which is ok since
//     MVCCState-RangeIDLocalMVCCStats is added to the merged range's stats and
//     the RHS's RangeForceFlushKey will be deleted.
//
//   - SubsumeResponse.LeaseAppliedIndex, say x, includes all requests preceding
//     this SubsumeRequest, but not this request. Since the merge txn only waits
//     for application on all RHS replicas up to x, it is possible that some
//     replica has not applied the SubsumeRequest and gets merged with the LHS.
//     This is ok since the RangeForceFlushKey of the RHS is deleted by the
//     merge.
//
// The SubsumeResponse.{FreezeStart,ClosedTimestamp,ReadSummary} are used in
// varying degrees to adjust the timestamp cache of the merged range (see
// Store.MergeRange). On the surface, this behavior is unaffected by whether
// SubsumeRequest is a read or write, since the effect of this SubsumeRequest
// on the timestamp cache is ignored either way (this request evaluation
// updates the timestamp cache after the call to Subsume returns), and is not
// relevant for correctness. However, there is a hazard if this SubsumeRequest
// (when it is a write) bumps the closed timestamp, say from t1 to t2.
// Followers could subsequently admit reads (on the RHS, before the merge)
// with timestamps in the interval (t1, t2], that will not be accounted for in
// the timestamp cache of the merged range (since the closed timestamp
// returned in SubsumeResponse.ClosedTimestamp=t1). Note that such follower
// writes are possible because followers do not freeze the range like the
// leaseholder does. The simple way to avoid this hazard is to not bump the
// closed timestamp when proposing a SubsumeRequest. This is accomplished by
// two means:
//   - Side transport (see Replica.BumpSideTransportClosed): Does not bump the
//     closed timestamp if the range merge is in progress. And Subsume ensures
//     that the merge is in progress by calling WatchForMerge below.
//   - Closed timestamp replicated via Raft (see
//     propBuf.allocateLAIAndClosedTimestampLocked): The closed timestamp is not
//     advanced if the BatchRequest contains a single request which is a
//     SubsumeRequest.
func Subsume(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.SubsumeRequest)
	reply := resp.(*kvpb.SubsumeResponse)

	// Verify that the Subsume request was sent to the correct range and that
	// the range's bounds have not changed during the merge transaction.
	desc := cArgs.EvalCtx.Desc()
	if !bytes.Equal(desc.StartKey, args.RightDesc.StartKey) ||
		!bytes.Equal(desc.EndKey, args.RightDesc.EndKey) {
		return result.Result{}, errors.Errorf("RHS range bounds do not match: %s != %s",
			args.RightDesc, desc)
	}

	// Sanity check that the requesting range is our left neighbor. The ordering
	// of operations in the AdminMerge transaction should make it impossible for
	// these ranges to be nonadjacent, but double check.
	if !bytes.Equal(args.LeftDesc.EndKey, desc.StartKey) {
		return result.Result{}, errors.Errorf("ranges are not adjacent: %s != %s",
			args.LeftDesc.EndKey, desc.StartKey)
	}

	// Sanity check the caller has initiated a merge transaction by checking for
	// a deletion intent on the local range descriptor. Read inconsistently at
	// the maximum timestamp to ensure that we see an intent if one exists,
	// regardless of what timestamp it is written at.
	descKey := keys.RangeDescriptorKey(desc.StartKey)
	intentRes, err := storage.MVCCGet(ctx, readWriter, descKey, hlc.MaxTimestamp,
		storage.MVCCGetOptions{Inconsistent: true, ReadCategory: fs.BatchEvalReadCategory})
	if err != nil {
		return result.Result{}, errors.Wrap(err, "fetching local range descriptor")
	} else if intentRes.Intent == nil {
		return result.Result{}, errors.Errorf("range missing intent on its local descriptor")
	}
	valRes, err := storage.MVCCGetAsTxn(ctx, readWriter, descKey, intentRes.Intent.Txn.WriteTimestamp, intentRes.Intent.Txn)
	if err != nil {
		return result.Result{}, errors.Wrap(err, "fetching local range descriptor as txn")
	} else if valRes.Value != nil {
		return result.Result{}, errors.Errorf("non-deletion intent on local range descriptor")
	}

	// NOTE: the deletion intent on the range's meta2 descriptor is just as
	// important to correctness as the deletion intent on the local descriptor,
	// but the check is too expensive as it would involve a network roundtrip on
	// most nodes.

	// Freeze the range. Do so by blocking all requests while a newly launched
	// async goroutine watches (pushes with low priority) the merge transaction.
	// This will also block the closed timestamp side-transport from closing new
	// timestamps, meaning that the following call to GetCurrentReadSummary is
	// guaranteed to observe the highest closed timestamp ever published by this
	// range (if the merge eventually completes).
	if err := cArgs.EvalCtx.WatchForMerge(ctx); err != nil {
		return result.Result{}, errors.Wrap(err, "watching for merge during subsume")
	}

	// Now that the range is frozen, collect some information to ship to the LHS
	// leaseholder through the merge trigger.
	reply.MVCCStats = cArgs.EvalCtx.GetMVCCStats()
	reply.LeaseAppliedIndex = cArgs.EvalCtx.GetLeaseAppliedIndex()
	reply.FreezeStart = cArgs.EvalCtx.Clock().NowAsClockTimestamp()

	// We ship the range ID-local replicated stats as well, since these must be
	// subtracted from MVCCStats for the merged range.
	//
	// NB: lease requests can race with this computation, since they ignore
	// latches and write to the range ID-local keyspace. This can very rarely
	// result in a minor SysBytes discrepancy when the GetMVCCStats() call above
	// is not consistent with this readWriter snapshot. We accept this for now,
	// rather than introducing additional synchronization complexity.
	ridPrefix := keys.MakeRangeIDReplicatedPrefix(desc.RangeID)
	reply.RangeIDLocalMVCCStats, err = storage.ComputeStats(
		ctx, readWriter, ridPrefix, ridPrefix.PrefixEnd(), 0 /* nowNanos */)
	if err != nil {
		return result.Result{}, err
	}

	// Collect a read summary from the RHS leaseholder to ship to the LHS
	// leaseholder. This is used to instruct the LHS on how to update its
	// timestamp cache to ensure that no future writes are allowed to invalidate
	// prior reads performed to this point on the RHS range.
	priorReadSum := cArgs.EvalCtx.GetCurrentReadSummary(ctx)
	reply.ReadSummary = &priorReadSum
	reply.ClosedTimestamp = cArgs.EvalCtx.GetCurrentClosedTimestamp(ctx)

	var pd result.Result
	// Set DoTimelyApplicationToAllReplicas so that merges are applied on all
	// replicas. This is needed since Replica.AdminMerge calls
	// waitForApplication when sending a kvpb.SubsumeRequest.
	if cArgs.EvalCtx.ClusterSettings().Version.IsActive(ctx, clusterversion.V25_1_AddRangeForceFlushKey) {
		pd.Replicated.DoTimelyApplicationToAllReplicas = true
	}
	return pd, nil
}
