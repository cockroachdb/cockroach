// Copyright 2018 The Cockroach Authors.
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
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(roachpb.Subsume, declareKeysSubsume, Subsume)
}

func declareKeysSubsume(
	_ *roachpb.RangeDescriptor,
	header roachpb.Header,
	req roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
) {
	// Subsume must not run concurrently with any other command. It declares a
	// non-MVCC write over every addressable key in the range; this guarantees
	// that it conflicts with any other command because every command must declare
	// at least one addressable key. It does not, in fact, write any keys.
	//
	// We use the key bounds from the range descriptor in the request instead
	// of the current range descriptor. Either would be fine because we verify
	// that these match during the evaluation of the Subsume request.
	args := req.(*roachpb.SubsumeRequest)
	desc := args.RightDesc
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
		Key:    desc.StartKey.AsRawKey(),
		EndKey: desc.EndKey.AsRawKey(),
	})
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
		Key:    keys.MakeRangeKeyPrefix(desc.StartKey),
		EndKey: keys.MakeRangeKeyPrefix(desc.EndKey).PrefixEnd(),
	})
	rangeIDPrefix := keys.MakeRangeIDReplicatedPrefix(desc.RangeID)
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{
		Key:    rangeIDPrefix,
		EndKey: rangeIDPrefix.PrefixEnd(),
	})
}

// Subsume freezes a range for merging with its left-hand neighbor. When called
// correctly, it provides important guarantees that ensure there is no moment in
// time where the ranges involved in the merge could both process commands for
// the same keys.
//
// Specifically, the receiving replica guarantees that:
//
//   1. it is the leaseholder at the time the request executes,
//   2. when it responds, there are no commands in flight with a timestamp
//      greater than the FreezeStart timestamp provided in the response,
//   3. the MVCC statistics in the response reflect the latest writes,
//   4. it, and all future leaseholders for the range, will not process another
//      command until they refresh their range descriptor with a consistent read
//      from meta2, and
//   5. if it or any future leaseholder for the range finds that its range
//      descriptor has been deleted, it self destructs.
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
func Subsume(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.SubsumeRequest)
	reply := resp.(*roachpb.SubsumeResponse)

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
	// a deletion intent on the local range descriptor.
	descKey := keys.RangeDescriptorKey(desc.StartKey)
	_, intent, err := storage.MVCCGet(ctx, readWriter, descKey, cArgs.Header.Timestamp,
		storage.MVCCGetOptions{Inconsistent: true})
	if err != nil {
		return result.Result{}, errors.Errorf("fetching local range descriptor: %s", err)
	} else if intent == nil {
		return result.Result{}, errors.New("range missing intent on its local descriptor")
	}
	val, _, err := storage.MVCCGetAsTxn(ctx, readWriter, descKey, cArgs.Header.Timestamp, intent.Txn)
	if err != nil {
		return result.Result{}, errors.Errorf("fetching local range descriptor as txn: %s", err)
	} else if val != nil {
		return result.Result{}, errors.New("non-deletion intent on local range descriptor")
	}

	// NOTE: the deletion intent on the range's meta2 descriptor is just as
	// important to correctness as the deletion intent on the local descriptor,
	// but the check is too expensive as it would involve a network roundtrip on
	// most nodes.

	reply.MVCCStats = cArgs.EvalCtx.GetMVCCStats()
	reply.LeaseAppliedIndex = cArgs.EvalCtx.GetLeaseAppliedIndex()
	reply.FreezeStart = cArgs.EvalCtx.Clock().Now()

	return result.Result{
		Local: result.LocalResult{MaybeWatchForMerge: true},
	}, nil
}
