// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import "github.com/cockroachdb/cockroach/pkg/storage/enginepb"

// splitStatsHelper codifies and explains the stats computations related to a
// split. The quantities known during a split (i.e. while the split trigger
// is evaluating) are
//
//   - AbsPreSplitBothStored: the stats of the range before the split trigger,
//     i.e. without accounting for any writes in the batch. This can have
//     ContainsEstimates set.
//   - DeltaBatchEstimated: the writes in the batch, i.e. the stats delta accrued
//     from the evaluation of the EndTxn so far (this is mostly the write to the
//     transaction record, as well as resolving the intent on the range descriptor,
//     but nothing in this code relies on that). Since we have no reason to
//     introduce ContainsEstimates in a split trigger, this typically has
//     ContainsEstimates unset, but the results will be estimate free either way.
//   - AbsPostSplit{Left,Right}: the stats of either the left or right hand side
//     range after applying the split, i.e. accounting both for the shrinking as
//     well as for the writes in DeltaBatch related to the shrunk keyrange. In
//     practice, we obtain this by recomputing the stats using the corresponding
//     AbsPostSplit{Left,Right}Fn, and so we don't expect ContainsEstimates to be
//     set in them. The choice of which side to scan is controlled by ScanRightFirst.
//   - DeltaRangeKey: the stats delta that must be added to the non-computed
//     half's stats to account for the splitting of range keys straddling the split
//     point. See computeSplitRangeKeyStatsDelta() for details.
//
// # We are interested in computing from this the quantities
//
//   - AbsPostSplitRight(): the stats of the right hand side created by the split,
//     i.e. the data taken over from the left hand side plus whatever was written to
//     the right hand side in the process (metadata etc). We can recompute this, but
//     try to avoid it unless necessary (when CombinedErrorDelta below is nonzero).
//   - DeltaPostSplitLeft(): the stats delta that should be emitted by the split
//     trigger itself, i.e. the data which the left hand side (initially comprising
//     both halves) loses by moving data into the right hand side (including whatever
//     DeltaBatch contained in contributions attributable to the keyspace on the
//     left).
//   - CombinedErrorDelta: the difference between (AbsPreSplitBoth+DeltaBatch) and
//     the recomputation of the pre-split range including the batch. This is zero if
//     neither of the inputs contains estimates. If it's not zero, we need to
//     recompute from scratch to obtain AbsPostSplitRight. What's interesting about
//     this quantity is that we never care what exactly it is, but we do care
//     whether it's zero or not because if it's zero we get to do less work.
//
// Moreover, we want both neither of AbsPostSplit{Right,Left} to end up with
// estimates. The way splits are set up right now, we sort of get this "for
// free" for the left hand side (since we recompute that unconditionally; there
// is a guarantee that the left hand side is never too large). We also don't
// want to create new ranges that start out with estimates (just to prevent the
// unbounded proliferation of estimates).
//
// The two unknown quantities can be expressed in terms of the known quantities
// because
//
// (1) AbsPreSplitBoth + DeltaBatch + DeltaRangeKey
//   - CombinedErrorDelta = AbsPostSplitLeft + AbsPostSplitRight
//
// In words, this corresponds to "all bytes are accounted for": from the initial
// stats that we have (accounting for the fact that AbsPreSplitBoth+DeltaBatch
// may contain estimates), everything we add/remove during the split ends up
// tracked either on the left and on the right, and nothing is created out of
// thin air.
//
// (2) AbsPreSplitBoth + DeltaPostSplitLeft() = AbsPostSplitLeft
//
// This expresses the fact that is always true whenever a command applies on a
// range without introducing an estimate: the stats before the command plus the
// delta emitted by the command equal the stats after the command. In this case,
// the stats before the command are that of the range before the split (remember
// that the split shrinks the range towards the start key, i.e. the left hand
// side is the same range as the pre-split one).
//
// These two equations are easily solved for the unknowns. First, we can express
// DeltaPostSplitLeft() in known quantities via (2) as
//
//	DeltaPostSplitLeft() = AbsPostSplitLeft - AbsPreSplitBothStored.
//
// Note that if we start out with estimates, DeltaPostSplitLeft() will wipe out
// those estimates when added to the absolute stats.
//
// For AbsPostSplitRight(), there are two cases. First, due to the identity
//
//	CombinedErrorDelta = AbsPreSplitBothStored + DeltaBatchEstimated
//	                     -(AbsPostSplitLeft + AbsPostSplitRight)
//	                     + DeltaRangeKey.
//
// and the fact that the second and third lines contain no estimates, we know
// that CombinedErrorDelta is zero if the first line contains no estimates.
// Using this, we can rearrange as
//
//	AbsPostSplitRight() = AbsPreSplitBoth + DeltaBatch - AbsPostSplitLeft
//	                      + DeltaRangeKey.
//
// where all quantities on the right are known. If CombinedErrorDelta is
// nonzero, we effectively have one more unknown in our linear system and we
// need to recompute AbsPostSplitRight from scratch. (As fallout, we can in
// principle compute CombinedError, but we don't care).
type splitStatsHelper struct {
	in splitStatsHelperInput

	absPostSplitLeft  *enginepb.MVCCStats
	absPostSplitRight *enginepb.MVCCStats
}

// splitStatsScanFn scans a post-split keyspace to compute its stats. The
// computed stats should not contain estimates.
type splitStatsScanFn func() (enginepb.MVCCStats, error)

// splitStatsHelperInput is passed to makeSplitStatsHelper.
type splitStatsHelperInput struct {
	AbsPreSplitBothStored enginepb.MVCCStats
	DeltaBatchEstimated   enginepb.MVCCStats
	DeltaRangeKey         enginepb.MVCCStats
	// PostSplitScanLeftFn returns the stats for the left hand side of the
	// split computed by scanning relevant part of the range.
	PostSplitScanLeftFn splitStatsScanFn
	// PostSplitScanRightFn returns the stats for the right hand side of the
	// split computed by scanning relevant part of the range.
	PostSplitScanRightFn splitStatsScanFn
	// ScanRightFirst controls whether the left hand side or the right hand
	// side of the split is scanned first. In cases where neither of the
	// input stats contain estimates, this is the only side that needs to
	// be scanned.
	ScanRightFirst bool
	// LeftIsEmpty denotes that the LHS is empty. If this is the case, the entire
	// LHS will be scanned to compute stats accurately. This is cheap because
	// the range is empty.
	LeftIsEmpty bool
	// RightIsEmpty denotes that the RHS is empty. If this is the case, the entire
	// RHS will be scanned to compute stats accurately. This is cheap because
	// the range is empty.
	RightIsEmpty bool
	// PreSplitLeftUser contains the pre-split user-only stats of the LHS. Those
	// are expensive to compute, so we compute them in AdminSplit, before holding
	// latches, and pass them to splitTrigger.
	PreSplitLeftUser enginepb.MVCCStats
	// PostSplitScanLocalLeftFn returns the stats for the non-user spans of the
	// LHS. Adding these to PreSplitLeftUser results in an estimate of the LHS
	// stats. It's only an estimate because any writes to the user spans of the
	// LHS, concurrent with the split, may not be accounted for.
	PostSplitScanLocalLeftFn splitStatsScanFn
}

// makeSplitStatsHelper initializes a splitStatsHelper. The values in the input
// are assumed to not change outside of the helper and must no longer be used.
// The provided PostSplitScanLeftFn and PostSplitScanRightFn recompute the left
// and right hand sides of the split after accounting for the split trigger
// batch. Each are only invoked at most once, and only when necessary.
func makeSplitStatsHelper(input splitStatsHelperInput) (splitStatsHelper, error) {
	h := splitStatsHelper{
		in: input,
	}

	// Scan to compute the stats for the first side.
	var absPostSplitFirst enginepb.MVCCStats
	var err error
	if h.in.ScanRightFirst {
		absPostSplitFirst, err = input.PostSplitScanRightFn()
		h.absPostSplitRight = &absPostSplitFirst
	} else {
		absPostSplitFirst, err = input.PostSplitScanLeftFn()
		h.absPostSplitLeft = &absPostSplitFirst
	}
	if err != nil {
		return splitStatsHelper{}, err
	}

	if h.in.AbsPreSplitBothStored.ContainsEstimates == 0 &&
		h.in.DeltaBatchEstimated.ContainsEstimates == 0 {
		// We have CombinedErrorDelta zero, so use arithmetic to compute the
		// stats for the second side.
		ms := h.in.AbsPreSplitBothStored
		ms.Subtract(absPostSplitFirst)
		ms.Add(h.in.DeltaBatchEstimated)
		ms.Add(h.in.DeltaRangeKey)
		if h.in.ScanRightFirst {
			h.absPostSplitLeft = &ms
		} else {
			h.absPostSplitRight = &ms
		}
		return h, nil
	}

	// Estimates are contained in the input, so ask the oracle to scan to compute
	// the stats for the second side. We only scan the second side when either of
	// the input stats above (AbsPreSplitBothStored or DeltaBatchEstimated)
	// contains estimates, so that we can guarantee that the post-splits stats
	// don't.
	var absPostSplitSecond enginepb.MVCCStats
	if h.in.ScanRightFirst {
		absPostSplitSecond, err = input.PostSplitScanLeftFn()
		h.absPostSplitLeft = &absPostSplitSecond
	} else {
		absPostSplitSecond, err = input.PostSplitScanRightFn()
		h.absPostSplitRight = &absPostSplitSecond
	}
	if err != nil {
		return splitStatsHelper{}, err
	}
	return h, nil
}

// makeEstimatedSplitStatsHelper is like makeSplitStatsHelper except it does
// not scan the entire LHS or RHS by calling PostSplitScan(Left|Right)Fn().
// Instead, this function derives the post-split LHS stats by adding
// the pre-split user LHS stats to the non-user LHS stats computed here by
// calling PostSplitScanLocalLeftFn(). The resulting stats are not guaranteed
// to be 100% accurate, so they are marked with ContainsEstimates. However,
// if there are no writes concurrent with the split (i.e. the pre-computed
// PreSplitLeftUser  still correspond to the correct LHS user stats), then the
// stats are guaranteed to be correct.
// INVARIANT 1: Non-user stats (SysBytes, SysCount, etc.) are always correct.
// This is because we scan those spans while holding latches in this function.
// INVARIANT 2: If there are no writes concurrent with the split, the stats are
// always correct.
func makeEstimatedSplitStatsHelper(input splitStatsHelperInput) (splitStatsHelper, error) {
	h := splitStatsHelper{
		in: input,
	}

	// Fast path: if both ranges are guaranteed to be empty, just scan them and
	// return early. They do not contain estimates.
	if h.in.LeftIsEmpty && h.in.RightIsEmpty {
		leftStats, err := input.PostSplitScanLeftFn()
		if err != nil {
			return splitStatsHelper{}, err
		}
		h.absPostSplitLeft = &leftStats
		rightStats, err := input.PostSplitScanRightFn()
		if err != nil {
			return splitStatsHelper{}, err
		}
		h.absPostSplitRight = &rightStats
		return h, nil
	}

	var absPostSplitFirst enginepb.MVCCStats
	var err error
	// If either the RHS or LHS is empty, scan it (it's cheap).
	if h.in.RightIsEmpty {
		absPostSplitFirst, err = input.PostSplitScanRightFn()
		h.absPostSplitRight = &absPostSplitFirst
	} else if h.in.LeftIsEmpty {
		absPostSplitFirst, err = input.PostSplitScanLeftFn()
		h.absPostSplitLeft = &absPostSplitFirst
	} else {
		// Otherwise, compute the LHS stats by adding the user-only LHS stats passed
		// into the split trigger and the non-user LHS stats computed by calling
		// PostSplitScanLocalLeftFn(). The local key space is very small, so this is
		// not expensive. Scanning the local key space ensures that any writes to
		// local keys during the split (e.g. to range descriptors and transaction
		// records) are accounted for.
		absPostSplitFirst, err = input.PostSplitScanLocalLeftFn()
		absPostSplitFirst.Add(input.PreSplitLeftUser)
		h.absPostSplitLeft = &absPostSplitFirst
	}
	if err != nil {
		return splitStatsHelper{}, err
	}

	// For the second side of the split, the computation is identical to that in
	// makeSplitStatsHelper: subtract the first side's stats from the total, and
	// adjust DeltaBatchEstimated and DeltaRangeKey.
	ms := h.in.AbsPreSplitBothStored
	ms.Subtract(absPostSplitFirst)
	ms.Add(h.in.DeltaBatchEstimated)
	ms.Add(h.in.DeltaRangeKey)
	if h.in.RightIsEmpty {
		h.absPostSplitLeft = &ms
	} else {
		h.absPostSplitRight = &ms
	}

	// Mark the new stats with ContainsEstimates only if the corresponding range
	// is not empty.
	if !h.in.LeftIsEmpty {
		h.absPostSplitLeft.ContainsEstimates++
	}
	if !h.in.RightIsEmpty {
		h.absPostSplitRight.ContainsEstimates++
	}

	return h, nil
}

// AbsPostSplitRight returns the stats of the right hand side created by the
// split. The result is returned as a pointer because the caller can freely
// modify it, assuming they're adding only stats corresponding to mutations that
// they know only affect the right hand side. (If estimates are introduced in
// the process, the right hand side will start out with estimates). Implicitly
// this changes the DeltaBatchEstimated supplied to makeSplitStatsHelper, but
// the contract assumes that that value will no longer be used.
func (h splitStatsHelper) AbsPostSplitRight() *enginepb.MVCCStats {
	return h.absPostSplitRight
}

// DeltaPostSplitLeft return the stats delta to be emitted on the left hand side
// as the result of the split. It accounts for the data moved to the right hand
// side as well as any mutations to the left hand side carried out during the
// split, and additionally removes any estimates present in the pre-split stats.
func (h splitStatsHelper) DeltaPostSplitLeft() enginepb.MVCCStats {
	// NB: if we ever wanted to also write to the left hand side after init'ing
	// the helper, we can make that work, too.
	// NB: note how none of this depends on mutations to absPostSplitRight.
	ms := *h.absPostSplitLeft
	ms.Subtract(h.in.AbsPreSplitBothStored)

	return ms
}
