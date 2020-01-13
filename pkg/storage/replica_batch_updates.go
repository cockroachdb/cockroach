// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/pkg/errors"
)

// ----------------------------------------------------------------------------
// This files contains functions performing updates to a BatchRequest performed
// on the server-side, specifically after the request has been routed to a
// replica (and thus the request has been split based on range boundaries).
// As per the client.Sender contract, these function need to consider the input
// batches as copy-on-write.
// ----------------------------------------------------------------------------

// maybeStripInFlightWrites attempts to remove all point writes and query
// intents that ended up in the same batch as an EndTxn request from that EndTxn
// request's "in-flight" write set. The entire batch will commit atomically, so
// there is no need to consider the writes in the same batch concurrent.
//
// The transformation can lead to bypassing the STAGING state for a transaction
// entirely. This is possible if the function removes all of the in-flight
// writes from an EndTxn request that was committing in parallel with writes
// which all happened to be on the same range as the transaction record.
func maybeStripInFlightWrites(ba *roachpb.BatchRequest) (*roachpb.BatchRequest, error) {
	args, hasET := ba.GetArg(roachpb.EndTxn)
	if !hasET {
		return ba, nil
	}

	et := args.(*roachpb.EndTxnRequest)
	otherReqs := ba.Requests[:len(ba.Requests)-1]
	if !et.IsParallelCommit() || len(otherReqs) == 0 {
		return ba, nil
	}

	// Clone the BatchRequest and the EndTxn request before modifying it. We nil
	// out the request's in-flight writes and make the intent spans immutable on
	// append. Code below can use origET to recreate the in-flight write set if
	// any elements remain in it.
	origET := et
	et = origET.ShallowCopy().(*roachpb.EndTxnRequest)
	et.InFlightWrites = nil
	et.IntentSpans = et.IntentSpans[:len(et.IntentSpans):len(et.IntentSpans)] // immutable
	ba.Requests = append([]roachpb.RequestUnion(nil), ba.Requests...)
	ba.Requests[len(ba.Requests)-1].MustSetInner(et)

	// Fast-path: If we know that this batch contains all of the transaction's
	// in-flight writes, then we can avoid searching in the in-flight writes set
	// for each request. Instead, we can blindly merge all in-flight writes into
	// the intent spans and clear out the in-flight writes set.
	if len(otherReqs) >= len(origET.InFlightWrites) {
		writes := 0
		for _, ru := range otherReqs {
			req := ru.GetInner()
			switch {
			case roachpb.IsTransactionWrite(req) && !roachpb.IsRange(req):
				// Concurrent point write.
				writes++
			case req.Method() == roachpb.QueryIntent:
				// Earlier pipelined point write that hasn't been proven yet.
				writes++
			default:
				// Ranged write or read. See below.
			}
		}
		if len(origET.InFlightWrites) < writes {
			return ba, errors.New("more write in batch with EndTxn than listed in in-flight writes")
		} else if len(origET.InFlightWrites) == writes {
			et.IntentSpans = make([]roachpb.Span, len(origET.IntentSpans)+len(origET.InFlightWrites))
			copy(et.IntentSpans, origET.IntentSpans)
			for i, w := range origET.InFlightWrites {
				et.IntentSpans[len(origET.IntentSpans)+i] = roachpb.Span{Key: w.Key}
			}
			// See below for why we set Header.DistinctSpans here.
			et.IntentSpans, ba.Header.DistinctSpans = roachpb.MergeSpans(et.IntentSpans)
			return ba, nil
		}
	}

	// Slow-path: If not then we remove each transaction write in the batch from
	// the in-flight write set and merge it into the intent spans.
	copiedTo := 0
	for _, ru := range otherReqs {
		req := ru.GetInner()
		seq := req.Header().Sequence
		switch {
		case roachpb.IsTransactionWrite(req) && !roachpb.IsRange(req):
			// Concurrent point write.
		case req.Method() == roachpb.QueryIntent:
			// Earlier pipelined point write that hasn't been proven yet. We
			// could remove from the in-flight writes set when we see these,
			// but doing so would prevent us from using the optimization we
			// have below where we rely on increasing sequence numbers for
			// each subsequent request.
			//
			// We already don't intend on sending QueryIntent requests in the
			// same batch as EndTxn requests because doing so causes a pipeline
			// stall, so this doesn't seem worthwhile to support.
			continue
		default:
			// Ranged write or read. These can make it into the final batch with
			// a parallel committing EndTxn request if the entire batch issued
			// by DistSender lands on the same range. Skip.
			continue
		}

		// Remove the write from the in-flight writes set. We only need to
		// search from after the previously removed sequence number forward
		// because both the InFlightWrites and the Requests in the batch are
		// stored in increasing sequence order.
		//
		// Maintaining an iterator into the in-flight writes slice and scanning
		// instead of performing a binary search on each request changes the
		// complexity of this loop from O(n*log(m)) to O(m) where n is the
		// number of point writes in the batch and m is the number of in-flight
		// writes. These complexities aren't directly comparable, but copying
		// all unstripped writes back into et.InFlightWrites is already O(m),
		// so the approach here was preferred over repeat binary searches.
		match := -1
		for i, w := range origET.InFlightWrites[copiedTo:] {
			if w.Sequence == seq {
				match = i + copiedTo
				break
			}
		}
		if match == -1 {
			return ba, errors.New("write in batch with EndTxn missing from in-flight writes")
		}
		w := origET.InFlightWrites[match]
		notInBa := origET.InFlightWrites[copiedTo:match]
		et.InFlightWrites = append(et.InFlightWrites, notInBa...)
		copiedTo = match + 1

		// Move the write to the intent spans set since it's
		// no longer being tracked in the in-flight write set.
		et.IntentSpans = append(et.IntentSpans, roachpb.Span{Key: w.Key})
	}
	if et != origET {
		// Finish building up the remaining in-flight writes.
		notInBa := origET.InFlightWrites[copiedTo:]
		et.InFlightWrites = append(et.InFlightWrites, notInBa...)
		// Re-sort and merge the intent spans. We can set the batch request's
		// DistinctSpans flag based on whether any of in-flight writes in this
		// batch overlap with each other. This will have (rare) false negatives
		// when the in-flight writes overlap with existing intent spans, but
		// never false positives.
		et.IntentSpans, ba.Header.DistinctSpans = roachpb.MergeSpans(et.IntentSpans)
	}
	return ba, nil
}
