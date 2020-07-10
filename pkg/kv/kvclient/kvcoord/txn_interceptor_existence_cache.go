// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/cache"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
)

const existenceCacheSize = 64

var existenceCacheEnabled = settings.RegisterBoolSetting(
	"kv.transaction.existence_cache_enabled",
	"if enabled, key range existence knowledge is maintained for each transaction",
	true,
)

// existenceCacheHitResponse is a response that can provided by the
// txnExistenceCache without needing to send an RPC if it contains
// sufficient local information.
var existenceCacheHitResponse = &roachpb.CheckExistsResponse{Exists: true}

// txnExistenceCache is a txnInterceptor that caches key existence knowledge and
// uses this cached information to attempt to answer CheckExistsRequests queries
// locally. The cache maintains an interval tree of key ranges that are known
// to contain at-least one key.
type txnExistenceCache struct {
	st      *cluster.Settings
	wrapped lockedSender

	existenceKnowledge *cache.IntervalCache
}

func makeTxnExistenceCache(st *cluster.Settings) txnExistenceCache {
	ec := txnExistenceCache{st: st}
	ec.existenceKnowledge = cache.NewIntervalCache(cache.Config{
		Policy: cache.CacheLRU,
		ShouldEvict: func(n int, k, v interface{}) bool {
			return n > existenceCacheSize
		},
	})
	return ec
}

// SendLocked implements the lockedSender interface.
func (ec *txnExistenceCache) SendLocked(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, *roachpb.Error) {
	// Fast-path for 1PC transactions.
	if ba.IsCompleteTransaction() {
		return ec.wrapped.SendLocked(ctx, ba)
	}

	// Turn on and off.
	if !existenceCacheEnabled.Get(&ec.st.SV) {
		return ec.wrapped.SendLocked(ctx, ba)
	}

	// Determine whether any existence checks can be fielded locally.
	ba, cacheHits := ec.consultExistenceKnowledge(ba)

	// If the batch is now empty, we can short-circuit the entire RPC.
	if len(ba.Requests) == 0 {
		br := &roachpb.BatchResponse{}
		br.Txn = ba.Txn
		br.Responses = make([]roachpb.ResponseUnion, len(ba.Requests))
		for i := range br.Responses {
			br.Responses[i].MustSetInner(existenceCacheHitResponse)
		}
		return br, nil
	}

	// Send through wrapped lockedSender. Unlocks while sending then re-locks.
	br, pErr := ec.wrapped.SendLocked(ctx, ba)
	if pErr != nil {
		return nil, ec.adjustError(ctx, cacheHits, pErr)
	}

	// Resolve any outstanding writes that we proved to exist.
	ec.augmentExistenceKnowledge(ba, br)
	ec.addCacheHitsToResp(br, cacheHits)
	return br, nil
}

func (ec *txnExistenceCache) consultExistenceKnowledge(
	ba roachpb.BatchRequest,
) (roachpb.BatchRequest, []int) {
	if ec.existenceKnowledge.Len() == 0 {
		return ba, nil
	}

	forked := false
	oldReqs := ba.Requests
	var cacheHits []int
reqLoop:
	for i, ru := range oldReqs {
		req := ru.GetInner()
		switch req.Method() {
		case roachpb.CheckExists:
			r := req.Header().Span().AsRange()
			overlaps := ec.existenceKnowledge.GetOverlaps(r.Start, r.End)
			for _, o := range overlaps {
				if rangeContains(r, o.Range()) {
					if !forked {
						ba.Requests = ba.Requests[:i:i]
						forked = true
					}
					cacheHits = append(cacheHits, i)
					continue reqLoop
				}
			}
		case roachpb.Delete, roachpb.DeleteRange, roachpb.ClearRange:
			r := req.Header().Span().AsRange()
			overlaps := ec.existenceKnowledge.GetOverlaps(r.Start, r.End)
			for _, o := range overlaps {
				ec.existenceKnowledge.DelEntry(o)
			}
		}

		// If the BatchRequest's slice of requests has been forked from the original,
		// append the request to the new slice.
		if forked {
			ba.Add(req)
		}
	}
	return ba, cacheHits
}

// TODO(nvanbenschoten): Only do this for small batches where it's likely that
// we'll actually need to query it.
func (ec *txnExistenceCache) augmentExistenceKnowledge(
	ba roachpb.BatchRequest, br *roachpb.BatchResponse,
) {
	if len(ba.Requests) > existenceCacheSize {
		// If the batch contains more responses then the total cache can hold,
		// don't cache any of it. It's likely that this was a batch insertion
		// and that it will never need to be queried.
		return
	}
	for i, ru := range ba.Requests {
		req := ru.GetInner()
		method := req.Method()
		switch method {
		case roachpb.CheckExists:
			resp := br.Responses[i].GetInner().(*roachpb.CheckExistsResponse)
			if resp.Exists {
				ec.addIfNewKnowledge(req.Header().Span().AsRange())
			}
		case roachpb.Put, roachpb.ConditionalPut, roachpb.Increment, roachpb.InitPut:
			ec.addIfNewKnowledge(req.Header().Span().AsRange())
		case roachpb.Get:
			resp := br.Responses[i].GetInner().(*roachpb.GetResponse)
			if resp.Value.IsPresent() {
				ec.addIfNewKnowledge(req.Header().Span().AsRange())
			}
		// WIP: probably want to limit this to only add knowledge if the number
		// of rows is beneath some threshold. If it's above, there's very little
		// chance the knowledge will be queried.
		case roachpb.Scan:
			resp := br.Responses[i].GetInner().(*roachpb.ScanResponse)
			if len(resp.Rows) > 0 {
				ec.addIfNewKnowledge(req.Header().Span().AsRange())
			}
		case roachpb.ReverseScan:
			resp := br.Responses[i].GetInner().(*roachpb.ReverseScanResponse)
			if len(resp.Rows) > 0 {
				ec.addIfNewKnowledge(req.Header().Span().AsRange())
			}
		}
	}
}

func (ec *txnExistenceCache) addIfNewKnowledge(r interval.Range) {
	overlaps := ec.existenceKnowledge.GetOverlaps(r.Start, r.End)
	for _, o := range overlaps {
		or := o.Range()
		if rangeContains(r, or) {
			// Already have equal or more specialized knowledge.
			return
		}
		if rangeContains(or, r) {
			// If the new knowledge is more specialized, replace the old.
			ec.existenceKnowledge.DelEntry(o)
		}
	}
	key := ec.existenceKnowledge.NewKey(r.Start, r.End)
	ec.existenceKnowledge.Add(key, nil)
}

func rangeContains(out, in interval.Range) bool {
	return in.Start.Compare(out.Start) >= 0 && out.End.Compare(in.End) >= 0
}

func (ec *txnExistenceCache) addCacheHitsToResp(br *roachpb.BatchResponse, cacheHits []int) {
	hits := len(cacheHits)
	if hits == 0 {
		return
	}
	br.Responses = append(br.Responses, make([]roachpb.ResponseUnion, hits)...)
	copy(br.Responses[hits:], br.Responses[:len(br.Responses)-hits])
	for i := range br.Responses {
		if len(cacheHits) > 0 && cacheHits[0] == i {
			br.Responses[i].MustSetInner(existenceCacheHitResponse)
		} else {
			br.Responses[i].MustSetInner(br.Responses[hits].GetInner())
			hits++
		}
	}
}

// adjustError adjusts the provided error based on the request that caused it.
func (ec *txnExistenceCache) adjustError(
	ctx context.Context, cacheHits []int, pErr *roachpb.Error,
) *roachpb.Error {
	// Fix the error index to hide the impact of removed CheckExists requests.
	if pErr.Index != nil {
		for _, hit := range cacheHits {
			if pErr.Index.Index < int32(hit) {
				break
			}
			pErr.Index.Index++
		}

	}
	return pErr
}

// setWrapped implements the txnInterceptor interface.
func (ec *txnExistenceCache) setWrapped(wrapped lockedSender) { ec.wrapped = wrapped }

// populateLeafInputState is part of the txnInterceptor interface.
func (*txnExistenceCache) populateLeafInputState(*roachpb.LeafTxnInputState) {}

// populateLeafFinalState is part of the txnInterceptor interface.
func (*txnExistenceCache) populateLeafFinalState(*roachpb.LeafTxnFinalState) {}

// importLeafFinalState implements the txnInterceptor interface.
func (*txnExistenceCache) importLeafFinalState(context.Context, *roachpb.LeafTxnFinalState) {}

// epochBumpedLocked implements the txnInterceptor interface.
func (ec *txnExistenceCache) epochBumpedLocked() {
	// Clear out all existence knowledge. We'll have to re-learn
	// everything again in the next epoch.
	ec.existenceKnowledge.Clear()
}

// createSavepointLocked is part of the txnInterceptor interface.
func (*txnExistenceCache) createSavepointLocked(context.Context, *savepoint) {}

// rollbackToSavepointLocked is part of the txnInterceptor interface.
func (*txnExistenceCache) rollbackToSavepointLocked(context.Context, savepoint) {}

// closeLocked implements the txnInterceptor interface.
func (*txnExistenceCache) closeLocked() {}
