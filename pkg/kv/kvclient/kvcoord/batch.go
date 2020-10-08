// Copyright 2015 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

var emptySpan = roachpb.Span{}

// truncate restricts all contained requests to the given key range and returns
// a new, truncated, BatchRequest. All requests contained in that batch are
// "truncated" to the given span, and requests which are found to not overlap
// the given span at all are removed. A mapping of response index to batch index
// is returned. For example, if
//
// ba = Put[a], Put[c], Put[b],
// rs = [a,bb],
//
// then truncate(ba,rs) returns a batch (Put[a], Put[b]) and positions [0,2].
func truncate(ba roachpb.BatchRequest, rs roachpb.RSpan) (roachpb.BatchRequest, []int, error) {
	truncateOne := func(args roachpb.Request) (bool, roachpb.Span, error) {
		header := args.Header().Span()
		if !roachpb.IsRange(args) {
			// This is a point request.
			if len(header.EndKey) > 0 {
				return false, emptySpan, errors.Errorf("%T is not a range command, but EndKey is set", args)
			}
			keyAddr, err := keys.Addr(header.Key)
			if err != nil {
				return false, emptySpan, err
			}
			if !rs.ContainsKey(keyAddr) {
				return false, emptySpan, nil
			}
			return true, header, nil
		}
		// We're dealing with a range-spanning request.
		local := false
		keyAddr, err := keys.Addr(header.Key)
		if err != nil {
			return false, emptySpan, err
		}
		endKeyAddr, err := keys.Addr(header.EndKey)
		if err != nil {
			return false, emptySpan, err
		}
		if l, r := keys.IsLocal(header.Key), keys.IsLocal(header.EndKey); l || r {
			if !l || !r {
				return false, emptySpan, errors.Errorf("local key mixed with global key in range")
			}
			local = true
		}
		if keyAddr.Less(rs.Key) {
			// rs.Key can't be local because it contains range split points, which
			// are never local.
			if !local {
				header.Key = rs.Key.AsRawKey()
			} else {
				// The local start key should be truncated to the boundary of local keys which
				// address to rs.Key.
				header.Key = keys.MakeRangeKeyPrefix(rs.Key)
			}
		}
		if !endKeyAddr.Less(rs.EndKey) {
			// rs.EndKey can't be local because it contains range split points, which
			// are never local.
			if !local {
				header.EndKey = rs.EndKey.AsRawKey()
			} else {
				// The local end key should be truncated to the boundary of local keys which
				// address to rs.EndKey.
				header.EndKey = keys.MakeRangeKeyPrefix(rs.EndKey)
			}
		}
		// Check whether the truncation has left any keys in the range. If not,
		// we need to cut it out of the request.
		if header.Key.Compare(header.EndKey) >= 0 {
			return false, emptySpan, nil
		}
		return true, header, nil
	}

	// TODO(tschottdorf): optimize so that we don't always make a new request
	// slice, only when something changed (copy-on-write).

	var positions []int
	truncBA := ba
	truncBA.Requests = nil
	for pos, arg := range ba.Requests {
		hasRequest, newSpan, err := truncateOne(arg.GetInner())
		if hasRequest {
			// Keep the old one. If we must adjust the header, must copy.
			inner := ba.Requests[pos].GetInner()
			oldHeader := inner.Header()
			if newSpan.EqualValue(oldHeader.Span()) {
				truncBA.Requests = append(truncBA.Requests, ba.Requests[pos])
			} else {
				oldHeader.SetSpan(newSpan)
				shallowCopy := inner.ShallowCopy()
				shallowCopy.SetHeader(oldHeader)
				truncBA.Requests = append(truncBA.Requests, roachpb.RequestUnion{})
				truncBA.Requests[len(truncBA.Requests)-1].MustSetInner(shallowCopy)
			}
			positions = append(positions, pos)
		}
		if err != nil {
			return roachpb.BatchRequest{}, nil, err
		}
	}
	return truncBA, positions, nil
}

// prev gives the right boundary of the union of all requests which don't
// affect keys larger than the given key. Note that a right boundary is
// exclusive, that is, the returned RKey is to be used as the exclusive
// right endpoint in finding the next range to query.
//
// Informally, a call `prev(ba, k)` means: we've already executed the parts
// of `ba` that intersect `[k, KeyMax)`; please tell me how far to the
// left the next relevant request begins.
//
// TODO(tschottdorf): again, better on BatchRequest itself, but can't pull
// 'keys' into 'roachpb'.
func prev(ba roachpb.BatchRequest, k roachpb.RKey) (roachpb.RKey, error) {
	candidate := roachpb.RKeyMin
	for _, union := range ba.Requests {
		inner := union.GetInner()
		h := inner.Header()
		addr, err := keys.Addr(h.Key)
		if err != nil {
			return nil, err
		}
		endKey := h.EndKey
		if len(endKey) == 0 {
			// This is unintuitive, but if we have a point request at `x=k` then that request has
			// already been satisfied (since the batch has already been executed for all keys `>=
			// k`). We treat `k` as `[k,k)` which does the right thing below. It also does when `x >
			// k` and `x < k`, so we're good.
			//
			// Note that if `x` is /Local/k/something, then AddrUpperBound below will turn it into
			// `k\x00`, and so we're looking at the key range `[k, k\x00)`. This is exactly what we
			// want since otherwise the result would be `k` and so the caller would restrict itself
			// to `key < k`, but that excludes `k` itself and thus all local keys attached to it.
			//
			// See TestBatchPrevNext for a test case with commentary.
			endKey = h.Key
		}
		eAddr, err := keys.AddrUpperBound(endKey)
		if err != nil {
			return nil, err
		}
		if !eAddr.Less(k) {
			// EndKey is k or higher.
			//           [x-------y)    !x.Less(k) -> skip
			//         [x-------y)      !x.Less(k) -> skip
			//      [x-------y)          x.Less(k) -> return k
			//  [x------y)               x.Less(k) -> return k
			// [x------y)                not in this branch
			//          k
			if addr.Less(k) {
				// Range contains k, so won't be able to go lower.
				// Note that in the special case in which the interval
				// touches k, we don't take this branch. This reflects
				// the fact that `prev(k)` means that all keys >= k have
				// been handled, so a request `[k, x)` should simply be
				// skipped.
				return k, nil
			}
			// Range is disjoint from [KeyMin,k).
			continue
		}
		// Current candidate interval is strictly to the left of `k`.
		// We want the largest surviving candidate.
		if candidate.Less(eAddr) {
			candidate = eAddr
		}
	}
	return candidate, nil
}

// next gives the left boundary of the union of all requests which don't affect
// keys less than the given key. Note that the left boundary is inclusive, that
// is, the returned RKey is the inclusive left endpoint of the keys the request
// should operate on next.
//
// Informally, a call `next(ba, k)` means: we've already executed the parts of
// `ba` that intersect `[KeyMin, k)`; please tell me how far to the right the
// next relevant request begins.
//
// TODO(tschottdorf): again, better on BatchRequest itself, but can't pull
// 'keys' into 'proto'.
func next(ba roachpb.BatchRequest, k roachpb.RKey) (roachpb.RKey, error) {
	candidate := roachpb.RKeyMax
	for _, union := range ba.Requests {
		inner := union.GetInner()
		h := inner.Header()
		addr, err := keys.Addr(h.Key)
		if err != nil {
			return nil, err
		}
		if addr.Less(k) {
			if len(h.EndKey) == 0 {
				// `h` affects only `[KeyMin,k)`, all of which is less than `k`.
				continue
			}
			eAddr, err := keys.AddrUpperBound(h.EndKey)
			if err != nil {
				return nil, err
			}
			if k.Less(eAddr) {
				// Starts below k, but continues beyond. Need to stay at k.
				return k, nil
			}
			// `h` affects only `[KeyMin,k)`, all of which is less than `k`.
			continue
		}
		// We want the smallest of the surviving candidates.
		if addr.Less(candidate) {
			candidate = addr
		}
	}
	return candidate, nil
}
