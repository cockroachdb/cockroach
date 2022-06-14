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

// BatchTruncationHelper is a utility struct that helps with truncating requests
// to range boundaries as well as figuring out the next key to seek to for the
// range iterator.
//
// The caller should not use the helper if all requests fit within a single
// range since the helper has non-trivial setup cost.
//
// It is designed to be used roughly as follows:
//
//   rs := keys.Range(requests)
//   ri.Seek(scanDir, rs.Key)
//   if !ri.NeedAnother(rs) {
//     // All requests fit within a single range, don't use the helper.
//     ...
//   }
//   helper := MakeBatchTruncationHelper(scanDir, requests)
//   for ri.Valid() {
//     curRangeRS := rs.Intersect(ri.Token().Desc())
//     curRangeReqs, positions, seekKey := helper.Truncate(curRangeRS)
//     // Process curRangeReqs that touch a single range and then use positions
//     // to reassemble the result.
//     ...
//     ri.Seek(scanDir, seekKey)
//   }
//
type BatchTruncationHelper struct {
	scanDir  ScanDirection
	requests []roachpb.RequestUnion
	// mustPreserveOrder indicates whether the requests must be returned by
	// Truncate() in the original order.
	mustPreserveOrder bool
}

// MakeBatchTruncationHelper returns a new BatchTruncationHelper for the given
// requests.
//
// mustPreserveOrder, if true, indicates that the caller requires that requests
// are returned by Truncate() in the original order (i.e. with strictly
// increasing positions values).
func MakeBatchTruncationHelper(
	scanDir ScanDirection, requests []roachpb.RequestUnion, mustPreserveOrder bool,
) (BatchTruncationHelper, error) {
	var ret BatchTruncationHelper
	ret.scanDir = scanDir
	ret.requests = requests
	ret.mustPreserveOrder = mustPreserveOrder
	return ret, nil
}

// Truncate restricts all requests to the given key range and returns new,
// truncated, requests. All returned requests are "truncated" to the given span,
// and requests which are found to not overlap the given span at all are
// removed. A mapping of response index to request index is returned. It also
// returns the next seek key for the range iterator. With Ascending scan
// direction, the next seek key is such that requests in [RKeyMin, seekKey)
// range have been processed, with Descending scan direction, it is such that
// requests in [seekKey, RKeyMax) range have been processed.
//
// For example, if
//
//   reqs = Put[a], Put[c], Put[b],
//   rs = [a,bb],
//   BatchTruncationHelper.Init(Ascending, reqs)
//
// then BatchTruncationHelper.Truncate(rs) returns (Put[a], Put[b]), positions
// [0,2] as well as seekKey 'c'.
//
// Truncate returns the requests in an arbitrary order (meaning that positions
// return values might not be ascending), unless mustPreserveOrder was true in
// Init().
//
// NOTE: it is assumed that
// 1. Truncate has been called on the previous ranges that intersect with
//    keys.Range(reqs);
// 2. rs is intersected with the current range boundaries.
func (h *BatchTruncationHelper) Truncate(
	rs roachpb.RSpan,
) ([]roachpb.RequestUnion, []int, roachpb.RKey, error) {
	truncReqs, positions, err := truncateLegacy(h.requests, rs)
	if err != nil {
		return nil, nil, nil, err
	}
	var seekKey roachpb.RKey
	if h.scanDir == Ascending {
		// In next iteration, query next range.
		// It's important that we use the EndKey of the current descriptor
		// as opposed to the StartKey of the next one: if the former is stale,
		// it's possible that the next range has since merged the subsequent
		// one, and unless both descriptors are stale, the next descriptor's
		// StartKey would move us to the beginning of the current range,
		// resulting in a duplicate scan.
		seekKey, err = nextLegacy(h.requests, rs.EndKey)
	} else {
		// In next iteration, query previous range.
		// We use the StartKey of the current descriptor as opposed to the
		// EndKey of the previous one since that doesn't have bugs when
		// stale descriptors come into play.
		seekKey, err = prevLegacy(h.requests, rs.Key)
	}
	return truncReqs, positions, seekKey, err
}

var emptyHeader = roachpb.RequestHeader{}

// truncateLegacy restricts all requests to the given key range and returns new,
// truncated, requests. All returned requests are "truncated" to the given span,
// and requests which are found to not overlap the given span at all are
// removed. A mapping of response index to request index is returned. For
// example, if
//
// reqs = Put[a], Put[c], Put[b],
// rs = [a,bb],
//
// then truncateLegacy(reqs,rs) returns (Put[a], Put[b]) and positions [0,2].
func truncateLegacy(
	reqs []roachpb.RequestUnion, rs roachpb.RSpan,
) ([]roachpb.RequestUnion, []int, error) {
	truncateOne := func(args roachpb.Request) (hasRequest bool, changed bool, _ roachpb.RequestHeader, _ error) {
		header := args.Header()
		if !roachpb.IsRange(args) {
			// This is a point request.
			if len(header.EndKey) > 0 {
				return false, false, emptyHeader, errors.Errorf("%T is not a range command, but EndKey is set", args)
			}
			keyAddr, err := keys.Addr(header.Key)
			if err != nil {
				return false, false, emptyHeader, err
			}
			if !rs.ContainsKey(keyAddr) {
				return false, false, emptyHeader, nil
			}
			return true, false, header, nil
		}
		// We're dealing with a range-spanning request.
		local := false
		keyAddr, err := keys.Addr(header.Key)
		if err != nil {
			return false, false, emptyHeader, err
		}
		endKeyAddr, err := keys.Addr(header.EndKey)
		if err != nil {
			return false, false, emptyHeader, err
		}
		if l, r := keys.IsLocal(header.Key), keys.IsLocal(header.EndKey); l || r {
			if !l || !r {
				return false, false, emptyHeader, errors.Errorf("local key mixed with global key in range")
			}
			local = true
		}
		if keyAddr.Less(rs.Key) {
			// rs.Key can't be local because it contains range split points,
			// which are never local.
			changed = true
			if !local {
				header.Key = rs.Key.AsRawKey()
			} else {
				// The local start key should be truncated to the boundary of
				// local keys which address to rs.Key.
				header.Key = keys.MakeRangeKeyPrefix(rs.Key)
			}
		}
		if !endKeyAddr.Less(rs.EndKey) {
			// rs.EndKey can't be local because it contains range split points,
			// which are never local.
			changed = true
			if !local {
				header.EndKey = rs.EndKey.AsRawKey()
			} else {
				// The local end key should be truncated to the boundary of
				// local keys which address to rs.EndKey.
				header.EndKey = keys.MakeRangeKeyPrefix(rs.EndKey)
			}
		}
		// Check whether the truncation has left any keys in the range. If not,
		// we need to cut it out of the request.
		if changed && header.Key.Compare(header.EndKey) >= 0 {
			return false, false, emptyHeader, nil
		}
		return true, changed, header, nil
	}

	// TODO(tschottdorf): optimize so that we don't always make a new request
	// slice, only when something changed (copy-on-write).

	var positions []int
	var truncReqs []roachpb.RequestUnion
	for pos, arg := range reqs {
		inner := arg.GetInner()
		hasRequest, changed, newHeader, err := truncateOne(inner)
		if hasRequest {
			// Keep the old one. If we must adjust the header, must copy.
			if changed {
				shallowCopy := inner.ShallowCopy()
				shallowCopy.SetHeader(newHeader)
				truncReqs = append(truncReqs, roachpb.RequestUnion{})
				truncReqs[len(truncReqs)-1].MustSetInner(shallowCopy)
			} else {
				truncReqs = append(truncReqs, arg)
			}
			positions = append(positions, pos)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	return truncReqs, positions, nil
}

// prevLegacy gives the right boundary of the union of all requests which don't
// affect keys larger than the given key. Note that a right boundary is
// exclusive, that is, the returned RKey is to be used as the exclusive right
// endpoint in finding the next range to query.
//
// Informally, a call `prevLegacy(reqs, k)` means: we've already executed the
// parts of `reqs` that intersect `[k, KeyMax)`; please tell me how far to the
// left the next relevant request begins.
func prevLegacy(reqs []roachpb.RequestUnion, k roachpb.RKey) (roachpb.RKey, error) {
	candidate := roachpb.RKeyMin
	for _, union := range reqs {
		inner := union.GetInner()
		h := inner.Header()
		addr, err := keys.Addr(h.Key)
		if err != nil {
			return nil, err
		}
		endKey := h.EndKey
		if len(endKey) == 0 {
			// If we have a point request for `x < k` then that request has not
			// been satisfied (since the batch has only been executed for keys
			// `>=k`). We treat `x` as `[x, x.Next())` which does the right
			// thing below. This also works when `x > k` or `x=k` as the logic
			// below will skip `x`.
			//
			// Note that if the key is /Local/x/something, then instead of using
			// /Local/x/something.Next() as the end key, we rely on
			// AddrUpperBound to handle local keys. In particular,
			// AddrUpperBound will turn it into `x\x00`, so we're looking at the
			// key-range `[x, x.Next())`. This is exactly what we want as the
			// local key is contained in that range.
			//
			// See TestBatchPrevNext for test cases with commentary.
			endKey = h.Key.Next()
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

// nextLegacy gives the left boundary of the union of all requests which don't
// affect keys less than the given key. Note that the left boundary is
// inclusive, that is, the returned RKey is the inclusive left endpoint of the
// keys the request should operate on next.
//
// Informally, a call `nextLegacy(reqs, k)` means: we've already executed the
// parts of `reqs` that intersect `[KeyMin, k)`; please tell me how far to the
// right the next relevant request begins.
func nextLegacy(reqs []roachpb.RequestUnion, k roachpb.RKey) (roachpb.RKey, error) {
	candidate := roachpb.RKeyMax
	for _, union := range reqs {
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
