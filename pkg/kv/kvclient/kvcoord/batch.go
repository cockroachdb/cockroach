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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/errors"
)

type BatchTruncationHelper struct {
	requests  []roachpb.RequestUnion
	headers   []roachpb.RequestHeader
	positions []int
	isRange   []bool
}

var _ sort.Interface = &BatchTruncationHelper{}

func (h *BatchTruncationHelper) Len() int {
	return len(h.requests)
}

func (h *BatchTruncationHelper) Less(i, j int) bool {
	return h.headers[i].Key.Compare(h.headers[j].Key) < 0
}

func (h *BatchTruncationHelper) Swap(i, j int) {
	h.requests[i], h.requests[j] = h.requests[j], h.requests[i]
	h.headers[i], h.headers[j] = h.headers[j], h.headers[i]
	h.positions[i], h.positions[j] = h.positions[j], h.positions[i]
	h.isRange[i], h.isRange[j] = h.isRange[j], h.isRange[i]
}

func (h *BatchTruncationHelper) Init(requests []roachpb.RequestUnion, scanDir ScanDirection) error {
	if scanDir == Descending {
		panic("unimplemented")
	}
	h.requests = requests
	h.headers = make([]roachpb.RequestHeader, len(requests))
	h.positions = make([]int, len(requests))
	h.isRange = make([]bool, len(requests))
	for i := range requests {
		req := requests[i].GetInner()
		h.headers[i] = req.Header()
		h.positions[i] = i
		h.isRange[i] = roachpb.IsRange(req)
		if !h.isRange[i] && len(h.headers[i].EndKey) > 0 {
			return errors.Errorf("%T is not a range command, but EndKey is set", req)
		}
		if h.isRange[i] {
			// We're dealing with a range-spanning request.
			if l, r := keys.IsLocal(h.headers[i].Key), keys.IsLocal(h.headers[i].EndKey); l || r {
				if !l || !r {
					return errors.Errorf("local key mixed with global key in range")
				}
			}
		}
	}
	sort.Sort(h)
	return nil
}

func (h *BatchTruncationHelper) Truncate(rs roachpb.RSpan) ([]roachpb.RequestUnion, []int, error) {
	var truncReqs []roachpb.RequestUnion
	var positions []int
	for i, pos := range h.positions {
		if pos < 0 {
			continue
		}
		header := h.headers[i]
		// TODO: check whether it's worth storing keyAddr explicitly.
		keyAddr, err := keys.Addr(header.Key)
		if err != nil {
			return nil, nil, err
		}
		if rs.EndKey.Compare(keyAddr) <= 0 {
			// All of the remaining requests start after this range, so we're
			// done.
			break
		}
		if !h.isRange[i] {
			// This is a point request, and the key is contained within this
			// range, so we include as is and mark it as "fully processed".
			truncReqs = append(truncReqs, h.requests[i])
			positions = append(positions, pos)
			h.positions[i] = -1
			continue
		}
		// We're dealing with a range-spanning request.
		endKeyAddr, err := keys.Addr(header.EndKey)
		if err != nil {
			return nil, nil, err
		}
		if buildutil.CrdbTestBuild {
			// rs.Key can't be local because it contains range split points,
			// which are never local.
			if keyAddr.Less(rs.Key) {
				return nil, nil, errors.AssertionFailedf(
					"unexpectedly keyAddr %s is less than rs %s", keyAddr, rs,
				)
			}
		}
		// rs.EndKey can't be local because it contains range split points,
		// which are never local.
		if endKeyAddr.Compare(rs.EndKey) <= 0 {
			// The last part of this request is fully contained within this
			// range, so we adjust the original request according to the
			// remaining part of its header and mark the request as "fully
			// processed".
			h.requests[i].GetInner().SetHeader(header)
			truncReqs = append(truncReqs, h.requests[i])
			h.positions[i] = -1
		} else {
			if !keys.IsLocal(header.EndKey) {
				header.EndKey = rs.EndKey.AsRawKey()
			} else {
				// The local end key should be truncated to the boundary of
				// local keys which address to rs.EndKey.
				header.EndKey = keys.MakeRangeKeyPrefix(rs.EndKey)
			}
			// There will be more parts from this request, so we make a copy and
			// and update the header.
			shallowCopy := h.requests[i].GetInner().ShallowCopy()
			shallowCopy.SetHeader(header)
			truncReqs = append(truncReqs, roachpb.RequestUnion{})
			truncReqs[len(truncReqs)-1].MustSetInner(shallowCopy)
			// Adjust the start key of the header so that it contained only the
			// unprocessed suffix of the request.
			h.headers[i].Key = header.EndKey
		}
		positions = append(positions, pos)
	}
	return truncReqs, positions, nil
}

func (h *BatchTruncationHelper) Next() (roachpb.RKey, error) {
	for i, pos := range h.positions {
		if pos < 0 {
			continue
		}
		return keys.Addr(h.headers[i].Key)
	}
	return roachpb.RKeyMax, nil
}

var emptyHeader = roachpb.RequestHeader{}

// Truncate restricts all requests to the given key range and returns new,
// truncated, requests. All returned requests are "truncated" to the given span,
// and requests which are found to not overlap the given span at all are
// removed. A mapping of response index to request index is returned. For
// example, if
//
// reqs = Put[a], Put[c], Put[b],
// rs = [a,bb],
//
// then Truncate(reqs,rs) returns (Put[a], Put[b]) and positions [0,2].
func Truncate(
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
			// rs.Key can't be local because it contains range split points, which
			// are never local.
			changed = true
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
			changed = true
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

// prev gives the right boundary of the union of all requests which don't
// affect keys larger than the given key. Note that a right boundary is
// exclusive, that is, the returned RKey is to be used as the exclusive
// right endpoint in finding the next range to query.
//
// Informally, a call `prev(reqs, k)` means: we've already executed the parts
// of `reqs` that intersect `[k, KeyMax)`; please tell me how far to the
// left the next relevant request begins.
//
// TODO(tschottdorf): again, better on BatchRequest itself, but can't pull
// 'keys' into 'roachpb'.
func prev(reqs []roachpb.RequestUnion, k roachpb.RKey) (roachpb.RKey, error) {
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
			// If we have a point request for `x < k` then that request has not been
			// satisfied (since the batch has only been executed for keys `>=k`). We
			// treat `x` as `[x, x.Next())` which does the right thing below. This
			// also works when `x > k` or `x=k` as the logic below will skip `x`.
			//
			// Note that if the key is /Local/x/something, then instead of using
			// /Local/x/something.Next() as the end key, we rely on AddrUpperBound to
			// handle local keys. In particular, AddrUpperBound will turn it into
			// `x\x00`, so we're looking at the key-range `[x, x.Next())`. This is
			// exactly what we want as the local key is contained in that range.
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

// Next gives the left boundary of the union of all requests which don't affect
// keys less than the given key. Note that the left boundary is inclusive, that
// is, the returned RKey is the inclusive left endpoint of the keys the request
// should operate on next.
//
// Informally, a call `Next(reqs, k)` means: we've already executed the parts of
// `reqs` that intersect `[KeyMin, k)`; please tell me how far to the right the
// next relevant request begins.
//
// TODO(tschottdorf): again, better on BatchRequest itself, but can't pull
// 'keys' into 'proto'.
func Next(reqs []roachpb.RequestUnion, k roachpb.RKey) (roachpb.RKey, error) {
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
