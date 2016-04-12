// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf

package kv

import (
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
)

var emptySpan = roachpb.Span{}
var noopRequest = roachpb.NoopRequest{}

// truncate restricts all contained requests to the given key range
// and returns a new BatchRequest.
// All requests contained in that batch are "truncated" to the given
// span, inserting NoopRequest appropriately to replace requests which
// are left without a key range to operate on. The number of non-noop
// requests after truncation is returned.
func truncate(ba roachpb.BatchRequest, rs roachpb.RSpan) (roachpb.BatchRequest, int, error) {
	truncateOne := func(args roachpb.Request) (bool, roachpb.Span, error) {
		if _, ok := args.(*roachpb.NoopRequest); ok {
			return true, emptySpan, nil
		}
		header := args.Header()
		if !roachpb.IsRange(args) {
			// This is a point request.
			if len(header.EndKey) > 0 {
				return false, emptySpan, util.Errorf("%T is not a range command, but EndKey is set", args)
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
		if l, r := !keyAddr.Equal(header.Key), !endKeyAddr.Equal(header.EndKey); l || r {
			if !l || !r {
				return false, emptySpan, util.Errorf("local key mixed with global key in range")
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

	var numNoop int
	origRequests := ba.Requests
	ba.Requests = make([]roachpb.RequestUnion, len(ba.Requests))
	for pos, arg := range origRequests {
		hasRequest, newHeader, err := truncateOne(arg.GetInner())
		if !hasRequest {
			// We omit this one, i.e. replace it with a Noop.
			numNoop++
			union := roachpb.RequestUnion{}
			union.MustSetInner(&noopRequest)
			ba.Requests[pos] = union
		} else {
			// Keep the old one. If we must adjust the header, must copy.
			if inner := origRequests[pos].GetInner(); newHeader.Equal(inner.Header()) {
				ba.Requests[pos] = origRequests[pos]
			} else {
				shallowCopy := inner.ShallowCopy()
				shallowCopy.SetHeader(newHeader)
				union := &ba.Requests[pos] // avoid operating on copy
				union.MustSetInner(shallowCopy)
			}
		}
		if err != nil {
			return roachpb.BatchRequest{}, 0, err
		}
	}
	return ba, len(ba.Requests) - numNoop, nil
}

// prev gives the right boundary of the union of all requests which don't
// affect keys larger than the given key.
// TODO(tschottdorf): again, better on BatchRequest itself, but can't pull
// 'keys' into 'roachpb'.
func prev(ba roachpb.BatchRequest, k roachpb.RKey) (roachpb.RKey, error) {
	candidate := roachpb.RKeyMin
	for _, union := range ba.Requests {
		h := union.GetInner().Header()
		addr, err := keys.Addr(h.Key)
		if err != nil {
			return nil, err
		}
		eAddr, err := keys.AddrUpperBound(h.EndKey)
		if err != nil {
			return nil, err
		}
		if len(eAddr) == 0 {
			eAddr = addr.Next()
		}
		if !eAddr.Less(k) {
			if !k.Less(addr) {
				// Range contains k, so won't be able to go lower.
				return k, nil
			}
			// Range is disjoint from [KeyMin,k).
			continue
		}
		// We want the largest surviving candidate.
		if candidate.Less(addr) {
			candidate = addr
		}
	}
	return candidate, nil
}

// next gives the left boundary of the union of all requests which don't
// affect keys less than the given key.
// TODO(tschottdorf): again, better on BatchRequest itself, but can't pull
// 'keys' into 'proto'.
func next(ba roachpb.BatchRequest, k roachpb.RKey) (roachpb.RKey, error) {
	candidate := roachpb.RKeyMax
	for _, union := range ba.Requests {
		h := union.GetInner().Header()
		addr, err := keys.Addr(h.Key)
		if err != nil {
			return nil, err
		}
		if addr.Less(k) {
			eAddr, err := keys.AddrUpperBound(h.EndKey)
			if err != nil {
				return nil, err
			}
			if k.Less(eAddr) {
				// Starts below k, but continues beyond. Need to stay at k.
				return k, nil
			}
			// Affects only [KeyMin,k).
			continue
		}
		// We want the smallest of the surviving candidates.
		if addr.Less(candidate) {
			candidate = addr
		}
	}
	return candidate, nil
}
