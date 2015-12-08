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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf

package kv

import (
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/gogo/protobuf/proto"
)

var emptySpan = roachpb.Span{}

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
		header := *args.Header()
		if !roachpb.IsRange(args) {
			// This is a point request.
			if len(header.EndKey) > 0 {
				return false, emptySpan, util.Errorf("%T is not a range command, but EndKey is set", args)
			}
			if !rs.ContainsKey(keys.Addr(header.Key)) {
				return false, emptySpan, nil
			}
			return true, header, nil
		}
		// We're dealing with a range-spanning request.
		keyAddr, endKeyAddr := keys.Addr(header.Key), keys.Addr(header.EndKey)
		if l, r := !keyAddr.Equal(header.Key), !endKeyAddr.Equal(header.EndKey); l || r {
			if !rs.ContainsKeyRange(keyAddr, endKeyAddr) {
				return false, emptySpan, util.Errorf("local key range must not span ranges")
			}
			if !l || !r {
				return false, emptySpan, util.Errorf("local key mixed with global key in range")
			}
			// Range-local local key range.
			return true, header, nil
		}
		// Below, {end,}keyAddr equals header.{End,}Key, so nothing is local.
		if keyAddr.Less(rs.Key) {
			header.Key = rs.Key.AsRawKey() // "key" can't be local
			keyAddr = rs.Key
		}
		if !endKeyAddr.Less(rs.EndKey) {
			header.EndKey = rs.EndKey.AsRawKey() // "endKey" can't be local
			endKeyAddr = rs.EndKey
		}
		// Check whether the truncation has left any keys in the range. If not,
		// we need to cut it out of the request.
		if !keyAddr.Less(endKeyAddr) {
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
			nReq := roachpb.RequestUnion{}
			if !nReq.SetValue(&roachpb.NoopRequest{}) {
				panic("RequestUnion excludes NoopRequest")
			}
			ba.Requests[pos] = nReq
		} else {
			// Keep the old one. If we must adjust the header, must copy.
			// TODO(tschottdorf): this could wind up cloning big chunks of data.
			// Can optimize by creating a new Request manually, but with the old
			// data.
			if newHeader.Equal(*origRequests[pos].GetInner().Header()) {
				ba.Requests[pos] = origRequests[pos]
			} else {
				ba.Requests[pos] = *proto.Clone(&origRequests[pos]).(*roachpb.RequestUnion)
				*ba.Requests[pos].GetInner().Header() = newHeader
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
func prev(ba roachpb.BatchRequest, k roachpb.RKey) roachpb.RKey {
	candidate := roachpb.RKeyMin
	for _, union := range ba.Requests {
		h := union.GetInner().Header()
		addr := keys.Addr(h.Key)
		eAddr := keys.Addr(h.EndKey)
		if len(eAddr) == 0 {
			// Can probably avoid having to compute Next() here if
			// we're in the mood for some more complexity.
			eAddr = addr.Next()
		}
		if !eAddr.Less(k) {
			if !k.Less(addr) {
				// Range contains k, so won't be able to go lower.
				return k
			}
			// Range is disjoint from [KeyMin,k).
			continue
		}
		// We want the largest surviving candidate.
		if candidate.Less(addr) {
			candidate = addr
		}
	}
	return candidate
}

// next gives the left boundary of the union of all requests which don't
// affect keys less than the given key.
// TODO(tschottdorf): again, better on BatchRequest itself, but can't pull
// 'keys' into 'proto'.
func next(ba roachpb.BatchRequest, k roachpb.RKey) roachpb.RKey {
	candidate := roachpb.RKeyMax
	for _, union := range ba.Requests {
		h := union.GetInner().Header()
		addr := keys.Addr(h.Key)
		if addr.Less(k) {
			if eAddr := keys.Addr(h.EndKey); k.Less(eAddr) {
				// Starts below k, but continues beyond. Need to stay at k.
				return k
			}
			// Affects only [KeyMin,k).
			continue
		}
		// We want the smallest of the surviving candidates.
		if addr.Less(candidate) {
			candidate = addr
		}
	}
	return candidate
}
