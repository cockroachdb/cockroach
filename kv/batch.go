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
	"math/rand"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"golang.org/x/net/context"
)

// truncate restricts all contained requests to the given key range.
// Even on error, the returned closure must be executed; it undoes any
// truncations performed.
// First, the boundaries of the truncation are obtained: This is the
// intersection between [from,to) and the descriptor's range.
// Secondly, all requests contained in the batch are "truncated" to
// the resulting range, inserting NoopRequest appropriately to
// replace requests which are left without a key range to operate on.
// The number of non-noop requests after truncation is returned along
// with a closure which must be executed to undo the truncation, even
// in case of an error.
// TODO(tschottdorf): Consider returning a new BatchRequest, which has more
// overhead in the common case of a batch which never needs truncation but is
// less magical.
func truncate(br *roachpb.BatchRequest, desc *roachpb.RangeDescriptor, from, to roachpb.RKey) (func(), int, error) {
	if !desc.ContainsKey(from) {
		from = desc.StartKey
	}
	if !desc.ContainsKeyRange(desc.StartKey, to) || to == nil {
		to = desc.EndKey
	}
	truncateOne := func(args roachpb.Request) (bool, []func(), error) {
		if _, ok := args.(*roachpb.NoopRequest); ok {
			return true, nil, nil
		}
		header := args.Header()
		if !roachpb.IsRange(args) {
			// This is a point request.
			if len(header.EndKey) > 0 {
				return false, nil, util.Errorf("%T is not a range command, but EndKey is set", args)
			}
			if !desc.ContainsKey(keys.Addr(header.Key)) {
				return true, nil, nil
			}
			return false, nil, nil
		}
		// We're dealing with a range-spanning request.
		var undo []func()
		keyAddr, endKeyAddr := keys.Addr(header.Key), keys.Addr(header.EndKey)
		if l, r := !keyAddr.Equal(header.Key), !endKeyAddr.Equal(header.EndKey); l || r {
			if !desc.ContainsKeyRange(keyAddr, endKeyAddr) {
				return false, nil, util.Errorf("local key range must not span ranges")
			}
			if !l || !r {
				return false, nil, util.Errorf("local key mixed with global key in range")
			}
		}
		// Below, {end,}keyAddr equals header.{End,}Key, so nothing is local.
		if keyAddr.Less(from) {
			{
				origKey := header.Key
				undo = append(undo, func() { header.Key = origKey })
			}
			header.Key = from.AsRawKey() // "from" can't be local
			keyAddr = from
		}
		if !endKeyAddr.Less(to) {
			{
				origEndKey := header.EndKey
				undo = append(undo, func() { header.EndKey = origEndKey })
			}
			header.EndKey = to.AsRawKey() // "to" can't be local
			endKeyAddr = to
		}
		// Check whether the truncation has left any keys in the range. If not,
		// we need to cut it out of the request.
		return !keyAddr.Less(endKeyAddr), undo, nil
	}

	var fns []func()
	gUndo := func() {
		for _, f := range fns {
			f()
		}
	}

	var numNoop int
	for pos, arg := range br.Requests {
		omit, undo, err := truncateOne(arg.GetInner())
		if omit {
			numNoop++
			nReq := &roachpb.RequestUnion{}
			if !nReq.SetValue(&roachpb.NoopRequest{}) {
				panic("RequestUnion excludes NoopRequest")
			}
			oReq := br.Requests[pos]
			br.Requests[pos] = *nReq
			posCpy := pos // for closure
			undo = append(undo, func() {
				br.Requests[posCpy] = oReq
			})
		}
		fns = append(fns, undo...)
		if err != nil {
			return gUndo, 0, err
		}
	}
	return gUndo, len(br.Requests) - numNoop, nil
}

// SenderFn is a function that implements a Sender.
type senderFn func(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)

// Send implements batch.Sender.
func (f senderFn) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	return f(ctx, ba)
}

// A ChunkingSender sends batches, subdividing them appropriately.
type chunkingSender struct {
	f senderFn
}

// NewChunkingSender returns a new chunking sender which sends through the supplied
// SenderFn.
func newChunkingSender(f senderFn) client.Sender {
	return &chunkingSender{f: f}
}

// Send implements Sender.
// TODO(tschottdorf): We actually don't want to chop EndTransaction off for
// single-range requests (but that happens now since EndTransaction has the
// isAlone flag). Whether it is one or not is unknown right now (you can only
// find out after you've sent to the Range/looked up a descriptor that suggests
// that you're multi-range. In those cases, the wrapped sender should return an
// error so that we split and retry once the chunk which contains
// EndTransaction (i.e. the last one).
func (cs *chunkingSender) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	if len(ba.Requests) < 1 {
		panic("empty batch")
	}

	// Deterministically create ClientCmdIDs for all parts of the batch if
	// a CmdID is already set (otherwise, leave them empty).
	var nextID func() roachpb.ClientCmdID
	empty := roachpb.ClientCmdID{}
	if empty == ba.CmdID {
		nextID = func() roachpb.ClientCmdID {
			return empty
		}
	} else {
		rng := rand.New(rand.NewSource(ba.CmdID.Random))
		id := ba.CmdID
		nextID = func() roachpb.ClientCmdID {
			curID := id             // copy
			id.Random = rng.Int63() // adjust for next call
			return curID
		}
	}

	parts := ba.Split(true /* canSplitET */)
	var rplChunks []*roachpb.BatchResponse
	for _, part := range parts {
		ba.Requests = part
		ba.CmdID = nextID()
		rpl, err := cs.f(ctx, ba)
		if err != nil {
			return nil, err
		}
		// Propagate transaction from last reply to next request. The final
		// update is taken and put into the response's main header.
		ba.Txn.Update(rpl.Header().Txn)

		rplChunks = append(rplChunks, rpl)
	}

	reply := rplChunks[0]
	for _, rpl := range rplChunks[1:] {
		reply.Responses = append(reply.Responses, rpl.Responses...)
	}
	lastHeader := rplChunks[len(rplChunks)-1].BatchResponse_Header
	reply.Error = lastHeader.Error
	reply.Timestamp = lastHeader.Timestamp
	reply.Txn = ba.Txn
	return reply, nil
}

// prev gives the right boundary of the union of all requests which don't
// affect keys larger than the given key.
// TODO(tschottdorf): again, better on BatchRequest itself, but can't pull
// 'keys' into 'proto'.
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
