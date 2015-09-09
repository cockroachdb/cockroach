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
	"github.com/cockroachdb/cockroach/proto"
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
func truncate(br *proto.BatchRequest, desc *proto.RangeDescriptor, from, to proto.Key) (func(), int, error) {
	if !desc.ContainsKey(from) {
		from = desc.StartKey
	}
	if !desc.ContainsKeyRange(desc.StartKey, to) || to == nil {
		to = desc.EndKey
	}
	truncateOne := func(args proto.Request) (bool, []func(), error) {
		header := args.Header()
		if !proto.IsRange(args) {
			if len(header.EndKey) > 0 {
				return false, nil, util.Errorf("%T is not a range command, but EndKey is set", args)
			}
			if !desc.ContainsKey(keys.KeyAddress(header.Key)) {
				return true, nil, nil
			}
			return false, nil, nil
		}
		var undo []func()
		key, endKey := header.Key, header.EndKey
		keyAddr, endKeyAddr := keys.KeyAddress(key), keys.KeyAddress(endKey)
		if keyAddr.Less(from) {
			undo = append(undo, func() { header.Key = key })
			header.Key = from
			keyAddr = from
		}
		if !endKeyAddr.Less(to) {
			undo = append(undo, func() { header.EndKey = endKey })
			header.EndKey = to
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
		omit, undo, err := truncateOne(arg.GetValue().(proto.Request))
		if omit {
			numNoop++
			nReq := &proto.RequestUnion{}
			nReq.SetValue(&proto.NoopRequest{})
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
type senderFn func(context.Context, proto.BatchRequest) (*proto.BatchResponse, error)

// SendBatch implements batch.Sender.
func (f senderFn) SendBatch(ctx context.Context, ba proto.BatchRequest) (*proto.BatchResponse, error) {
	return f(ctx, ba)
}

// A ChunkingSender sends batches, subdividing them appropriately.
type chunkingSender struct {
	f senderFn
}

// NewChunkingSender returns a new chunking sender which sends through the supplied
// SenderFn.
func newChunkingSender(f senderFn) client.BatchSender {
	return &chunkingSender{f: f}
}

// SendBatch implements Sender.
// TODO(tschottdorf): We actually don't want to chop EndTransaction off for
// single-range requests. Whether it is one or not is unknown right now (you
// can only find out after you've sent to the Range/looked up a descriptor that
// suggests that you're multi-range. In those cases, the wrapped sender should
// return an error so that we split and retry once the chunk which contains
// EndTransaction (i.e. the last one).
func (cs *chunkingSender) SendBatch(ctx context.Context, ba proto.BatchRequest) (*proto.BatchResponse, error) {
	if len(ba.Requests) < 1 {
		panic("empty batch")
	}

	// Deterministically create ClientCmdIDs for all parts of the batch if
	// a CmdID is already set (otherwise, leave them empty).
	var nextID func() proto.ClientCmdID
	empty := proto.ClientCmdID{}
	if empty == ba.CmdID {
		nextID = func() proto.ClientCmdID {
			return empty
		}
	} else {
		rng := rand.New(rand.NewSource(ba.CmdID.Random))
		id := ba.CmdID
		nextID = func() proto.ClientCmdID {
			curID := id             // copy
			id.Random = rng.Int63() // adjust for next call
			return curID
		}
	}

	parts := ba.Split()
	var rplChunks []*proto.BatchResponse
	var part []proto.RequestUnion
	for len(parts) > 0 {
		part, parts = parts[0], parts[1:]
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
	reply.ResponseHeader = rplChunks[len(rplChunks)-1].ResponseHeader
	reply.Txn = ba.Txn
	return reply, nil
}

// prev gives the right boundary of the union of all requests which don't
// affect keys larger than the given key.
// TODO(tschottdorf): again, better on BatchRequest itself, but can't pull
// 'keys' into 'proto'.
func prev(ba proto.BatchRequest, k proto.Key) proto.Key {
	candidate := proto.KeyMin
	for _, union := range ba.Requests {
		h := union.GetValue().(proto.Request).Header()
		addr := keys.KeyAddress(h.Key)
		eAddr := keys.KeyAddress(h.EndKey)
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
func next(ba proto.BatchRequest, k proto.Key) proto.Key {
	candidate := proto.KeyMax
	for _, union := range ba.Requests {
		h := union.GetValue().(proto.Request).Header()
		addr := keys.KeyAddress(h.Key)
		if addr.Less(k) {
			if eAddr := keys.KeyAddress(h.EndKey); k.Less(eAddr) {
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
