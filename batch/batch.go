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

// TODO(tschottdorf): provisional home for all of the below.

package batch

import (
	"bytes"
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"golang.org/x/net/context"

	gogoproto "github.com/gogo/protobuf/proto"
)

// UpdateForBatch updates the first argument (the header of a request contained
// in a batch) from the second one (the batch header), returning an error when
// inconsistencies are found.
// It is checked that the individual call does not have a UserPriority
// or Txn set that differs from the batch's.
// TODO(tschottdorf): will go with #2143.
func UpdateForBatch(args proto.Request, bHeader proto.RequestHeader) error {
	// Disallow transaction, user and priority on individual calls, unless
	// equal.
	aHeader := args.Header()
	if aPrio := aHeader.GetUserPriority(); aPrio != proto.Default_RequestHeader_UserPriority && aPrio != bHeader.GetUserPriority() {
		return util.Errorf("conflicting user priority on call in batch")
	}
	aHeader.UserPriority = bHeader.UserPriority
	aHeader.Txn = bHeader.Txn // reqs always take Txn from batch
	return nil
}

// MaybeWrap wraps the given argument in a batch, unless it is already one.
// TODO(tschottdorf): will go when proto.Call does.
func MaybeWrap(args proto.Request) (*proto.BatchRequest, func(proto.Response) proto.Response) {
	if bArgs, ok := args.(*proto.BatchRequest); ok {
		return bArgs, func(a proto.Response) proto.Response { return a }
	}
	bArgs := &proto.BatchRequest{}
	bArgs.RequestHeader = *(gogoproto.Clone(args.Header()).(*proto.RequestHeader))
	bArgs.Add(args)
	return bArgs, func(reply proto.Response) proto.Response {
		bReply, ok := reply.(*proto.BatchResponse)
		if !ok {
			// Request likely never sent, but caught a local error.
			return reply
		}
		var unwrappedReply proto.Response
		if len(bReply.Responses) == 0 {
			unwrappedReply = args.CreateReply()
		} else {
			unwrappedReply = bReply.Responses[0].GetValue().(proto.Response)
		}
		// The ReplyTxn is propagated from one response to the next request,
		// and we adopt the mechanism that whenever the Txn changes, it needs
		// to be set in the reply, for example to ratched up the transaction
		// timestamp on writes when necessary.
		// This is internally necessary to sequentially execute the batch,
		// so it makes some sense to take the burden of updating the Txn
		// from TxnCoordSender - it will only need to act on retries/aborts
		// in the future.
		unwrappedReply.Header().Txn = bReply.Txn
		if unwrappedReply.Header().Error == nil {
			unwrappedReply.Header().Error = bReply.Error
		}
		return unwrappedReply
	}
}

// MaybeWrapCall returns a new call which wraps the original Args and Reply
// in a batch, if necessary.
// TODO(tschottdorf): will go when proto.Call does.
func MaybeWrapCall(call proto.Call) (proto.Call, func(proto.Call) proto.Call) {
	var unwrap func(proto.Response) proto.Response
	call.Args, unwrap = MaybeWrap(call.Args)
	newUnwrap := func(origReply proto.Response) func(proto.Call) proto.Call {
		return func(newCall proto.Call) proto.Call {
			origReply.Reset()
			gogoproto.Merge(origReply, unwrap(newCall.Reply))
			*origReply.Header() = *newCall.Reply.Header()
			newCall.Reply = origReply
			return newCall
		}
	}(call.Reply)
	call.Reply = call.Args.CreateReply()
	return call, newUnwrap
}

// KeyRange returns a key range which contains all keys in the Batch.
// In particular, this resolves local addressing.
// TODO(tschottdorf): testing.
// TODO(tschottdorf): ideally method on *BatchRequest. See #2198.
func KeyRange(br *proto.BatchRequest) (proto.Key, proto.Key) {
	from := proto.KeyMax
	to := proto.KeyMin
	for _, arg := range br.Requests {
		req := arg.GetValue().(proto.Request)
		if req.Method() == proto.Noop {
			continue
		}
		h := req.Header()
		key := keys.KeyAddress(h.Key)
		if key.Less(keys.KeyAddress(from)) {
			// Key is smaller than `from`.
			from = key
		}
		if keys.KeyAddress(to).Less(key) {
			// Key is larger than `to`.
			to = key.Next()
		}
		if endKey := keys.KeyAddress(h.EndKey); keys.KeyAddress(to).Less(endKey) {
			// EndKey is larger than `to`.
			to = endKey
		}
	}
	return from, to
}

// Short gives a brief summary of the contained requests and keys in the batch.
// TODO(tschottdorf): awkward to not have this on BatchRequest, but can't pull
// `keys` into `proto` (req'd by KeyAddress). See #2198.
func Short(br *proto.BatchRequest) string {
	var str []string
	for _, arg := range br.Requests {
		req := arg.GetValue().(proto.Request)
		h := req.Header()
		str = append(str, fmt.Sprintf("%T [%s,%s)", req, h.Key, h.EndKey))
	}
	from, to := KeyRange(br)
	return fmt.Sprintf("[%s,%s): ", from, to) + strings.Join(str, ", ")
}

// Sender is a new incarnation of client.Sender which only supports batches
// and uses a request-response pattern.
// TODO(tschottdorf): do away with client.Sender.
// TODO(tschottdorf) rename to SendBatch->Send when client.Sender is out of the way.
type Sender interface {
	SendBatch(context.Context, *proto.BatchRequest) (*proto.BatchResponse, error)
}

// SenderFn is a function that implements a Sender.
type SenderFn func(context.Context, *proto.BatchRequest) (*proto.BatchResponse, error)

// SendBatch implements batch.Sender.
func (f SenderFn) SendBatch(ctx context.Context, ba *proto.BatchRequest) (*proto.BatchResponse, error) {
	return f(ctx, ba)
}

// A ChunkingSender sends batches, subdividing them appropriately.
// TODO(tschottdorf): doesn't actually chunk much yet, only puts EndTransaction
// into an extra batch. Easy to complete.
// TODO(tschottdorf): only used by DistSender, but let's be modular.
type ChunkingSender struct {
	f SenderFn
}

// NewChunkingSender returns a new chunking sender which sends through the supplied
// SenderFn.
func NewChunkingSender(f SenderFn) Sender {
	return &ChunkingSender{f: f}
}

// SendBatch implements Sender.
// TODO(tschottdorf): only cuts an EndTransaction request off. Also need
// to untangle reverse/forward, txn/non-txn, ...
// We actually don't want to chop EndTransaction off for single-range
// requests. Whether it is one or not is unknown right now (you can only
// find out after you've sent to the Range/looked up a descriptor that
// suggests that you're multi-range. In those cases, the wrapped sender should
// return an error so that we split and retry once the chunk which contains
// EndTransaction (i.e. the last one).
func (cs *ChunkingSender) SendBatch(ctx context.Context, ba *proto.BatchRequest) (*proto.BatchResponse, error) {
	var argChunks []*proto.BatchRequest
	if len(ba.Requests) < 1 {
		panic("empty batchArgs")
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

	if rArg, ok := ba.GetArg(proto.EndTransaction); ok &&
		len(ba.Requests) > 1 {
		et := rArg.(*proto.EndTransactionRequest)
		firstChunk := *ba // shallow copy so that we get to manipulate fields
		etChunk := &proto.BatchRequest{}
		etChunk.Add(et)
		etChunk.RequestHeader = *gogoproto.Clone(&ba.RequestHeader).(*proto.RequestHeader)
		firstChunk.Requests = ba.Requests[:len(ba.Requests)-1]
		argChunks = append(argChunks, &firstChunk, etChunk)
	} else {
		argChunks = append(argChunks, ba)
	}
	var rplChunks []*proto.BatchResponse
	for len(argChunks) > 0 {
		ba, argChunks = argChunks[0], argChunks[1:]
		ba.CmdID = nextID()
		rpl, err := cs.f(ctx, ba)
		if err != nil {
			return nil, err
		}
		// Propagate transaction from last reply to next request.
		if len(argChunks) > 0 {
			argChunks[0].Header().Txn.Update(rpl.Header().Txn)
		}
		rplChunks = append(rplChunks, rpl)
	}

	reply := rplChunks[0]
	for _, rpl := range rplChunks[1:] {
		reply.Responses = append(reply.Responses, rpl.Responses...)
	}
	reply.ResponseHeader = rplChunks[len(rplChunks)-1].ResponseHeader
	return reply, nil
}

// SendCallConverted is a wrapped to go from the (ctx,call) interface to the
// one used by batch.Sender.
// TODO(tschottdorf): remove when new proto.Call is gone.
func SendCallConverted(sender Sender, ctx context.Context, call proto.Call) {
	call, unwrap := MaybeWrapCall(call)
	defer unwrap(call)

	{
		br := call.Args.(*proto.BatchRequest)
		if len(br.Requests) == 0 {
			panic(Short(br))
		}
		br.Key, br.EndKey = KeyRange(br)
		if bytes.Equal(br.Key, proto.KeyMax) {
			panic(Short(br))
		}
	}

	reply, err := sender.SendBatch(ctx, call.Args.(*proto.BatchRequest))

	if reply != nil {
		call.Reply.Reset() // required for BatchRequest (concats response otherwise)
		gogoproto.Merge(call.Reply, reply)
	}
	if call.Reply.Header().GoError() != nil {
		panic(proto.ErrorUnexpectedlySet)
	}
	if err != nil {
		call.Reply.Header().SetGoError(err)
	}
}

// Prev gives the right boundary of the union of all requests which don't
// affect keys larger than the given key.
func Prev(ba *proto.BatchRequest, k proto.Key) proto.Key {
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

// Next gives the left boundary of the union of all requests which don't
// affect keys less than the given key.
func Next(ba *proto.BatchRequest, k proto.Key) proto.Key {
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
