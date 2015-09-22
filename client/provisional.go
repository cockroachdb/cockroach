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

package client

import (
	"bytes"

	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"golang.org/x/net/context"

	gogoproto "github.com/gogo/protobuf/proto"
)

// SendCall is a convenience function for old tests that still use proto.Call.
// It will disassemble the call, wrap the request in a BatchRequest if
// necessary, send via the provided Sender and re-assemble the call
// transparently.
// TODO(tschottdorf): get rid of proto.Call, change this to
// SendWrapped(proto.BatchRequest).
func SendCall(sender Sender, call proto.Call) error {
	call, unwrap := MaybeWrapCall(call)
	defer unwrap(call)
	// TODO(tschottdorf): remove this crutch.
	sendCallConverted(sender, context.TODO(), call)
	return call.Reply.Header().GoError()
}

// sendCallConverted is a wrapped to go from the (ctx,call) interface to the
// one used by batch.Sender.
// TODO(tschottdorf): remove when new proto.Call is gone.
func sendCallConverted(sender Sender, ctx context.Context, call proto.Call) {
	call, unwrap := MaybeWrapCall(call)
	defer unwrap(call)

	{
		br := *call.Args.(*proto.BatchRequest)
		if len(br.Requests) == 0 {
			panic(br)
		}
		br.Key, br.EndKey = keys.Range(br)
		if bytes.Equal(br.Key, proto.KeyMax) {
			panic(br)
		}
	}

	reply, pErr := sender.Send(ctx, *call.Args.(*proto.BatchRequest))

	if reply != nil {
		call.Reply.Reset() // required for BatchRequest (concats response otherwise)
		gogoproto.Merge(call.Reply, reply)
	}
	if call.Reply.Header().GoError() != nil {
		panic(proto.ErrorUnexpectedlySet(sender, call.Reply))
	}
	if pErr != nil {
		call.Reply.Header().Error = pErr
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

// MaybeWrap wraps the given argument in a batch, unless it is already one.
// TODO(tschottdorf): will go when proto.Call does.
func MaybeWrap(args proto.Request) (*proto.BatchRequest, func(proto.Response) proto.Response) {
	if ba, ok := args.(*proto.BatchRequest); ok {
		return ba, func(a proto.Response) proto.Response { return a }
	}
	ba := &proto.BatchRequest{}
	ba.RequestHeader = *(gogoproto.Clone(args.Header()).(*proto.RequestHeader))
	ba.Add(args)
	return ba, func(reply proto.Response) proto.Response {
		br, ok := reply.(*proto.BatchResponse)
		if !ok {
			// Request likely never sent, but caught a local error.
			return reply
		}
		var unwrappedReply proto.Response
		if len(br.Responses) == 0 {
			unwrappedReply = args.CreateReply()
		} else {
			unwrappedReply = br.Responses[0].GetInner()
		}
		// The ReplyTxn is propagated from one response to the next request,
		// and we adopt the mechanism that whenever the Txn changes, it needs
		// to be set in the reply, for example to ratched up the transaction
		// timestamp on writes when necessary.
		// This is internally necessary to sequentially execute the batch,
		// so it makes some sense to take the burden of updating the Txn
		// from TxnCoordSender - it will only need to act on retries/aborts
		// in the future.
		unwrappedReply.Header().Txn = br.Txn
		if unwrappedReply.Header().Error == nil {
			unwrappedReply.Header().Error = br.Error
		}
		return unwrappedReply
	}
}
