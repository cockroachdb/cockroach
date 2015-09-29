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

package batchutil

import (
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"golang.org/x/net/context"

	gogoproto "github.com/gogo/protobuf/proto"
)

// SendWrapped is a convenience function which wraps the request in a batch,
// sends it via the provided Sender, and returns the unwrapped response
// or an error.
func SendWrapped(sender client.Sender, args roachpb.Request) (roachpb.Response, error) {
	ba, unwrap := maybeWrap(args)
	br, pErr := sender.Send(context.TODO(), *ba)
	if err := pErr.GoError(); err != nil {
		return nil, err
	}
	return unwrap(br), nil
}

// MaybeWrap wraps the given argument in a batch, unless it is already one.
func maybeWrap(args roachpb.Request) (*roachpb.BatchRequest, func(*roachpb.BatchResponse) roachpb.Response) {
	ba := &roachpb.BatchRequest{}
	ba.RequestHeader = *(gogoproto.Clone(args.Header()).(*roachpb.RequestHeader))
	ba.Add(args)
	return ba, func(br *roachpb.BatchResponse) roachpb.Response {
		var unwrappedReply roachpb.Response
		if len(br.Responses) == 0 {
			unwrappedReply = args.CreateReply()
		} else {
			unwrappedReply = br.Responses[0].GetInner()
		}
		// The ReplyTxn is propagated from one response to the next request,
		// and we adopt the mechanism that whenever the Txn changes, it needs
		// to be set in the reply, for example to ratchet up the transaction
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
