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
// Author: Peter Mattis (peter@cockroachlabs.com)

package client

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/roachpb"
)

// Sender is the interface used to call into a Cockroach instance.
// If the returned *roachpb.Error is not nil, no response should be returned.
type Sender interface {
	Send(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)
}

// SenderFunc is an adapter to allow the use of ordinary functions
// as Senders.
type SenderFunc func(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error)

// Send calls f(ctx, c).
func (f SenderFunc) Send(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
	return f(ctx, ba)
}

// SendWrappedWith is a convenience function which wraps the request in a batch
// and sends it via the provided Sender and headers. It returns the unwrapped
// response or an error. It's valid to pass a `nil` context; an empty one is
// used in that case.
func SendWrappedWith(
	sender Sender, ctx context.Context, h roachpb.Header, args roachpb.Request,
) (roachpb.Response, *roachpb.Error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ba := roachpb.BatchRequest{}
	ba.Header = h
	ba.Add(args)

	br, pErr := sender.Send(ctx, ba)
	if pErr != nil {
		return nil, pErr
	}
	unwrappedReply := br.Responses[0].GetInner()
	header := unwrappedReply.Header()
	header.Txn = br.Txn
	unwrappedReply.SetHeader(header)
	return unwrappedReply, nil
}

// SendWrapped is identical to SendWrappedAt with a zero header.
// TODO(tschottdorf): should move this to testutils and merge with other helpers
// which are used, for example, in `storage`.
func SendWrapped(sender Sender, ctx context.Context, args roachpb.Request) (roachpb.Response, *roachpb.Error) {
	return SendWrappedWith(sender, ctx, roachpb.Header{}, args)
}

// Wrap returns a Sender which applies the given function before delegating to
// the supplied Sender.
func Wrap(sender Sender, f func(roachpb.BatchRequest) roachpb.BatchRequest) Sender {
	return SenderFunc(func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		return sender.Send(ctx, f(ba))
	})
}
