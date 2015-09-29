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
// Author: Peter Mattis (peter@cockroachlabs.com)

package client

import (
	"fmt"
	"net/url"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/gogo/protobuf/proto"
)

// defaultRetryOptions sets the retry options for handling retryable errors and
// connection I/O errors.
var defaultRetryOptions = retry.Options{
	InitialBackoff: 250 * time.Millisecond,
	MaxBackoff:     5 * time.Second,
	Multiplier:     2,
	MaxRetries:     5,
}

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

// NewSenderFunc creates a new sender for the registered scheme.
type NewSenderFunc func(u *url.URL, ctx *base.Context, retryOpts retry.Options, stopper *stop.Stopper) (Sender, error)

var sendersMu sync.Mutex
var senders = map[string]NewSenderFunc{}

// RegisterSender registers the specified function to be used for
// creation of a new sender when the specified scheme is encountered.
func RegisterSender(scheme string, f NewSenderFunc) {
	if f == nil {
		log.Fatalf("unable to register nil function for \"%s\"", scheme)
	}
	sendersMu.Lock()
	defer sendersMu.Unlock()
	if _, ok := senders[scheme]; ok {
		log.Fatalf("sender already registered for \"%s\"", scheme)
	}
	senders[scheme] = f
}

func newSender(u *url.URL, ctx *base.Context, retryOptions retry.Options, stopper *stop.Stopper) (Sender, error) {
	sendersMu.Lock()
	defer sendersMu.Unlock()
	f := senders[u.Scheme]
	if f == nil {
		return nil, fmt.Errorf("no sender registered for \"%s\"", u.Scheme)
	}
	return f(u, ctx, retryOptions, stopper)
}

// SendWrappedAt is a convenience function which wraps the request in a batch,
// and send it via the provided Sender at the given timestamp. It returns the
// unwrapped response or an error. It's valid to pass a `nil` context;
// context.TODO() is used in that case.
func SendWrappedAt(sender Sender, ctx context.Context, ts roachpb.Timestamp, args roachpb.Request) (roachpb.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ba, unwrap := func(args roachpb.Request) (*roachpb.BatchRequest, func(*roachpb.BatchResponse) roachpb.Response) {
		ba := &roachpb.BatchRequest{}
		{
			h := *(proto.Clone(args.Header()).(*roachpb.RequestHeader))
			h.Timestamp = roachpb.ZeroTimestamp
			ba.Key, ba.EndKey = h.Key, h.EndKey
			ba.Timestamp = ts
			ba.CmdID = h.CmdID
			ba.Replica = h.Replica
			ba.RangeID = h.RangeID
			ba.UserPriority = h.UserPriority
			ba.Txn = h.Txn
			ba.ReadConsistency = h.ReadConsistency
		}
		ba.Add(args)
		return ba, func(br *roachpb.BatchResponse) roachpb.Response {
			unwrappedReply := br.Responses[0].GetInner()
			// The ReplyTxn is propagated from one response to the next request,
			// and we adopt the mechanism that whenever the Txn changes, it needs
			// to be set in the reply, for example to ratchet up the transaction
			// timestamp on writes when necessary.
			// This is internally necessary to sequentially execute the batch,
			// so it makes some sense to take the burden of updating the Txn
			// from TxnCoordSender - it will only need to act on retries/aborts
			// in the future.
			unwrappedReply.Header().Txn = br.Txn
			return unwrappedReply
		}
	}(args)
	br, pErr := sender.Send(ctx, *ba)
	if err := pErr.GoError(); err != nil {
		return nil, err
	}
	return unwrap(br), nil
}

// SendWrapped is identical to SendWrappedAt with a zero timestamp.
func SendWrapped(sender Sender, ctx context.Context, args roachpb.Request) (roachpb.Response, error) {
	return SendWrappedAt(sender, ctx, roachpb.ZeroTimestamp, args)
}
