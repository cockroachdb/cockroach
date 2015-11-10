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

// SendWrappedWith is a convenience function which wraps the request in a batch
// and sends it via the provided Sender at the given timestamp. It returns the
// unwrapped response or an error. It's valid to pass a `nil` context;
// context.Background() is used in that case.
func SendWrappedWith(sender Sender, ctx context.Context, h roachpb.Header, args roachpb.Request) (roachpb.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	ba := roachpb.BatchRequest{}
	ba.Header = h
	ba.CmdID = ba.GetOrCreateCmdID(time.Now().UnixNano())
	ba.Add(args)

	br, pErr := sender.Send(ctx, ba)
	if err := pErr.GoError(); err != nil {
		return nil, err
	}
	unwrappedReply := br.Responses[0].GetInner()
	unwrappedReply.Header().Txn = br.Txn
	return unwrappedReply, nil
}

// SendWrapped is identical to SendWrappedAt with a zero header.
func SendWrapped(sender Sender, ctx context.Context, args roachpb.Request) (roachpb.Response, error) {
	return SendWrappedWith(sender, ctx, roachpb.Header{}, args)
}

// Wrap returns a Sender which applies the given function before delegating to
// the supplied Sender.
func Wrap(sender Sender, f func(roachpb.BatchRequest) roachpb.BatchRequest) Sender {
	return SenderFunc(func(ctx context.Context, ba roachpb.BatchRequest) (*roachpb.BatchResponse, *roachpb.Error) {
		return sender.Send(ctx, f(ba))
	})
}
