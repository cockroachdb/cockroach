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
	"github.com/cockroachdb/cockroach/proto"
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

// BatchSender is a new incarnation of client.Sender which only supports batches
// and uses a request-response pattern.
// TODO(tschottdorf): do away with client.Sender.
// TODO(tschottdorf) s/Batch// when client.Sender is out of the way.
type BatchSender interface {
	SendBatch(context.Context, proto.BatchRequest) (*proto.BatchResponse, *proto.Error)
}

// Sender is an interface for sending a request to a Key-Value
// database backend.
type Sender interface {
	// Send invokes the Call.Method with Call.Args and sets the result
	// in Call.Reply.
	Send(context.Context, proto.Call)
}

// SenderFunc is an adapter to allow the use of ordinary functions
// as Senders.
type SenderFunc func(context.Context, proto.Call)

// Send calls f(ctx, c).
func (f SenderFunc) Send(ctx context.Context, c proto.Call) {
	f(ctx, c)
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
