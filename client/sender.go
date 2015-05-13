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
// Author: Peter Mattis (peter.mattis@gmail.com)

package client

import (
	"fmt"
	"log"
	"net/url"
	"sync"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
)

// KVSender is an interface for sending a request to a Key-Value
// database backend.
type KVSender interface {
	// Send invokes the Call.Method with Call.Args and sets the result
	// in Call.Reply.
	Send(context.Context, Call)
}

// KVSenderFunc is an adapter to allow the use of ordinary functions
// as KVSenders.
type KVSenderFunc func(context.Context, Call)

// Send calls f(c).
func (f KVSenderFunc) Send(ctx context.Context, c Call) {
	f(ctx, c)
}

type newSenderFunc func(u *url.URL, ctx *base.Context) (KVSender, error)

var sendersMu sync.Mutex
var senders = map[string]newSenderFunc{}

// RegisterSender registers the specified function to be used for
// creation of a new sender when the specified scheme is encountered.
func RegisterSender(scheme string, f newSenderFunc) {
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

func newSender(u *url.URL, ctx *base.Context) (KVSender, error) {
	sendersMu.Lock()
	defer sendersMu.Unlock()
	f := senders[u.Scheme]
	if f == nil {
		return nil, fmt.Errorf("no sender registered for \"%s\"", u.Scheme)
	}
	return f(u, ctx)
}
