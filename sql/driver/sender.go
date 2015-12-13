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

package driver

import (
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util/retry"
)

// defaultRetryOptions sets the retry options for handling retryable errors and
// connection I/O errors.
var defaultRetryOptions = retry.Options{
	InitialBackoff: 50 * time.Millisecond,
	MaxBackoff:     5 * time.Second,
	Multiplier:     2,
	MaxRetries:     2,
}

// Sender is an interface for sending a request to a SQL database backend.
type Sender interface {
	// Send dispatches a `Request` and returns the resulting `Response` with an
	// optional transmission error.
	Send(Request) (Response, error)
}

// NewSenderFunc creates a new sender for the registered scheme.
type NewSenderFunc func(u *url.URL, ctx *base.Context, retryOpts retry.Options) (Sender, error)

var senders = struct {
	sync.Mutex
	m map[string]NewSenderFunc
}{m: map[string]NewSenderFunc{}}

// RegisterSender registers the specified function to be used for
// creation of a new sender when the specified scheme is encountered.
func RegisterSender(scheme string, f NewSenderFunc) {
	if f == nil {
		log.Fatalf("unable to register nil function for \"%s\"", scheme)
	}
	senders.Lock()
	defer senders.Unlock()
	if _, ok := senders.m[scheme]; ok {
		log.Fatalf("sender already registered for \"%s\"", scheme)
	}
	senders.m[scheme] = f
}

func newSender(u *url.URL, ctx *base.Context) (Sender, error) {
	senders.Lock()
	defer senders.Unlock()
	f := senders.m[u.Scheme]
	if f == nil {
		return nil, fmt.Errorf("no sender registered for \"%s\"", u.Scheme)
	}
	return f(u, ctx, defaultRetryOptions)
}
