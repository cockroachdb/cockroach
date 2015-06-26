// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package client

import (
	"net/url"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/retry"
)

const (
	// KVDBEndpoint is the URL path prefix which accepts incoming
	// HTTP requests for the KV API.
	KVDBEndpoint = "/kv/db/"
	// StatusTooManyRequests indicates client should retry due to
	// server having too many requests.
	StatusTooManyRequests = 429
)

func init() {
	f := func(u *url.URL, ctx *base.Context, retryOpts retry.Options) (Sender, error) {
		ctx.Insecure = (u.Scheme != "https")
		return newHTTPSender(u.Host, ctx, retryOpts)
	}
	RegisterSender("http", f)
	RegisterSender("https", f)
}

// httpSender is an implementation of Sender which exposes the
// Key-Value database provided by a Cockroach cluster by connecting
// via HTTP to a Cockroach node.
type httpSender struct {
	ctx PostContext
}

// newHTTPSender returns a new instance of httpSender.
func newHTTPSender(server string, ctx *base.Context, retryOpts retry.Options) (*httpSender, error) {
	sender := &httpSender{
		ctx: PostContext{
			Server:    server,
			Endpoint:  KVDBEndpoint,
			Context:   ctx,
			RetryOpts: retryOpts,
		},
	}
	// Ensure that the context returns an HTTPClient.
	if _, err := ctx.GetHTTPClient(); err != nil {
		return nil, err
	}
	return sender, nil
}

// Send sends call to Cockroach via an HTTP post. HTTP response codes
// which are retryable are retried with backoff in a loop using the
// default retry options.
func (s *httpSender) Send(_ context.Context, call proto.Call) {
	if err := HTTPPost(s.ctx, call.Args, call.Reply, call.Method()); err != nil {
		call.Reply.Header().SetGoError(err)
	}
}
