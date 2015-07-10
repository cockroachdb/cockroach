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
// Author: Spencer Kimball (spencer@cockroachlabs.com)
//         Vivek Menezes (vivek@cockroachlabs.com)

package driver

import (
	"net/url"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/sql/sqlwire"
	"github.com/cockroachdb/cockroach/util/retry"
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
// SQL database provided by a Cockroach cluster by connecting
// via HTTP to a Cockroach node.
type httpSender struct {
	ctx client.PostContext
}

// newHTTPSender returns a new instance of httpSender.
func newHTTPSender(server string, ctx *base.Context, retryOpts retry.Options) (*httpSender, error) {
	sender := &httpSender{
		ctx: client.PostContext{
			Server:    server,
			Endpoint:  sqlwire.Endpoint,
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
func (s *httpSender) Send(args sqlwire.Request) (sqlwire.Response, error) {
	reply := sqlwire.Response{}
	return reply, client.HTTPPost(s.ctx, &args, &reply, args.Method())
}
