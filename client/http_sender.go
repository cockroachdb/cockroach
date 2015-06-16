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
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"

	"golang.org/x/net/context"

	snappy "github.com/cockroachdb/c-snappy"
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/retry"
	gogoproto "github.com/gogo/protobuf/proto"
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
// via HTTP to a Cockroach node. Overly-busy nodes will redirect
// this client to other nodes.
type httpSender struct {
	server    string        // The host:port address of the Cockroach gateway node
	client    *http.Client  // The HTTP client
	context   *base.Context // The base context: needed for client setup.
	retryOpts retry.Options
}

// newHTTPSender returns a new instance of httpSender.
func newHTTPSender(server string, ctx *base.Context, retryOpts retry.Options) (*httpSender, error) {
	sender := &httpSender{
		server:    server,
		context:   ctx,
		retryOpts: retryOpts,
	}
	var err error
	sender.client, err = ctx.GetHTTPClient()
	if err != nil {
		return nil, err
	}
	return sender, nil
}

// Send sends call to Cockroach via an HTTP post. HTTP response codes
// which are retryable are retried with backoff in a loop using the
// default retry options. Other errors sending HTTP request are
// retried indefinitely using the same client command ID to avoid
// reporting failure when in fact the command may have gone through
// and been executed successfully. We retry here to eventually get
// through with the same client command ID and be given the cached
// response.
func (s *httpSender) Send(_ context.Context, call proto.Call) {
	if err := s.post(call); err != nil {
		call.Reply.Header().SetGoError(err)
	}
}

// post posts the call using the HTTP client. The call's method is
// appended to KVDBEndpoint and set as the URL path. The call's arguments
// are protobuf-serialized and written as the POST body. The content
// type is set to application/x-protobuf.
//
// On success, the response body is unmarshalled into call.Reply.
func (s *httpSender) post(call proto.Call) error {
	retryOpts := s.retryOpts
	retryOpts.Tag = fmt.Sprintf("%s %s", s.context.RequestScheme(), call.Method())

	// Marshal the args into a request body.
	body, err := gogoproto.Marshal(call.Args)
	if err != nil {
		return err
	}

	url := s.context.RequestScheme() + "://" + s.server + KVDBEndpoint + call.Method().String()

	return retry.WithBackoff(retryOpts, func() (retry.Status, error) {
		req, err := http.NewRequest("POST", url, bytes.NewReader(body))
		if err != nil {
			return retry.Break, err
		}
		req.Header.Add(util.ContentTypeHeader, util.ProtoContentType)
		req.Header.Add(util.AcceptHeader, util.ProtoContentType)
		req.Header.Add(util.AcceptEncodingHeader, util.SnappyEncoding)

		resp, err := s.client.Do(req)
		if err != nil {
			return retry.Continue, err
		}

		defer resp.Body.Close()

		switch resp.StatusCode {
		case http.StatusOK:
			// We're cool.
		case http.StatusServiceUnavailable, http.StatusGatewayTimeout, StatusTooManyRequests:
			// Retry on service unavailable and request timeout.
			// TODO(spencer): consider respecting the Retry-After header for
			// backoff / retry duration.
			return retry.Continue, errors.New(resp.Status)
		default:
			// Can't recover from all other errors.
			return retry.Break, errors.New(resp.Status)
		}

		if resp.Header.Get(util.ContentEncodingHeader) == util.SnappyEncoding {
			resp.Body = &snappyReader{body: resp.Body}
		}

		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return retry.Continue, err
		}

		if err := gogoproto.Unmarshal(b, call.Reply); err != nil {
			return retry.Continue, err
		}

		return retry.Break, nil
	})
}

// snappyReader wraps a response body so it can lazily
// call snappy.NewReader on the first call to Read
type snappyReader struct {
	body io.ReadCloser // underlying Response.Body
	sr   io.Reader     // lazily-initialized snappy reader
}

func (s *snappyReader) Read(p []byte) (n int, err error) {
	if s.sr == nil {
		s.sr = snappy.NewReader(s.body)
	}
	return s.sr.Read(p)
}

func (s *snappyReader) Close() error {
	return s.body.Close()
}
