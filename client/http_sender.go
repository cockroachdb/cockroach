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
	"time"

	"golang.org/x/net/context"

	snappy "github.com/cockroachdb/c-snappy"
	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
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
	f := func(u *url.URL, ctx *base.Context) (Sender, error) {
		ctx.Insecure = (u.Scheme != "https")
		return newHTTPSender(u.Host, ctx)
	}
	RegisterSender("http", f)
	RegisterSender("https", f)
}

// httpSendError wraps any error returned when sending an HTTP request
// in order to signal the retry loop that it should backoff and retry.
type httpSendError struct {
	error
}

// httpRetryOptions sets the retry options for handling retryable
// HTTP errors and connection I/O errors.
var httpRetryOptions = retry.Options{
	Backoff:     50 * time.Millisecond,
	MaxBackoff:  5 * time.Second,
	Constant:    2,
	MaxAttempts: 0, // retry indefinitely
	UseV1Info:   true,
}

// httpSender is an implementation of Sender which exposes the
// Key-Value database provided by a Cockroach cluster by connecting
// via HTTP to a Cockroach node. Overly-busy nodes will redirect
// this client to other nodes.
type httpSender struct {
	server  string        // The host:port address of the Cockroach gateway node
	client  *http.Client  // The HTTP client
	context *base.Context // The base context: needed for client setup.
}

// newHTTPSender returns a new instance of httpSender.
func newHTTPSender(server string, ctx *base.Context) (*httpSender, error) {
	sender := &httpSender{
		server:  server,
		context: ctx,
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
func (s *httpSender) Send(_ context.Context, call Call) {
	retryOpts := httpRetryOptions
	retryOpts.Tag = fmt.Sprintf("%s %s", s.context.RequestScheme(), call.Method())

	if err := retry.WithBackoff(retryOpts, func() (retry.Status, error) {
		resp, err := s.post(call)
		if err != nil {
			if resp != nil {
				log.Warningf("failed to send HTTP request with status code %d, %s", resp.StatusCode, resp.Status)
				// See if we can retry based on HTTP response code.
				switch resp.StatusCode {
				case http.StatusServiceUnavailable, http.StatusGatewayTimeout, StatusTooManyRequests:
					// Retry on service unavailable and request timeout.
					// TODO(spencer): consider respecting the Retry-After header for
					// backoff / retry duration.
					return retry.Continue, nil
				default:
					// Can't recover from all other errors.
					return retry.Break, err
				}
			}
			switch t := err.(type) {
			case *httpSendError:
				// Assume all errors sending request are retryable. The actual
				// number of things that could go wrong is vast, but we don't
				// want to miss any which should in theory be retried with
				// the same client command ID. We log the error here as a
				// warning so there's visiblity that this is happening. Some of
				// the errors we'll sweep up in this net shouldn't be retried,
				// but we can't really know for sure which.
				log.Warningf("failed to send HTTP request or read its response: %s", t)
				return retry.Continue, nil
			default:
				// Can't retry in order to recover from this error. Propagate.
				return retry.Break, err
			}
		}
		// On successful post, we're done with retry loop.
		return retry.Break, nil
	}); err != nil {
		call.Reply.Header().SetGoError(err)
	}
}

// post posts the call using the HTTP client. The call's method is
// appended to KVDBEndpoint and set as the URL path. The call's arguments
// are protobuf-serialized and written as the POST body. The content
// type is set to application/x-protobuf.
//
// On success, the response body is unmarshalled into call.Reply.
func (s *httpSender) post(call Call) (*http.Response, error) {
	// Marshal the args into a request body.
	body, err := gogoproto.Marshal(call.Args)
	if err != nil {
		return nil, err
	}

	url := s.context.RequestScheme() + "://" + s.server + KVDBEndpoint + call.Method().String()
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, util.Errorf("unable to create request: %s", err)
	}
	req.Header.Add("Content-Type", "application/x-protobuf")
	req.Header.Add("Accept", "application/x-protobuf")
	req.Header.Add("Accept-Encoding", "snappy")
	resp, err := s.client.Do(req)
	if resp == nil {
		return nil, &httpSendError{util.Errorf("http client was closed: %s", err)}
	}
	defer resp.Body.Close()
	if err != nil {
		return nil, &httpSendError{err}
	}
	if resp.Header.Get("Content-Encoding") == "snappy" {
		resp.Body = &snappyReader{body: resp.Body}
	}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, &httpSendError{err}
	}
	if resp.StatusCode != 200 {
		return resp, errors.New(resp.Status)
	}
	if err := gogoproto.Unmarshal(b, call.Reply); err != nil {
		log.Errorf("request completed, but unable to unmarshal response from server: %s; body=%q", err, b)
		return nil, &httpSendError{err}
	}
	return resp, nil
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
